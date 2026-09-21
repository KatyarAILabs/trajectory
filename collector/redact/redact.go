// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package redact removes or tokenizes sensitive values before anything is
// written (F-5).
//
// Three properties matter more than features here:
//
//   - It runs before any write to disk buffer, sink or log (F-5.3). In this
//     build that is structural: redaction is a processor, and the sink is the
//     only thing downstream of the processors.
//   - It fails closed (F-5.5). A policy that cannot be evaluated quarantines
//     the record. An error must never result in an unredacted record moving on.
//   - The manifest it emits carries rule ids, field paths and match counts, and
//     never a payload value (F-5.4). A manifest that quoted what it redacted
//     would recreate the leak it exists to prove was closed.
package redact

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
)

// Action is what a matching rule does to a value.
const (
	ActionTokenize = "tokenize"
	ActionDrop     = "drop"
)

// Rule is a compiled redaction rule.
type Rule struct {
	ID     string
	Re     *regexp.Regexp
	Path   jp.Expr
	Action string
	// Fields narrows the rule to particular field paths; empty means all.
	Fields []string
}

// Manifest records what redaction did, with no payload values (F-5.4).
type Manifest struct {
	EpisodeID string          `json:"episode_id"`
	Entries   []ManifestEntry `json:"entries"`
}

// ManifestEntry is one rule's effect on one field path.
type ManifestEntry struct {
	RuleID  string `json:"rule_id"`
	Path    string `json:"path"`
	Action  string `json:"action"`
	Matches int    `json:"matches"`
}

// Redactor applies a policy to assembled episodes.
type Redactor struct {
	rules        []Rule
	allow        []string
	deny         []string
	denyDefault  bool
	metadataOnly bool
	key          []byte
	keyID        string

	onManifest func(Manifest)

	// detector is the optional external entity detector (F-5.8).
	detector      Detector
	detectAction  string
	detectTimeout time.Duration

	// Episodes are processed concurrently, so the counters need a lock.
	// They feed §11 metrics, which must stay accurate under load — an
	// undercounted redaction match is a false assurance.
	mu    sync.Mutex
	stats Stats
}

// Stats are the counters this stage contributes to §11.
type Stats struct {
	Matches     map[string]int64
	Errors      map[string]int64
	Quarantined int64
}

// New compiles a policy.
func New(cfg config.Redaction, key []byte, onManifest func(Manifest)) (*Redactor, error) {
	r := &Redactor{
		allow:        cfg.Allow,
		deny:         cfg.Deny,
		denyDefault:  cfg.Default == "deny",
		metadataOnly: cfg.MetadataOnly,
		key:          key,
		onManifest:   onManifest,
		stats: Stats{
			Matches: map[string]int64{},
			Errors:  map[string]int64{},
		},
	}

	for _, rc := range cfg.Rules {
		rule := Rule{ID: rc.ID, Action: rc.Action, Fields: rc.Fields}

		if rc.Match.Regex != "" {
			re, err := regexp.Compile(rc.Match.Regex)
			if err != nil {
				return nil, fmt.Errorf("redaction rule %q: %w", rc.ID, err)
			}
			rule.Re = re
		}
		if rc.Match.Path != "" {
			expr, err := jp.ParseString(rc.Match.Path)
			if err != nil {
				return nil, fmt.Errorf("redaction rule %q: cannot parse path %q: %w",
					rc.ID, rc.Match.Path, err)
			}
			rule.Path = expr
		}
		if rule.Re == nil && rule.Path == nil {
			return nil, fmt.Errorf("redaction rule %q matches nothing: no regex and no path", rc.ID)
		}

		r.rules = append(r.rules, rule)
	}

	if d := cfg.Detector; d.Endpoint != "" {
		r.detector = &PresidioDetector{
			Endpoint: d.Endpoint, Language: d.Language,
			Entities: d.Entities, MinScore: d.MinScore,
		}
		r.detectAction = orDefault(d.Action, ActionTokenize)
		r.detectTimeout = d.Timeout
		if r.detectTimeout <= 0 {
			r.detectTimeout = 5 * time.Second
		}
	}

	if len(key) > 0 {
		// key_id makes a rotation detectable rather than silent (§14).
		sum := sha256.Sum256(key)
		r.keyID = hex.EncodeToString(sum[:4])
	}
	return r, nil
}

// Name implements pipeline.Processor.
func (r *Redactor) Name() string { return "redact" }

// KeyID identifies the tokenization key without revealing it. Rotating the key
// breaks joinability of new records against old ones, so a consumer needs to be
// able to tell that it happened (§14).
func (r *Redactor) KeyID() string { return r.keyID }

// Process redacts an episode in place.
//
// Returning an error means the episode is quarantined and never reaches a sink.
// Every exit path either fully redacts or returns an error; there is
// deliberately no path that returns nil after a partial pass.
func (r *Redactor) Process(_ context.Context, ep *pipeline.Assembled) error {
	entries, err := r.apply(ep)
	if err != nil {
		r.countQuarantine()
		return err
	}

	if r.onManifest != nil {
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Path != entries[j].Path {
				return entries[i].Path < entries[j].Path
			}
			return entries[i].RuleID < entries[j].RuleID
		})
		r.onManifest(Manifest{EpisodeID: ep.Episode.EpisodeID, Entries: entries})
	}
	return nil
}

// Preview runs the policy over a copy and reports what would change, without
// mutating the input. It backs `cc redact --test` (F-14.5, F-11.3).
func (r *Redactor) Preview(ep *pipeline.Assembled) ([]ManifestEntry, error) {
	clone := cloneAssembled(ep)
	return r.apply(clone)
}

func (r *Redactor) apply(ep *pipeline.Assembled) ([]ManifestEntry, error) {
	var entries []ManifestEntry

	for i := range ep.Steps {
		s := &ep.Steps[i]

		if s.ContentInline != nil {
			path := fmt.Sprintf("steps[%d].content_inline", i)
			out, ents, err := r.applyField(path, *s.ContentInline)
			if err != nil {
				return nil, err
			}
			if out == "" {
				s.ContentInline = nil
			} else {
				s.ContentInline = &out
			}
			entries = append(entries, ents...)
		}

		// raw holds whatever the producer sent that this build did not
		// map. It is the likeliest place for an unexpected identifier to
		// hide, so it gets the same policy as the payload.
		for _, k := range sortedKeys(s.Raw) {
			path := fmt.Sprintf("steps[%d].raw.%s", i, k)
			out, ents, err := r.applyField(path, s.Raw[k])
			if err != nil {
				return nil, err
			}
			if out == "" {
				delete(s.Raw, k)
			} else {
				s.Raw[k] = out
			}
			entries = append(entries, ents...)
		}
	}

	for _, k := range sortedKeys(ep.Episode.Raw) {
		path := "episode.raw." + k
		out, ents, err := r.applyField(path, ep.Episode.Raw[k])
		if err != nil {
			return nil, err
		}
		if out == "" {
			delete(ep.Episode.Raw, k)
		} else {
			ep.Episode.Raw[k] = out
		}
		entries = append(entries, ents...)
	}

	return entries, nil
}

// applyField runs the policy over one field value.
//
// Order is deliberate and load-bearing: metadata-only, then explicit deny, then
// deny-by-default, then rules. Each earlier stage is strictly more restrictive,
// so no combination of later settings can re-admit something an earlier one
// removed.
func (r *Redactor) applyField(path, value string) (string, []ManifestEntry, error) {
	if r.metadataOnly {
		return "", []ManifestEntry{{
			RuleID: "metadata_only", Path: path, Action: ActionDrop, Matches: 1,
		}}, nil
	}

	if matchAny(r.deny, path) {
		return "", []ManifestEntry{{
			RuleID: "explicit:deny", Path: path, Action: ActionDrop, Matches: 1,
		}}, nil
	}

	if r.denyDefault && !matchAny(r.allow, path) {
		return "", []ManifestEntry{{
			RuleID: "default:deny", Path: path, Action: ActionDrop, Matches: 1,
		}}, nil
	}

	var entries []ManifestEntry
	out := value

	for _, rule := range r.rules {
		if len(rule.Fields) > 0 && !matchAny(rule.Fields, path) {
			continue
		}

		// A path rule operates on the parsed payload; a regex-only rule
		// operates on the raw string.
		if rule.Path != nil {
			next, n, err := r.applyPathRule(rule, out)
			if err != nil {
				r.countError(rule.ID)
				return "", nil, err
			}
			if n > 0 {
				out = next
				r.countMatch(rule.ID, n)
				entries = append(entries, ManifestEntry{
					RuleID: rule.ID, Path: path, Action: rule.Action, Matches: n,
				})
			}
			continue
		}

		n := len(rule.Re.FindAllStringIndex(out, -1))
		if n == 0 {
			continue
		}
		r.countMatch(rule.ID, n)

		next, err := r.applyAction(rule, out)
		if err != nil {
			r.countError(rule.ID)
			return "", nil, err
		}
		out = next

		entries = append(entries, ManifestEntry{
			RuleID: rule.ID, Path: path, Action: rule.Action, Matches: n,
		})
	}

	// The detector runs after the rules, over what they left, so a value a
	// rule already tokenized is not sent to the service at all.
	if r.detector != nil && out != "" {
		next, ents, err := r.applyDetector(path, out)
		if err != nil {
			return "", nil, err
		}
		out = next
		entries = append(entries, ents...)
	}

	return out, entries, nil
}

// SetDetector replaces the entity detector, for tests and embedding hosts.
func (r *Redactor) SetDetector(d Detector, action string) {
	r.detector = d
	r.detectAction = orDefault(action, ActionTokenize)
	if r.detectTimeout <= 0 {
		r.detectTimeout = 5 * time.Second
	}
}

// applyPathRule redacts JSON nodes selected by a JSONPath.
//
// A payload that does not parse as JSON is not an error: producers send prose,
// and a path rule simply does not apply to it. Erroring would quarantine
// perfectly good trajectories for the crime of not being JSON.
func (r *Redactor) applyPathRule(rule Rule, value string) (string, int, error) {
	doc, err := oj.ParseString(value)
	if err != nil {
		return value, 0, nil
	}

	found := rule.Path.Get(doc)
	if len(found) == 0 {
		return value, 0, nil
	}

	n := 0
	for _, node := range found {
		s := stringify(node)
		if s == "" {
			continue
		}

		// When a regex is also set, it narrows the rule within the
		// selected node rather than replacing the node wholesale.
		var replacement any
		if rule.Re != nil {
			if !rule.Re.MatchString(s) {
				continue
			}
			red, err := r.applyAction(rule, s)
			if err != nil {
				return value, 0, err
			}
			replacement = red
		} else {
			red, err := r.redactWhole(rule, s)
			if err != nil {
				return value, 0, err
			}
			replacement = red
		}

		if err := rule.Path.Set(doc, replacement); err != nil {
			return value, 0, fmt.Errorf("redaction rule %q: cannot rewrite path: %w", rule.ID, err)
		}
		n++
	}

	if n == 0 {
		return value, 0, nil
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return value, 0, fmt.Errorf("redaction rule %q: cannot re-encode payload: %w", rule.ID, err)
	}
	return string(b), n, nil
}

// applyAction rewrites every regex match within a value.
func (r *Redactor) applyAction(rule Rule, value string) (string, error) {
	switch rule.Action {
	case ActionTokenize:
		if len(r.key) == 0 {
			// Fail closed: tokenizing without a key would either emit
			// the value or emit an unjoinable constant. Neither is
			// acceptable (F-5.5).
			return "", fmt.Errorf(
				"redaction rule %q: action tokenize requires a key, none configured", rule.ID)
		}
		return rule.Re.ReplaceAllStringFunc(value, r.tokenize), nil
	case ActionDrop:
		return rule.Re.ReplaceAllString(value, "[redacted:"+rule.ID+"]"), nil
	default:
		return "", fmt.Errorf("redaction rule %q: unknown action %q", rule.ID, rule.Action)
	}
}

// redactWhole replaces an entire selected value, for a path rule with no regex.
func (r *Redactor) redactWhole(rule Rule, value string) (string, error) {
	switch rule.Action {
	case ActionTokenize:
		if len(r.key) == 0 {
			return "", fmt.Errorf(
				"redaction rule %q: action tokenize requires a key, none configured", rule.ID)
		}
		return r.tokenize(value), nil
	case ActionDrop:
		return "[redacted:" + rule.ID + "]", nil
	default:
		return "", fmt.Errorf("redaction rule %q: unknown action %q", rule.ID, rule.Action)
	}
}

func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// matchAny reports whether a field path matches any pattern.
func matchAny(patterns []string, path string) bool {
	norm := indexWildcard(path)
	for _, pat := range patterns {
		p := indexWildcard(pat)
		if p == path || p == norm {
			return true
		}
		// A prefix pattern such as steps[*].raw covers everything
		// beneath it, so an operator need not enumerate raw keys they
		// have never seen.
		if strings.HasPrefix(norm, p+".") {
			return true
		}
		// A trailing .* is the same intent written explicitly.
		if strings.HasSuffix(p, ".*") && strings.HasPrefix(norm, strings.TrimSuffix(p, "*")) {
			return true
		}
	}
	return false
}

var indexRe = regexp.MustCompile(`\[\d+\]`)

// indexWildcard rewrites steps[3].content_inline to steps[*].content_inline so
// patterns are written once rather than per index.
func indexWildcard(path string) string {
	return indexRe.ReplaceAllString(path, "[*]")
}

// tokenize replaces a value with a deterministic HMAC so the same identifier
// matches across records without being readable (F-5.2).
//
// The key never leaves the deployment. Truncating to 24 hex characters keeps
// records small while leaving a collision space far beyond any plausible
// corpus.
func (r *Redactor) tokenize(v string) string {
	mac := hmac.New(sha256.New, r.key)
	mac.Write([]byte(v))
	return "tok_" + hex.EncodeToString(mac.Sum(nil))[:24]
}

func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (r *Redactor) countMatch(ruleID string, n int) {
	r.mu.Lock()
	r.stats.Matches[ruleID] += int64(n)
	r.mu.Unlock()
}

func (r *Redactor) countError(ruleID string) {
	r.mu.Lock()
	r.stats.Errors[ruleID]++
	r.mu.Unlock()
}

func (r *Redactor) countQuarantine() {
	r.mu.Lock()
	r.stats.Quarantined++
	r.mu.Unlock()
}

// Stats returns a snapshot of the stage counters.
func (r *Redactor) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := Stats{
		Matches:     make(map[string]int64, len(r.stats.Matches)),
		Errors:      make(map[string]int64, len(r.stats.Errors)),
		Quarantined: r.stats.Quarantined,
	}
	for k, v := range r.stats.Matches {
		out.Matches[k] = v
	}
	for k, v := range r.stats.Errors {
		out.Errors[k] = v
	}
	return out
}
