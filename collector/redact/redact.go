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
//   - The manifest it emits contains rule ids, field paths and match counts,
//     and never a payload value (F-5.4). A manifest that quoted what it
//     redacted would recreate the leak it exists to prove was closed.
package redact

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

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
	Action string
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
	rules   []Rule
	allow   []string
	denyAll bool
	key     []byte
	keyID   string

	// onManifest receives each episode's manifest. It is separate from the
	// record so the manifest can be written somewhere a reader of the data
	// cannot reach.
	onManifest func(Manifest)

	// Episodes are processed concurrently, so the counters need a lock.
	// They feed §11 metrics, which must stay accurate under load — an
	// undercounted redaction match is a false assurance.
	mu    sync.Mutex
	stats Stats
}

// Stats are the counters this stage contributes to §11.
type Stats struct {
	Matches     map[string]int64 // by rule id
	Errors      map[string]int64 // by rule id
	Quarantined int64
}

// New compiles a policy. A rule that does not compile is an error at
// construction, not a surprise at the first matching record — config
// validation catches it earlier, and this is the backstop.
func New(cfg config.Redaction, key []byte, onManifest func(Manifest)) (*Redactor, error) {
	r := &Redactor{
		allow:      cfg.Allow,
		denyAll:    cfg.Default == "deny",
		key:        key,
		onManifest: onManifest,
		stats: Stats{
			Matches: map[string]int64{},
			Errors:  map[string]int64{},
		},
	}

	for _, rc := range cfg.Rules {
		re, err := regexp.Compile(rc.Match.Regex)
		if err != nil {
			return nil, fmt.Errorf("redaction rule %q: %w", rc.ID, err)
		}
		r.rules = append(r.rules, Rule{ID: rc.ID, Re: re, Action: rc.Action})
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
// breaks joinability of new records against old ones, so a consumer needs to
// be able to tell that it happened (§14).
func (r *Redactor) KeyID() string { return r.keyID }

// Process redacts an episode in place.
//
// Returning an error means the episode is quarantined and never reaches a
// sink. Every exit path from this function either fully redacts or returns an
// error; there is deliberately no path that returns nil after a partial pass.
func (r *Redactor) Process(_ context.Context, ep *pipeline.Assembled) error {
	var entries []ManifestEntry

	for i := range ep.Steps {
		s := &ep.Steps[i]

		if s.ContentInline != nil {
			path := fmt.Sprintf("steps[%d].content_inline", i)
			out, ents, err := r.applyField(path, *s.ContentInline)
			if err != nil {
				r.countQuarantine()
				return err
			}
			s.ContentInline = &out
			entries = append(entries, ents...)
		}

		// raw holds whatever the producer sent that this build did not
		// map. It is the likeliest place for an unexpected identifier to
		// hide, so it is subject to the same policy as the payload —
		// under deny-by-default, to the allow-list too.
		for k, v := range s.Raw {
			path := fmt.Sprintf("steps[%d].raw.%s", i, k)
			out, ents, err := r.applyField(path, v)
			if err != nil {
				r.countQuarantine()
				return err
			}
			s.Raw[k] = out
			entries = append(entries, ents...)
		}
	}

	for k, v := range ep.Episode.Raw {
		path := "episode.raw." + k
		out, ents, err := r.applyField(path, v)
		if err != nil {
			r.countQuarantine()
			return err
		}
		ep.Episode.Raw[k] = out
		entries = append(entries, ents...)
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

// applyField runs the policy over one field value.
func (r *Redactor) applyField(path, value string) (string, []ManifestEntry, error) {
	// Deny-by-default: a field not on the allow-list does not survive at
	// all, whatever the rules say. The rules then narrow what remains.
	if r.denyAll && !r.allowed(path) {
		return "", []ManifestEntry{{
			RuleID: "default:deny", Path: path, Action: ActionDrop, Matches: 1,
		}}, nil
	}

	var entries []ManifestEntry
	out := value

	for _, rule := range r.rules {
		n := len(rule.Re.FindAllStringIndex(out, -1))
		if n == 0 {
			continue
		}
		r.countMatch(rule.ID, n)

		switch rule.Action {
		case ActionTokenize:
			if len(r.key) == 0 {
				// Fail closed: tokenizing without a key would
				// either emit the value or emit an unjoinable
				// constant. Neither is acceptable (F-5.5).
				r.countError(rule.ID)
				return "", nil, fmt.Errorf(
					"redaction rule %q: action tokenize requires a key, none configured", rule.ID)
			}
			out = rule.Re.ReplaceAllStringFunc(out, r.tokenize)
		case ActionDrop:
			out = rule.Re.ReplaceAllString(out, "[redacted:"+rule.ID+"]")
		default:
			r.countError(rule.ID)
			return "", nil, fmt.Errorf("redaction rule %q: unknown action %q", rule.ID, rule.Action)
		}

		entries = append(entries, ManifestEntry{
			RuleID: rule.ID, Path: path, Action: rule.Action, Matches: n,
		})
	}

	return out, entries, nil
}

// allowed reports whether a field path survives under deny-by-default.
// A pattern may end in [*] or use [*] as a wildcard index segment.
func (r *Redactor) allowed(path string) bool {
	norm := indexWildcard(path)
	for _, pat := range r.allow {
		if pat == path || pat == norm {
			return true
		}
		// A prefix rule such as steps[*].raw allows everything beneath
		// it, so an operator does not have to enumerate raw keys they
		// have never seen.
		if strings.HasPrefix(norm, indexWildcard(pat)+".") {
			return true
		}
	}
	return false
}

var indexRe = regexp.MustCompile(`\[\d+\]`)

// indexWildcard rewrites steps[3].content_inline to steps[*].content_inline so
// allow-list patterns are written once rather than per index.
func indexWildcard(path string) string {
	return indexRe.ReplaceAllString(path, "[*]")
}

// tokenize replaces a value with a deterministic HMAC so the same identifier
// matches across records without being readable (F-5.2).
//
// The key never leaves the deployment. Truncating to 12 bytes keeps records
// small while leaving a collision space far beyond any plausible corpus.
func (r *Redactor) tokenize(v string) string {
	mac := hmac.New(sha256.New, r.key)
	mac.Write([]byte(v))
	return "tok_" + hex.EncodeToString(mac.Sum(nil))[:24]
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
