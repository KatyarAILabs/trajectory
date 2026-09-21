// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

func ptr[T any](v T) *T { return &v }

var testKey = []byte("test-hmac-key-do-not-use-in-production")

func policy() config.Redaction {
	var c config.Redaction
	c.Default = "deny"
	c.OnError = "quarantine"
	c.Allow = []string{"steps[*].content_inline", "steps[*].raw"}
	c.Rules = []config.Rule{
		{ID: "email", Action: ActionTokenize},
		{ID: "card", Action: ActionDrop},
	}
	c.Rules[0].Match.Regex = `[\w.+-]+@[\w-]+\.[\w.]+`
	c.Rules[1].Match.Regex = `\b(?:\d[ -]*?){13,16}\b`
	return c
}

func episode(contents ...string) *pipeline.Assembled {
	ep := &pipeline.Assembled{
		Episode: record.Episode{EpisodeID: "ep-1", Tenant: "acme"},
	}
	for i, c := range contents {
		ep.Steps = append(ep.Steps, record.Step{
			EpisodeID: "ep-1", StepIdx: int32(i),
			Kind: record.KindLLM, ContentInline: ptr(c),
		})
	}
	return ep
}

// seededPII is the corpus for the F-5 acceptance criterion: known secrets at
// known counts, so a leak and a miscount are both detectable.
var seededPII = []struct {
	content string
	emails  int
	cards   int
}{
	{"contact me at alice@example.com about the refund", 1, 0},
	{"cc 4111 1111 1111 1111 was declined", 0, 1},
	{"bob@corp.co.uk and carol@corp.co.uk both replied", 2, 0},
	{"card 4111111111111111 for dave@example.org", 1, 1},
	{"no sensitive data in this one", 0, 0},
}

// F-5 acceptance: a seeded corpus of synthetic PII passes through with zero
// leaks in output, and manifest counts match the seed exactly.
func TestSeededCorpusHasZeroLeaks(t *testing.T) {
	var manifests []Manifest
	r, err := New(policy(), testKey, func(m Manifest) { manifests = append(manifests, m) })
	if err != nil {
		t.Fatal(err)
	}

	wantEmails, wantCards := 0, 0
	var contents []string
	for _, c := range seededPII {
		contents = append(contents, c.content)
		wantEmails += c.emails
		wantCards += c.cards
	}

	ep := episode(contents...)
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatalf("process: %v", err)
	}

	// Zero leaks: no seeded secret appears anywhere in the output.
	secrets := []string{
		"alice@example.com", "bob@corp.co.uk", "carol@corp.co.uk", "dave@example.org",
		"4111 1111 1111 1111", "4111111111111111",
	}
	for i, s := range ep.Steps {
		got := *s.ContentInline
		for _, secret := range secrets {
			if strings.Contains(got, secret) {
				t.Errorf("step %d leaked %q: %q", i, secret, got)
			}
		}
	}

	// Manifest counts match the seed exactly.
	gotEmails, gotCards := 0, 0
	for _, m := range manifests {
		for _, e := range m.Entries {
			switch e.RuleID {
			case "email":
				gotEmails += e.Matches
			case "card":
				gotCards += e.Matches
			}
		}
	}
	if gotEmails != wantEmails {
		t.Errorf("email matches: got %d want %d", gotEmails, wantEmails)
	}
	if gotCards != wantCards {
		t.Errorf("card matches: got %d want %d", gotCards, wantCards)
	}
}

// F-5.4: the manifest proves what was redacted without recreating the leak.
func TestManifestCarriesNoPayloadValues(t *testing.T) {
	var manifests []Manifest
	r, _ := New(policy(), testKey, func(m Manifest) { manifests = append(manifests, m) })

	if err := r.Process(context.Background(), episode("reach me at secret@example.com")); err != nil {
		t.Fatal(err)
	}

	for _, m := range manifests {
		for _, e := range m.Entries {
			for _, f := range []string{e.RuleID, e.Path, e.Action} {
				if strings.Contains(f, "secret@example.com") || strings.Contains(f, "example.com") {
					t.Errorf("manifest field %q contains payload content", f)
				}
			}
		}
	}
}

// F-5.2: the same identifier tokenizes identically across records, so it stays
// joinable, and differently under a different key.
func TestTokenizationIsDeterministicAndKeyed(t *testing.T) {
	r1, _ := New(policy(), testKey, nil)
	r2, _ := New(policy(), []byte("a-different-key"), nil)

	ep1 := episode("ping alice@example.com")
	ep2 := episode("also alice@example.com")
	ep3 := episode("ping alice@example.com")

	_ = r1.Process(context.Background(), ep1)
	_ = r1.Process(context.Background(), ep2)
	_ = r2.Process(context.Background(), ep3)

	if *ep1.Steps[0].ContentInline == *ep2.Steps[0].ContentInline {
		t.Fatal("test setup: contents differ beyond the email")
	}
	tok1 := extractToken(t, *ep1.Steps[0].ContentInline)
	tok2 := extractToken(t, *ep2.Steps[0].ContentInline)
	tok3 := extractToken(t, *ep3.Steps[0].ContentInline)

	if tok1 != tok2 {
		t.Errorf("same identifier tokenized differently under one key: %q vs %q", tok1, tok2)
	}
	if tok1 == tok3 {
		t.Error("same identifier tokenized identically under a different key; the token is not keyed")
	}
	if r1.KeyID() == r2.KeyID() {
		t.Error("different keys produced the same key_id; a rotation would be undetectable")
	}
}

func extractToken(t *testing.T, s string) string {
	t.Helper()
	i := strings.Index(s, "tok_")
	if i < 0 {
		t.Fatalf("no token in %q", s)
	}
	return strings.Fields(s[i:])[0]
}

// Deny-by-default: a field nobody allow-listed does not survive, whatever the
// rules say about its contents.
func TestDenyByDefaultDropsUnlistedFields(t *testing.T) {
	c := policy()
	c.Allow = []string{"steps[*].content_inline"} // raw deliberately not allowed
	r, _ := New(c, testKey, nil)

	ep := episode("harmless")
	ep.Steps[0].Raw = map[string]string{"internal.customer_ssn": "123-45-6789"}

	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if got := ep.Steps[0].Raw["internal.customer_ssn"]; got != "" {
		t.Errorf("unlisted raw field survived deny-by-default: %q", got)
	}
}

// F-5.5: a policy that cannot be evaluated quarantines the record. The failure
// must never be "pass the unredacted value through".
func TestFailsClosedWithoutKey(t *testing.T) {
	r, err := New(policy(), nil, nil) // tokenize rule, no key
	if err != nil {
		t.Fatal(err)
	}

	ep := episode("contact alice@example.com")
	err = r.Process(context.Background(), ep)
	if err == nil {
		t.Fatal("tokenize without a key returned nil; the episode would have reached a sink")
	}
	if s := r.Stats(); s.Quarantined != 1 {
		t.Errorf("Quarantined = %d, want 1", s.Quarantined)
	}
}

// Redaction must be idempotent: running it twice must not double-tokenize or
// corrupt an already-redacted value. Phase 2's patch records (F-3.5) will
// reprocess episodes, so this property is load-bearing later.
func TestRedactionIsIdempotent(t *testing.T) {
	r, _ := New(policy(), testKey, nil)

	ep := episode("mail alice@example.com now")
	_ = r.Process(context.Background(), ep)
	once := *ep.Steps[0].ContentInline

	_ = r.Process(context.Background(), ep)
	twice := *ep.Steps[0].ContentInline

	if once != twice {
		t.Errorf("not idempotent:\n once: %q\ntwice: %q", once, twice)
	}
}

// F-5.1 path-based policy: a known-sensitive JSON field is redacted whole,
// without writing a regex that might also hit something innocuous elsewhere.
func TestPathRuleRedactsSelectedNode(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.Rules = []config.Rule{{
		ID:     "ssn_field",
		Action: ActionDrop,
		Match:  config.RuleMatch{Path: "$.args.ssn"},
	}}

	r, err := New(c, testKey, nil)
	if err != nil {
		t.Fatal(err)
	}

	ep := episode(`{"args":{"id":"TKT-1","ssn":"123-45-6789"},"result":{"note":"123-45-6789 appears here too"}}`)
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}

	got := *ep.Steps[0].ContentInline
	if strings.Contains(got, `"ssn":"123-45-6789"`) {
		t.Errorf("targeted field not redacted: %s", got)
	}
	if !strings.Contains(got, "TKT-1") {
		t.Errorf("path rule damaged a sibling field: %s", got)
	}
	// The rule targeted one path, so the same digits elsewhere are
	// untouched. That precision is the reason to use a path rather than a
	// regex.
	if !strings.Contains(got, "123-45-6789 appears here too") {
		t.Errorf("path rule reached outside its path: %s", got)
	}
}

// A path rule combined with a regex narrows the rule within the selected node.
func TestPathRuleWithRegexNarrows(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.Rules = []config.Rule{{
		ID:     "email_in_requester",
		Action: ActionTokenize,
		Match:  config.RuleMatch{Path: "$.args.requester", Regex: `[\w.+-]+@[\w-]+\.[\w.]+`},
	}}

	r, _ := New(c, testKey, nil)
	ep := episode(`{"args":{"requester":"contact alice@example.com now","other":"bob@example.com"}}`)
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}

	got := *ep.Steps[0].ContentInline
	if strings.Contains(got, "alice@example.com") {
		t.Errorf("email in the targeted path was not tokenized: %s", got)
	}
	if !strings.Contains(got, "contact tok_") {
		t.Errorf("surrounding text in the node was lost: %s", got)
	}
	if !strings.Contains(got, "bob@example.com") {
		t.Errorf("rule reached outside its path: %s", got)
	}
}

// A payload that is not JSON must not be quarantined by a path rule. Producers
// send prose, and a path rule simply does not apply to it.
func TestPathRuleIgnoresNonJSON(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.Rules = []config.Rule{{
		ID: "x", Action: ActionDrop, Match: config.RuleMatch{Path: "$.args.ssn"},
	}}

	r, _ := New(c, testKey, nil)
	ep := episode("just some prose, definitely not JSON")
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatalf("a non-JSON payload was quarantined by a path rule: %v", err)
	}
	if *ep.Steps[0].ContentInline != "just some prose, definitely not JSON" {
		t.Error("non-JSON payload was altered")
	}
}

// An explicit deny wins over an allow, so a broad allow prefix can be carved
// out without rewriting it.
func TestExplicitDenyBeatsAllow(t *testing.T) {
	var c config.Redaction
	c.Default = "deny"
	c.Allow = []string{"steps[*].raw"}
	c.Deny = []string{"steps[*].raw.secret_token"}

	r, _ := New(c, testKey, nil)
	ep := episode("payload")
	ep.Steps[0].Raw = map[string]string{
		"harmless":     "keep me",
		"secret_token": "sk-live-abc123",
	}

	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if got := ep.Steps[0].Raw["secret_token"]; got != "" {
		t.Errorf("explicitly denied field survived: %q", got)
	}
	if got := ep.Steps[0].Raw["harmless"]; got != "keep me" {
		t.Errorf("deny removed an unrelated allowed field: %q", got)
	}
}

// F-5.6: metadata-only discards every payload, and no other setting re-admits
// one.
func TestMetadataOnlyDropsEverything(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.MetadataOnly = true
	// Deliberately permissive settings alongside it; none may win.
	c.Allow = []string{"steps[*].content_inline", "steps[*].raw"}

	r, _ := New(c, testKey, nil)
	ep := episode("a very sensitive prompt")
	ep.Steps[0].Raw = map[string]string{"k": "v"}
	ep.Steps[0].Model = ptr("claude-opus-5")

	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if ep.Steps[0].ContentInline != nil {
		t.Errorf("payload survived metadata-only mode: %q", *ep.Steps[0].ContentInline)
	}
	if len(ep.Steps[0].Raw) != 0 {
		t.Errorf("raw survived metadata-only mode: %v", ep.Steps[0].Raw)
	}
	// Metadata is the point of the mode, so it must remain.
	if ep.Steps[0].Model == nil || *ep.Steps[0].Model != "claude-opus-5" {
		t.Error("metadata-only removed metadata")
	}
}

// A rule can be scoped to particular fields.
func TestRuleFieldScoping(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.Rules = []config.Rule{{
		ID:     "email",
		Action: ActionDrop,
		Match:  config.RuleMatch{Regex: `[\w.+-]+@[\w-]+\.[\w.]+`},
		Fields: []string{"steps[*].raw"},
	}}

	r, _ := New(c, testKey, nil)
	ep := episode("payload has alice@example.com in it")
	ep.Steps[0].Raw = map[string]string{"note": "bob@example.com"}

	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ep.Steps[0].Raw["note"], "bob@example.com") {
		t.Error("scoped rule did not fire on its field")
	}
	if !strings.Contains(*ep.Steps[0].ContentInline, "alice@example.com") {
		t.Error("scoped rule fired outside its field scope")
	}
}

// F-14.5: `cc redact --test` must not modify what it inspects.
func TestPreviewDoesNotMutate(t *testing.T) {
	r, _ := New(policy(), testKey, nil)

	ep := episode("mail alice@example.com")
	ep.Steps[0].Raw = map[string]string{"k": "carol@example.com"}
	before := *ep.Steps[0].ContentInline
	beforeRaw := ep.Steps[0].Raw["k"]

	entries, err := r.Preview(ep)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Error("preview reported no changes for a payload containing an email")
	}
	if *ep.Steps[0].ContentInline != before {
		t.Errorf("preview mutated the payload: %q -> %q", before, *ep.Steps[0].ContentInline)
	}
	if ep.Steps[0].Raw["k"] != beforeRaw {
		t.Errorf("preview mutated raw: %q -> %q", beforeRaw, ep.Steps[0].Raw["k"])
	}
}

// A rule with neither regex nor path never fires; constructing one is an error
// rather than a silently inert policy.
func TestRuleMatchingNothingRejected(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	c.Rules = []config.Rule{{ID: "inert", Action: ActionDrop}}

	if _, err := New(c, testKey, nil); err == nil {
		t.Fatal("a rule with no regex and no path was accepted")
	}
}

// fakeDetector finds a fixed word, standing in for a model-backed service.
type fakeDetector struct {
	word string
	err  error
}

func (f fakeDetector) Detect(_ context.Context, text string) ([]Finding, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []Finding
	for i := 0; i+len(f.word) <= len(text); i++ {
		if text[i:i+len(f.word)] == f.word {
			out = append(out, Finding{EntityType: "PERSON", Start: i, End: i + len(f.word), Score: 0.9})
		}
	}
	return out, nil
}

// F-5.8: an entity no regex catches is redacted by the detector.
func TestDetectorRedactsNames(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	r, _ := New(c, testKey, nil)
	r.SetDetector(fakeDetector{word: "Priya Raman"}, ActionTokenize)

	ep := episode("the customer Priya Raman asked for a refund")
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	got := *ep.Steps[0].ContentInline
	if strings.Contains(got, "Priya Raman") {
		t.Errorf("name survived: %q", got)
	}
	if !strings.Contains(got, "the customer tok_") || !strings.Contains(got, "asked for a refund") {
		t.Errorf("surrounding text damaged: %q", got)
	}
}

// The detector being down must quarantine, not pass: if the service that finds
// names is unavailable, the safe assumption is the names are still there.
func TestDetectorFailureFailsClosed(t *testing.T) {
	var c config.Redaction
	c.Default = "allow"
	r, _ := New(c, testKey, nil)
	r.SetDetector(fakeDetector{err: fmt.Errorf("connection refused")}, ActionDrop)

	if err := r.Process(context.Background(), episode("Priya Raman")); err == nil {
		t.Fatal("detector outage let an episode through unchecked")
	}
}

// End to end against an HTTP server speaking Presidio's analyzer protocol.
func TestPresidioProtocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in struct{ Text string }
		json.NewDecoder(req.Body).Decode(&in)
		i := strings.Index(in.Text, "Priya")
		json.NewEncoder(w).Encode([]Finding{
			{EntityType: "PERSON", Start: i, End: i + 5, Score: 0.85},
			{EntityType: "LOCATION", Start: 0, End: 3, Score: 0.2}, // below min_score
		})
	}))
	defer srv.Close()

	var c config.Redaction
	c.Default = "allow"
	c.Detector.Endpoint = srv.URL + "/analyze"
	c.Detector.MinScore = 0.5
	c.Detector.Action = ActionDrop
	r, err := New(c, testKey, nil)
	if err != nil {
		t.Fatal(err)
	}

	ep := episode("hi Priya, welcome")
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	got := *ep.Steps[0].ContentInline
	if got != "hi [redacted:PERSON], welcome" {
		t.Errorf("got %q", got)
	}
}

// The outcome join depends on this: a key tokenized on the episode side must
// tokenize identically on the outcome side, or the join matches nothing.
func TestTransformKeyMatchesEpisodeSide(t *testing.T) {
	r, _ := New(policy(), testKey, nil)

	// Episode side: the key is inside a payload and redacted as a field.
	ep := episode(`{"args":{"requester":"alice@example.com"}}`)
	if err := r.Process(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	episodeToken := extractToken(t, *ep.Steps[0].ContentInline)

	// Outcome side: the raw value as a ticketing system would export it.
	outcomeKey, err := r.TransformKey("alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if outcomeKey != strings.Trim(episodeToken, `"}`) {
		t.Errorf("outcome key %q does not match episode token %q; the join would miss", outcomeKey, episodeToken)
	}

	// A key no rule touches passes through unchanged.
	if got, _ := r.TransformKey("TKT-77"); got != "TKT-77" {
		t.Errorf("an untouched key changed: %q", got)
	}
}
