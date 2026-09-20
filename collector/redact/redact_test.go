// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"context"
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
