// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package sample

import (
	"fmt"
	"math"
	"testing"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

func rate(v float64) *float64 { return &v }

func policy(head *float64, keepIf []string, otherwise *float64) config.Sampling {
	var c config.Sampling
	c.Head.Rate = head
	c.Tail.KeepIf = keepIf
	c.Tail.OtherwiseRate = otherwise
	return c
}

func ep(id, status string, withError bool, steps int32) *pipeline.Assembled {
	e := record.Episode{
		EpisodeID: id, Tenant: "acme", Source: "otlp",
		Status: status, StepCount: steps,
	}
	if withError {
		e.Error = &record.Error{Type: "boom", Message: "failed"}
	}
	return &pipeline.Assembled{Episode: e}
}

// THE invariant of this package: sampling never splits an episode.
//
// Head sampling runs per span, before assembly, so it must return the same
// answer for every span of a session. A random draw per span would admit some
// and reject others, producing a trajectory that looks complete but is not.
func TestHeadDecisionIsStablePerSession(t *testing.T) {
	s, err := New(policy(rate(0.5), nil, nil))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("session-%d", i)
		first := s.HeadKeep(key)
		// Simulate the other spans of the same session arriving.
		for j := 0; j < 10; j++ {
			if got := s.HeadKeep(key); got != first {
				t.Fatalf("session %q got different decisions for different spans "+
					"(%v then %v); this would split the episode", key, first, got)
			}
		}
	}
}

// A rate must actually sample at roughly that rate, or the corpus size is a
// surprise.
func TestHeadRateApproximatesTarget(t *testing.T) {
	const n = 20000
	for _, want := range []float64{0.1, 0.25, 0.5, 0.9} {
		s, _ := New(policy(rate(want), nil, nil))

		kept := 0
		for i := 0; i < n; i++ {
			if s.HeadKeep(fmt.Sprintf("session-%d", i)) {
				kept++
			}
		}
		got := float64(kept) / n
		if math.Abs(got-want) > 0.02 {
			t.Errorf("head rate %v: kept %.3f, want within 0.02", want, got)
		}
	}
}

func TestHeadRateBoundaries(t *testing.T) {
	keepAll, _ := New(policy(rate(1.0), nil, nil))
	dropAll, _ := New(policy(rate(0.0), nil, nil))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("s-%d", i)
		if !keepAll.HeadKeep(key) {
			t.Fatal("rate 1.0 dropped a session")
		}
		if dropAll.HeadKeep(key) {
			t.Fatal("rate 0.0 kept a session")
		}
	}
}

// F-7.2: errors and flagged episodes are always kept, whatever the rate.
func TestTailAlwaysKeepsErrorsAndFlagged(t *testing.T) {
	s, err := New(policy(nil, []string{
		"status != 'complete'",
		"has_error",
		"raw['human_edited'] == 'true'",
	}, rate(0.0))) // drop everything that no rule matches
	if err != nil {
		t.Fatal(err)
	}

	errored := ep("e1", record.StatusComplete, true, 3)
	if !s.TailKeep(errored) {
		t.Error("an episode with an error was dropped")
	}

	timedOut := ep("e2", record.StatusTimedOut, false, 3)
	if !s.TailKeep(timedOut) {
		t.Error("a non-complete episode was dropped")
	}

	edited := ep("e3", record.StatusComplete, false, 3)
	edited.Episode.Raw = map[string]string{"human_edited": "true"}
	if !s.TailKeep(edited) {
		t.Error("a human-edited episode was dropped")
	}

	ordinary := ep("e4", record.StatusComplete, false, 3)
	if s.TailKeep(ordinary) {
		t.Error("an ordinary episode was kept despite otherwise_rate 0")
	}
}

// F-7.4: the decision is recorded so a consumer can reason about bias.
func TestSampledByRecorded(t *testing.T) {
	s, _ := New(policy(nil, []string{"has_error"}, rate(1.0)))

	errored := ep("e1", record.StatusComplete, true, 1)
	s.TailKeep(errored)
	if errored.Episode.SampledBy == nil {
		t.Fatal("sampled_by not recorded")
	}
	if got := *errored.Episode.SampledBy; got != "tail:has_error" {
		t.Errorf("sampled_by = %q, want tail:has_error", got)
	}

	ordinary := ep("e2", record.StatusComplete, false, 1)
	s.TailKeep(ordinary)
	if ordinary.Episode.SampledBy == nil || *ordinary.Episode.SampledBy != ReasonTailRate {
		t.Errorf("sampled_by = %v, want %q", ordinary.Episode.SampledBy, ReasonTailRate)
	}
}

// A broken sampling rule must keep the episode. The failure mode of a sampling
// bug must be too much data, never silent deletion — the opposite trade-off
// from redaction, where failing closed means dropping.
func TestBrokenRuleKeepsEpisode(t *testing.T) {
	// Division by zero compiles but fails at evaluation time.
	s, err := New(policy(nil, []string{"step_count / (step_count - step_count) > 0"}, rate(0.0)))
	if err != nil {
		t.Fatalf("expected the expression to compile: %v", err)
	}

	e := ep("e1", record.StatusComplete, false, 5)
	if !s.TailKeep(e) {
		t.Fatal("a rule that failed to evaluate caused the episode to be dropped")
	}
	if st := s.Stats(); st.RuleErrors != 1 {
		t.Errorf("RuleErrors = %d, want 1 — the failure must be visible", st.RuleErrors)
	}
	if e.Episode.SampledBy == nil || *e.Episode.SampledBy != "tail:rule_error" {
		t.Errorf("sampled_by = %v, want tail:rule_error", e.Episode.SampledBy)
	}
}

// A malformed expression is a config error, caught at startup rather than at
// the first episode.
func TestInvalidExpressionRejectedAtCompile(t *testing.T) {
	if _, err := New(policy(nil, []string{"this is not ( valid CEL"}, nil)); err == nil {
		t.Fatal("invalid CEL accepted")
	}
	// An expression that evaluates to a non-bool would silently never match.
	if _, err := New(policy(nil, []string{"step_count"}, nil)); err == nil {
		t.Fatal("a non-boolean expression was accepted")
	}
}

// The tail decision must be reproducible, so replaying a corpus samples it
// identically.
func TestTailRateIsDeterministic(t *testing.T) {
	s1, _ := New(policy(nil, nil, rate(0.5)))
	s2, _ := New(policy(nil, nil, rate(0.5)))

	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("ep-%d", i)
		a := s1.TailKeep(ep(id, record.StatusComplete, false, 1))
		b := s2.TailKeep(ep(id, record.StatusComplete, false, 1))
		if a != b {
			t.Fatalf("episode %s sampled differently across runs", id)
		}
	}
}

// Tail rules can reference extracted entity keys, which is why tail sampling
// runs after the extractor.
func TestTailCanReferenceEntityKeys(t *testing.T) {
	s, err := New(policy(nil, []string{"entity_key_count == 0"}, rate(0.0)))
	if err != nil {
		t.Fatal(err)
	}

	unjoinable := ep("e1", record.StatusComplete, false, 2)
	if !s.TailKeep(unjoinable) {
		t.Error("an episode with no entity keys was dropped; it is the one worth inspecting")
	}

	joinable := ep("e2", record.StatusComplete, false, 2)
	joinable.Episode.EntityKeys = []record.EntityKey{{Name: "ticket_id", Value: "tok_x"}}
	if s.TailKeep(joinable) {
		t.Error("an episode with entity keys was kept despite otherwise_rate 0")
	}
}

// An absent raw key evaluates to the empty string rather than erroring, so a
// tail policy written the way the spec's example config writes it does not
// silently degrade into "keep everything" on every ordinary episode.
func TestAbsentRawKeyIsNotAnError(t *testing.T) {
	s, err := New(policy(nil, []string{"raw['human_edited'] == 'true'"}, rate(0.0)))
	if err != nil {
		t.Fatal(err)
	}

	ordinary := ep("e1", record.StatusComplete, false, 1)
	if s.TailKeep(ordinary) {
		t.Error("an episode without the key was kept; the rule errored instead of being false")
	}
	if st := s.Stats(); st.RuleErrors != 0 {
		t.Errorf("RuleErrors = %d, want 0 — a missing key is not a broken policy", st.RuleErrors)
	}

	flagged := ep("e2", record.StatusComplete, false, 1)
	flagged.Episode.Raw = map[string]string{"human_edited": "true"}
	if !s.TailKeep(flagged) {
		t.Error("the flagged episode was dropped")
	}
}

// Presence remains distinguishable via `in`, which is the honest test.
func TestInOperatorStillDistinguishesPresence(t *testing.T) {
	s, err := New(policy(nil, []string{"'reviewed' in raw"}, rate(0.0)))
	if err != nil {
		t.Fatal(err)
	}

	absent := ep("e1", record.StatusComplete, false, 1)
	if s.TailKeep(absent) {
		t.Error("`in` reported a key that is not there")
	}

	// Present but empty: `in` must still see it.
	present := ep("e2", record.StatusComplete, false, 1)
	present.Episode.Raw = map[string]string{"reviewed": ""}
	if !s.TailKeep(present) {
		t.Error("`in` missed a key that is present with an empty value")
	}
}
