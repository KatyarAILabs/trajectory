// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package rules

import (
	"context"
	"testing"

	"github.com/KatyarAILabs/trajectory/pkg/record"
	"github.com/KatyarAILabs/trajectory/pkg/scorer"
)

const refund = `
verifier_id: refund.resolved
version: "1"
rules:
  - clause: reopened
    when: "latest['ticket_status'] == 'reopened'"
    reward: 0.0
  - clause: refund_completed
    when: "latest['refund_status'] == 'completed'"
    reward: 1.0
default_reward: 0.5
`

func ep(status string, latest map[string]string) scorer.Episode {
	return scorer.Episode{
		Episode:     record.Episode{EpisodeID: "e1", Status: record.StatusComplete},
		Latest:      latest,
		LabelStatus: status,
	}
}

func TestFirstMatchingRuleWins(t *testing.T) {
	s, err := Parse([]byte(refund), "t")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		latest map[string]string
		reward float64
		clause string
	}{
		// Reopened wins even though the refund also completed: rule order
		// is the policy.
		{map[string]string{"ticket_status": "reopened", "refund_status": "completed"}, 0, "reopened"},
		{map[string]string{"refund_status": "completed"}, 1, "refund_completed"},
		{map[string]string{}, 0.5, "default"},
	}
	for _, c := range cases {
		r, ok, err := s.Score(context.Background(), ep("final", c.latest))
		if err != nil || !ok {
			t.Fatalf("%v: ok=%v err=%v", c.latest, ok, err)
		}
		if r.Reward != c.reward || r.Clauses[c.clause] != c.reward {
			t.Errorf("%v: reward %v clauses %v, want %v via %s", c.latest, r.Reward, r.Clauses, c.reward, c.clause)
		}
		if r.VerifierID != "refund.resolved" || r.VerifierVersion != "1" {
			t.Errorf("verifier identity not recorded: %+v", r)
		}
	}
}

// A provisional label is not scored by default: rewarding silence before the
// complaint window closes rewards whatever has not been caught yet.
func TestProvisionalNotScored(t *testing.T) {
	s, _ := Parse([]byte(refund), "t")
	if _, ok, _ := s.Score(context.Background(), ep("provisional", map[string]string{"refund_status": "completed"})); ok {
		t.Error("a provisional label was scored")
	}
}

func TestNoDefaultMeansDoesNotApply(t *testing.T) {
	s, _ := Parse([]byte(`
verifier_id: v
version: "1"
rules:
  - {clause: done, when: "latest['refund_status'] == 'completed'", reward: 1}
`), "t")
	if _, ok, _ := s.Score(context.Background(), ep("final", nil)); ok {
		t.Error("with no default, an unmatched episode must not get a reward of zero")
	}
}

func TestBadRulesRejectedAtLoad(t *testing.T) {
	for name, body := range map[string]string{
		"not bool":       "verifier_id: v\nversion: '1'\nrules: [{clause: c, when: 'outcome_count', reward: 1}]",
		"bad CEL":        "verifier_id: v\nversion: '1'\nrules: [{clause: c, when: 'latest[', reward: 1}]",
		"no identity":    "rules: [{clause: c, when: 'true', reward: 1}]",
		"dup clause":     "verifier_id: v\nversion: '1'\nrules: [{clause: c, when: 'true', reward: 1}, {clause: c, when: 'false', reward: 0}]",
		"unknown key":    "verifier_id: v\nversion: '1'\nrewards: []",
		"scores nothing": "verifier_id: v\nversion: '1'\nrules: []",
	} {
		if _, err := Parse([]byte(body), name); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
