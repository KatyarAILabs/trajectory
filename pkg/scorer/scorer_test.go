// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package scorer

import (
	"context"
	"testing"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// completedScorer is the smallest useful verifier: did the trajectory finish
// without error. It exists to prove the interface is implementable against the
// real record types, not as something the collector ships.
type completedScorer struct{}

func (completedScorer) ID() string      { return "example.completed" }
func (completedScorer) Version() string { return "1" }

func (completedScorer) Score(_ context.Context, ep Episode) (record.Reward, bool, error) {
	r := record.Reward{
		EpisodeID:       ep.Episode.EpisodeID,
		VerifierID:      "example.completed",
		VerifierVersion: "1",
	}
	if ep.Episode.Status == record.StatusComplete && ep.Episode.Error == nil {
		r.Reward = 1
	}
	return r, true, nil
}

func TestScorerIsImplementable(t *testing.T) {
	var s Scorer = completedScorer{}
	r, ok, err := s.Score(context.Background(), Episode{
		Episode: record.Episode{EpisodeID: "e1", Status: record.StatusComplete},
	})
	if err != nil || !ok || r.Reward != 1 || r.VerifierID != s.ID() {
		t.Errorf("reward = %+v ok=%v err=%v", r, ok, err)
	}
}
