// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package scorer defines how an external system attaches rewards to episodes
// (F-13.2).
//
// The collector ships no implementation, and that is a decision rather than an
// omission: computing rewards and running verifiers are declared non-goals
// (N-3) — the collector has no opinion about correctness. The interface is here
// so that a verifier written later reads the same records a trainer does and
// writes into the reserved `rewards` table (§7.4) in a known shape, instead of
// every consumer inventing its own.
//
// A Scorer runs outside the collector, against the lake. Nothing in the
// collector calls one.
package scorer

import (
	"context"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// Episode is one trajectory as a scorer sees it: the episode record and its
// steps in canonical order, payloads already resolved from blobs.
type Episode struct {
	Episode record.Episode
	Steps   []record.Step
	// Payloads maps step_idx to the resolved payload, whether it was inline
	// or externalised, so a scorer never has to know about content_ref.
	Payloads map[int32]string
}

// Scorer assigns a reward to an episode.
//
// Implementations must be deterministic for a given (VerifierID, Version) and
// episode: a reward that changes between two runs over the same data cannot be
// trained on, and cannot be audited.
type Scorer interface {
	// ID and Version identify the verifier. They are recorded on every
	// reward, so two versions of one verifier never overwrite each other.
	ID() string
	Version() string

	// Score returns a reward, or ok=false when the verifier does not apply
	// to this episode — which is different from a reward of zero.
	Score(ctx context.Context, ep Episode) (reward record.Reward, ok bool, err error)
}
