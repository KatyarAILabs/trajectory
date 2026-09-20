// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

// RESERVED SURFACE (§2.3, §7.4).
//
// These tables are created empty and NEVER written in v1. Joining
// trajectories to outcomes (N-2) and computing rewards (N-3) are declared
// non-goals; the collector has no opinion about correctness.
//
// They are defined now because it costs nothing, and it means the join and the
// CT export later arrive as additive schema changes rather than a major version
// bump that breaks every reader.
//
// Do not add a writer for these without reopening §2.2 of the spec.

// Outcome is a business result keyed by an extracted entity key.
//
// OccurredAt and ObservedAt are distinct because a future join needs as-of
// semantics and watermarks; recording only one forecloses that.
type Outcome struct {
	EntityKey  string `parquet:"entity_key"`
	Kind       string `parquet:"kind"`
	Value      string `parquet:"value"`
	OccurredAt int64  `parquet:"occurred_at,timestamp(microsecond)"`
	ObservedAt int64  `parquet:"observed_at,timestamp(microsecond)"`
	Source     string `parquet:"source"`
}

// Label is a human or machine annotation on a step.
type Label struct {
	EpisodeID string `parquet:"episode_id"`
	StepIdx   int32  `parquet:"step_idx"`
	Kind      string `parquet:"kind"`
	Value     string `parquet:"value"`
	Actor     string `parquet:"actor"`
	At        int64  `parquet:"at,timestamp(microsecond)"`
}

// Reward is a verifier's score for an episode. The collector ships no verifier
// (N-3); a Scorer interface exists so an external system can attach these later
// (F-13.2).
type Reward struct {
	EpisodeID       string             `parquet:"episode_id"`
	VerifierID      string             `parquet:"verifier_id"`
	VerifierVersion string             `parquet:"verifier_version"`
	Reward          float64            `parquet:"reward"`
	Clauses         map[string]float64 `parquet:"clauses,optional"`
	At              int64              `parquet:"at,timestamp(microsecond)"`
}
