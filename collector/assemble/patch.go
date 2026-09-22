// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package assemble

import (
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/internal/version"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// patchFor builds an append-only patch record for a span that arrived after
// its episode was emitted (F-3.5).
//
// The emitted episode is never mutated. Files are already written and may
// already have been read; rewriting them would break the immutability a
// consumer relies on, and would mean a reader could see different content for
// the same episode_id at two points in time.
//
// A patch carries the same episode_id and status "patched". A reader
// reconstructs the full trajectory by unioning the original with its patches,
// which is why step_idx continues from where the original ended rather than
// restarting.
func (a *Assembler) patchFor(episodeID string, env pipeline.Envelope) *pipeline.Assembled {
	step := env.Step
	step.EpisodeID = episodeID
	// The original episode's indices are already written and immutable. A
	// patch step therefore cannot know its true position, so it takes the
	// next free index from this assembler's view and a reader orders by
	// started_at, which is stable across both records.
	step.StepIdx = a.nextPatchIdx(episodeID)

	ep := record.Episode{
		EpisodeID:     episodeID,
		Tenant:        a.opts.Tenant,
		Source:        env.Source,
		Status:        record.StatusPatched,
		StartedAt:     step.StartedAt,
		ReceivedAt:    a.opts.Now().UnixMicro(),
		StepCount:     1,
		SchemaVersion: version.Schema,
		EntityKeys:    []record.EntityKey{},
		// Fidelity is deliberately nil on a patch: the flags describe a
		// whole trajectory, and a one-step fragment cannot speak for it.
		// A consumer reads fidelity from the original record.
	}

	if env.Meta.TaskType != "" {
		v := env.Meta.TaskType
		ep.TaskType = &v
	}
	if env.Meta.GroupID != "" {
		v := env.Meta.GroupID
		ep.GroupID = &v
	}
	if env.Meta.Instrumentation != "" {
		v := env.Meta.Instrumentation
		ep.Instrumentation = &v
	}
	if env.Meta.InstrumentationVersion != "" {
		v := env.Meta.InstrumentationVersion
		ep.InstrumentationVersion = &v
	}

	return &pipeline.Assembled{Episode: ep, Steps: []record.Step{step}}
}

// nextPatchIdx hands out increasing indices for patches of one episode, so two
// late spans do not collide on the same step_idx.
func (a *Assembler) nextPatchIdx(episodeID string) int32 {
	if a.patchIdx == nil {
		a.patchIdx = map[string]int32{}
	}
	// Patch indices start high so they cannot be confused with an original
	// step index by a reader that concatenates without sorting.
	idx, ok := a.patchIdx[episodeID]
	if !ok {
		idx = patchIdxBase
	}
	a.patchIdx[episodeID] = idx + 1
	return idx
}

// patchIdxBase keeps patch step indices clear of any plausible original index.
const patchIdxBase = 1 << 20

// rememberEmitted records a closed session so late spans can find their
// episode. The map is bounded and evicted in insertion order (F-3.7's spirit:
// no unbounded growth anywhere in assembly).
func (a *Assembler) rememberEmitted(sessionKey, episodeID string) {
	if _, exists := a.emitted[sessionKey]; !exists {
		a.emittedFIFO = append(a.emittedFIFO, sessionKey)
	}
	a.emitted[sessionKey] = episodeID

	for len(a.emittedFIFO) > a.opts.PatchMemory {
		oldest := a.emittedFIFO[0]
		a.emittedFIFO = a.emittedFIFO[1:]
		if id, ok := a.emitted[oldest]; ok {
			delete(a.emitted, oldest)
			delete(a.patchIdx, id)
			delete(a.emittedSpans, oldest)
		}
	}
}
