// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package assemble

import (
	"sort"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/internal/version"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// closeLocked finalises an in-flight episode and removes it from the open set.
// The caller holds a.mu and must emit the result after releasing it.
func (a *Assembler) closeLocked(f *inFlight, status string) *pipeline.Assembled {
	if el, ok := a.open[f.key]; ok {
		a.order.Remove(el)
		delete(a.open, f.key)
	}
	a.rememberEmitted(f.key, f.episodeID)
	for id := range f.spans {
		a.markEmittedSpan(f.key, id)
	}

	steps := f.orderedSteps()

	ep := record.Episode{
		EpisodeID:     f.episodeID,
		Tenant:        a.opts.Tenant,
		Source:        f.source,
		Status:        status,
		StartedAt:     f.startedAt,
		ReceivedAt:    a.opts.Now().UnixMicro(),
		StepCount:     int32(len(steps)),
		SchemaVersion: version.Schema,
		Error:         f.err,
		Fidelity:      fidelityOf(steps),
		EntityKeys:    []record.EntityKey{},
	}

	if f.endedAt > 0 {
		end := f.endedAt
		ep.EndedAt = &end
	}
	if f.meta.TaskType != "" {
		v := f.meta.TaskType
		ep.TaskType = &v
	}
	if f.meta.GroupID != "" {
		v := f.meta.GroupID
		ep.GroupID = &v
	}
	if f.meta.Instrumentation != "" {
		v := f.meta.Instrumentation
		ep.Instrumentation = &v
	}
	if f.meta.InstrumentationVersion != "" {
		v := f.meta.InstrumentationVersion
		ep.InstrumentationVersion = &v
	}
	if len(f.meta.Raw) > 0 {
		ep.Raw = f.meta.Raw
	}

	return &pipeline.Assembled{Episode: ep, Steps: steps}
}

// orderedSteps produces the canonical linear ordering and resolves the tree
// links (F-3.4).
//
// Ordering is derived only from span content — timestamp, then span id as a
// total-order tiebreak — and never from arrival order. That is the whole
// reason a shuffled, duplicated stream produces byte-identical output
// (F-3 acceptance). Sorting by arrival sequence would pass a naive test and
// fail the shuffle harness.
func (f *inFlight) orderedSteps() []record.Step {
	ids := make([]string, 0, len(f.spans))
	for id := range f.spans {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		a, b := f.spans[ids[i]], f.spans[ids[j]]
		if a.Step.StartedAt != b.Step.StartedAt {
			return a.Step.StartedAt < b.Step.StartedAt
		}
		// Span ids are unique within an episode, so this is a total
		// order: no pair can compare equal, and the sort is stable
		// regardless of the input permutation.
		return ids[i] < ids[j]
	})

	// idx maps span id to its position, so parent links resolve to indices.
	idx := make(map[string]int32, len(ids))
	for i, id := range ids {
		idx[id] = int32(i)
	}

	steps := make([]record.Step, 0, len(ids))
	for i, id := range ids {
		env := f.spans[id]
		s := env.Step
		s.EpisodeID = f.episodeID
		s.StepIdx = int32(i)

		// A parent outside this episode, or one whose span never
		// arrived, leaves the step as a root rather than inventing a
		// link.
		if env.ParentSpanID != "" {
			if p, ok := idx[env.ParentSpanID]; ok && p != int32(i) {
				pi := p
				s.ParentIdx = &pi
			}
		}

		steps = append(steps, s)
	}
	return steps
}

// fidelityOf summarises which replay-critical fields the producer supplied
// (F-4.6), so a consumer can filter to trainable episodes with one predicate
// instead of re-deriving it per field.
//
// Each flag is true only if every step that could carry the field does. A
// partially-instrumented episode is not "has params" — a trainer that assumed
// otherwise would train on steps it cannot reproduce.
func fidelityOf(steps []record.Step) *record.Fidelity {
	if len(steps) == 0 {
		return &record.Fidelity{}
	}

	fid := record.Fidelity{
		HasParams:       true,
		HasTokenSpans:   true,
		HasToolVersions: true,
	}
	sawLLM, sawTool := false, false

	for _, s := range steps {
		if s.Kind == record.KindLLM {
			sawLLM = true
			if s.Params == nil {
				fid.HasParams = false
			}
			if len(s.TokenSpans) == 0 {
				fid.HasTokenSpans = false
			}
		}
		if s.Kind == record.KindTool {
			sawTool = true
			if s.ToolVersion == nil {
				fid.HasToolVersions = false
			}
		}
	}

	// A flag about a step kind the episode does not contain is not a
	// fidelity gap. An episode with no tool calls has not "lost" tool
	// versions, and claiming it did would make the filter useless.
	if !sawLLM {
		fid.HasParams = false
		fid.HasTokenSpans = false
	}
	if !sawTool {
		fid.HasToolVersions = false
	}

	return &fid
}
