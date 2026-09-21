// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	v1 "github.com/trajectory-project/trajectory/gen/go/trajectory/v1"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// DecodeEpisodesProto parses a protobuf EpisodeBatch (F-1.2).
//
// It converts into the same wire.Episode the JSON path produces, and from there
// both share ToEnvelopes. Two conversions into the pipeline — one per encoding
// — would be two places for enum handling, int64 handling and step ordering to
// disagree, and a producer's data would then depend on which encoding its SDK
// happened to pick.
func DecodeEpisodesProto(body []byte) ([]Episode, error) {
	var batch v1.EpisodeBatch
	if err := proto.Unmarshal(body, &batch); err != nil {
		return nil, fmt.Errorf("invalid protobuf EpisodeBatch: %w", err)
	}
	if len(batch.Episodes) == 0 {
		return nil, fmt.Errorf("EpisodeBatch contains no episodes")
	}

	out := make([]Episode, 0, len(batch.Episodes))
	for i, ews := range batch.Episodes {
		if ews.GetEpisode() == nil {
			return nil, fmt.Errorf("episodes[%d]: episode is missing", i)
		}
		out = append(out, fromProto(ews))
	}
	return out, nil
}

func fromProto(ews *v1.EpisodeWithSteps) Episode {
	pe := ews.GetEpisode()

	e := Episode{
		EpisodeID:              pe.GetEpisodeId(),
		Tenant:                 pe.GetTenant(),
		GroupID:                pe.GetGroupId(),
		TaskType:               pe.GetTaskType(),
		Source:                 pe.GetSource(),
		Instrumentation:        pe.GetInstrumentation(),
		InstrumentationVersion: pe.GetInstrumentationVersion(),
		Status:                 record.StatusFromProto(pe.GetStatus()),
		StartedAt:              Int64(pe.GetStartedAt()),
		Raw:                    pe.GetRaw(),
	}
	if pe.EndedAt != nil {
		v := Int64(pe.GetEndedAt())
		e.EndedAt = &v
	}
	if pe.GetError() != nil {
		e.Error = &Error{Type: pe.GetError().GetType(), Message: pe.GetError().GetMessage()}
	}

	for _, ps := range ews.GetSteps() {
		e.Steps = append(e.Steps, stepFromProto(ps))
	}
	return e
}

func stepFromProto(ps *v1.Step) Step {
	idx := ps.GetStepIdx()
	s := Step{
		StepIdx:      &idx,
		Attempt:      ps.GetAttempt(),
		Kind:         record.KindFromProto(ps.GetKind()),
		Role:         ps.GetRole(),
		Content:      ps.GetContentInline(),
		Truncated:    ps.GetTruncated(),
		ToolName:     ps.GetToolName(),
		ToolVersion:  ps.GetToolVersion(),
		Model:        ps.GetModel(),
		Provider:     ps.GetProvider(),
		Trainable:    record.TrainableFromProto(ps.GetTrainable()),
		FinishReason: ps.GetFinishReason(),
		StartedAt:    Int64(ps.GetStartedAt()),
		Raw:          ps.GetRaw(),
	}
	if ps.ParentIdx != nil {
		p := ps.GetParentIdx()
		s.ParentIdx = &p
	}
	if ps.CostUsd != nil {
		c := ps.GetCostUsd()
		s.CostUSD = &c
	}
	if ps.LatencyMs != nil {
		l := ps.GetLatencyMs()
		s.LatencyMs = &l
	}
	if ps.GetError() != nil {
		s.Error = &Error{Type: ps.GetError().GetType(), Message: ps.GetError().GetMessage()}
	}

	if p := ps.GetParams(); p != nil {
		s.Params = &Params{Stop: p.GetStop()}
		if p.Temperature != nil {
			v := p.GetTemperature()
			s.Params.Temperature = &v
		}
		if p.TopP != nil {
			v := p.GetTopP()
			s.Params.TopP = &v
		}
		if p.MaxTokens != nil {
			v := p.GetMaxTokens()
			s.Params.MaxTokens = &v
		}
		if p.Seed != nil {
			v := Int64(p.GetSeed())
			s.Params.Seed = &v
		}
	}

	if t := ps.GetTokenCounts(); t != nil {
		s.TokenCounts = &TokenCounts{}
		if t.Input != nil {
			v := Int64(t.GetInput())
			s.TokenCounts.Input = &v
		}
		if t.Output != nil {
			v := Int64(t.GetOutput())
			s.TokenCounts.Output = &v
		}
		if t.Cached != nil {
			v := Int64(t.GetCached())
			s.TokenCounts.Cached = &v
		}
		if t.Reasoning != nil {
			v := Int64(t.GetReasoning())
			s.TokenCounts.Reasoning = &v
		}
	}

	for _, ts := range ps.GetTokenSpans() {
		s.TokenSpans = append(s.TokenSpans, TokenSpan{
			Start:     Int64(ts.GetStart()),
			End:       Int64(ts.GetEnd()),
			Trainable: record.TrainableFromProto(ts.GetTrainable()),
		})
	}
	return s
}
