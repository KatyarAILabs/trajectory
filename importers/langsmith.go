// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package importers

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

func init() { register(&LangSmith{}) }

// LangSmith imports a LangSmith runs export.
//
// LangSmith's run tree is already the shape we want: parent_run_id gives the
// tree, and run_type maps onto step kinds. Its extra field is
// `extra.invocation_params`, which is the only place in these vendor formats
// where a seed survives — so LangSmith imports can reach has_params true where
// Langfuse imports cannot.
type LangSmith struct{}

func (*LangSmith) Name() string { return "langsmith" }
func (*LangSmith) Describe() string {
	return "LangSmith runs export (array, {runs:[...]}, or newline-delimited)"
}

type langsmithRun struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	RunType     string          `json:"run_type"`
	ParentRunID string          `json:"parent_run_id"`
	TraceID     string          `json:"trace_id"`
	SessionID   string          `json:"session_id"`
	StartTime   string          `json:"start_time"`
	EndTime     string          `json:"end_time"`
	Inputs      json.RawMessage `json:"inputs"`
	Outputs     json.RawMessage `json:"outputs"`
	Error       string          `json:"error"`
	Extra       struct {
		InvocationParams struct {
			Model       string   `json:"model"`
			ModelName   string   `json:"model_name"`
			Temperature *float64 `json:"temperature"`
			TopP        *float64 `json:"top_p"`
			MaxTokens   *int32   `json:"max_tokens"`
			Seed        *int64   `json:"seed"`
			Stop        []string `json:"stop"`
		} `json:"invocation_params"`
		Metadata map[string]any `json:"metadata"`
	} `json:"extra"`
	Tags []string `json:"tags"`
}

func (*LangSmith) Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error) {
	var st Stats

	body, err := io.ReadAll(r)
	if err != nil {
		return st, fmt.Errorf("read: %w", err)
	}
	raws, err := decodeLoose(body, "runs", "data")
	if err != nil {
		return st, err
	}

	for i, raw := range raws {
		var run langsmithRun
		if err := json.Unmarshal(raw, &run); err != nil {
			st.Warn("record %d: %v", i, err)
			continue
		}

		session := firstNonEmpty(run.SessionID, run.TraceID, run.ID)
		if session == "" {
			st.Warn("record %d: no session_id, trace_id or id to group by", i)
			continue
		}

		start, ok := parseTime(run.StartTime)
		if !ok {
			st.Warn("record %d: unparseable start_time %q", i, run.StartTime)
			continue
		}

		kind := langsmithKind(run.RunType)
		step := record.Step{
			Kind:      kind,
			StartedAt: start,
			Trainable: record.TrainableUnknown,
			Raw:       stringMap(run.Extra.Metadata),
		}
		if end, ok := parseTime(run.EndTime); ok && end > start {
			ms := int32((end - start) / 1000)
			step.LatencyMs = &ms
		}

		ip := run.Extra.InvocationParams
		if m := firstNonEmpty(ip.Model, ip.ModelName); m != "" {
			step.Model = &m
		}
		if ip.Temperature != nil || ip.TopP != nil || ip.MaxTokens != nil ||
			ip.Seed != nil || len(ip.Stop) > 0 {
			step.Params = &record.Params{
				Temperature: ip.Temperature, TopP: ip.TopP,
				MaxTokens: ip.MaxTokens, Seed: ip.Seed, Stop: ip.Stop,
			}
		}
		if kind == record.KindTool && run.Name != "" {
			step.ToolName = &run.Name
		}
		if c := payloadEnvelope(kind, run.Inputs, run.Outputs); c != "" {
			step.ContentInline = &c
		}

		env := pipeline.Envelope{
			SessionKey:   session,
			Source:       sourceName,
			SpanID:       run.ID,
			ParentSpanID: run.ParentRunID,
			Step:         step,
			Meta: pipeline.EpisodeMeta{
				Instrumentation:        "langsmith-import",
				InstrumentationVersion: "unknown",
			},
		}
		if run.Error != "" {
			e := record.Error{Type: "langsmith_error", Message: run.Error}
			env.Step.Error = &e
			env.Error = &e
		}

		if err := emit(env); err != nil {
			return st, err
		}
		st.Spans++
	}

	st.Records = st.Spans
	return st, nil
}

func langsmithKind(t string) string {
	switch t {
	case "llm":
		return record.KindLLM
	case "tool":
		return record.KindTool
	case "retriever":
		return record.KindRetrieval
	case "chain", "prompt", "parser", "embedding":
		return record.KindOther
	default:
		return record.KindOther
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
