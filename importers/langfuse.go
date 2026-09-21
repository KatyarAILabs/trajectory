// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package importers

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

func init() { register(&Langfuse{}) }

// Langfuse imports a Langfuse observations export.
//
// Langfuse models a trace as a flat list of observations with parent links,
// which maps onto the step tree directly. What it does not carry is a seed or a
// tool version, so imported episodes will report has_params false — that is a
// real fidelity gap in the source, and F-4.6 exists so a consumer can see it
// rather than discover it while training.
type Langfuse struct{}

func (*Langfuse) Name() string { return "langfuse" }
func (*Langfuse) Describe() string {
	return "Langfuse observations export (array, {data:[...]}, or newline-delimited)"
}

type langfuseObservation struct {
	ID                  string          `json:"id"`
	TraceID             string          `json:"traceId"`
	ParentObservationID string          `json:"parentObservationId"`
	Type                string          `json:"type"`
	Name                string          `json:"name"`
	StartTime           string          `json:"startTime"`
	EndTime             string          `json:"endTime"`
	Model               string          `json:"model"`
	Input               json.RawMessage `json:"input"`
	Output              json.RawMessage `json:"output"`
	Level               string          `json:"level"`
	StatusMessage       string          `json:"statusMessage"`
	SessionID           string          `json:"sessionId"`
	ModelParameters     struct {
		Temperature *float64 `json:"temperature"`
		TopP        *float64 `json:"top_p"`
		MaxTokens   *int32   `json:"max_tokens"`
	} `json:"modelParameters"`
	Usage struct {
		Input  *int64 `json:"input"`
		Output *int64 `json:"output"`
	} `json:"usage"`
	Metadata map[string]any `json:"metadata"`
}

func (*Langfuse) Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error) {
	var st Stats

	body, err := io.ReadAll(r)
	if err != nil {
		return st, fmt.Errorf("read: %w", err)
	}
	raws, err := decodeLoose(body, "data", "observations")
	if err != nil {
		return st, err
	}

	for i, raw := range raws {
		var o langfuseObservation
		if err := json.Unmarshal(raw, &o); err != nil {
			st.Warn("record %d: %v", i, err)
			continue
		}
		if o.TraceID == "" && o.SessionID == "" {
			st.Warn("record %d: no traceId or sessionId, cannot group it into an episode", i)
			continue
		}

		start, ok := parseTime(o.StartTime)
		if !ok {
			st.Warn("record %d: unparseable startTime %q", i, o.StartTime)
			continue
		}

		step := record.Step{
			Kind:      langfuseKind(o.Type),
			StartedAt: start,
			Trainable: record.TrainableUnknown,
			Raw:       stringMap(o.Metadata),
		}
		if end, ok := parseTime(o.EndTime); ok && end > start {
			ms := int32((end - start) / 1000)
			step.LatencyMs = &ms
		}
		if o.Model != "" {
			step.Model = &o.Model
		}
		if o.Name != "" && step.Kind == record.KindTool {
			step.ToolName = &o.Name
		}
		if p := o.ModelParameters; p.Temperature != nil || p.TopP != nil || p.MaxTokens != nil {
			step.Params = &record.Params{
				Temperature: p.Temperature, TopP: p.TopP, MaxTokens: p.MaxTokens,
			}
		}
		if o.Usage.Input != nil || o.Usage.Output != nil {
			step.TokenCounts = &record.TokenCounts{Input: o.Usage.Input, Output: o.Usage.Output}
		}
		if c := payloadEnvelope(step.Kind, o.Input, o.Output); c != "" {
			step.ContentInline = &c
		}

		session := o.SessionID
		if session == "" {
			session = o.TraceID
		}

		env := pipeline.Envelope{
			SessionKey:   session,
			Source:       sourceName,
			SpanID:       o.ID,
			ParentSpanID: o.ParentObservationID,
			Step:         step,
			Meta: pipeline.EpisodeMeta{
				Instrumentation:        "langfuse-import",
				InstrumentationVersion: "unknown",
			},
		}
		if o.Level == "ERROR" {
			e := record.Error{Type: "langfuse_error", Message: o.StatusMessage}
			env.Step.Error = &e
			env.Error = &e
		}

		if err := emit(env); err != nil {
			return st, err
		}
		st.Spans++
	}

	// An export is a set of observations, not episodes; the assembler
	// decides how many episodes they form.
	st.Records = st.Spans
	return st, nil
}

func langfuseKind(t string) string {
	switch t {
	case "GENERATION":
		return record.KindLLM
	case "SPAN":
		return record.KindOther
	case "EVENT":
		return record.KindOther
	case "TOOL":
		return record.KindTool
	case "RETRIEVER":
		return record.KindRetrieval
	default:
		// F-2.5 applies to imports too: an unknown type is retained.
		return record.KindOther
	}
}

// parseTime accepts the timestamp spellings these exports actually contain.
func parseTime(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999Z0700",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMicro(), true
		}
	}
	return 0, false
}

// payloadEnvelope builds the same JSON envelope the live path produces, so
// imported and live records are addressable by the same entity expressions.
func payloadEnvelope(kind string, in, out json.RawMessage) string {
	inKey, outKey := "input", "output"
	if kind == record.KindTool {
		inKey, outKey = "args", "result"
	}

	doc := map[string]any{}
	if len(in) > 0 && string(in) != "null" {
		doc[inKey] = json.RawMessage(in)
	}
	if len(out) > 0 && string(out) != "null" {
		doc[outKey] = json.RawMessage(out)
	}
	if len(doc) == 0 {
		return ""
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return ""
	}
	return string(b)
}

// stringMap flattens arbitrary metadata to strings, because raw is
// map<string,string> and normalisation must stay lossless (F-2.2).
func stringMap(m map[string]any) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case string:
			out[k] = t
		default:
			b, err := json.Marshal(t)
			if err != nil {
				continue
			}
			out[k] = string(b)
		}
	}
	return out
}
