// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Int64 accepts a 64-bit integer as either a JSON number or a JSON string.
//
// protojson encodes 64-bit integers as strings to survive JavaScript's 2^53
// limit, and the published JSON Schema says so. Microsecond timestamps exceed
// 2^53 in the year 2255, so this is about matching the contract, not about
// guarding a real overflow today.
type Int64 int64

func (i *Int64) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		return nil
	}
	s = strings.Trim(s, `"`)
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid 64-bit integer %s", string(b))
	}
	*i = Int64(n)
	return nil
}

// Episode is the native JSON wire shape for POST /v1/episodes (§9.1).
//
// It is a separate type from record.Episode because the wire is permissive
// where storage is strict: enums arrive in either spelling, integers in either
// encoding, and steps are nested rather than being a separate table.
type Episode struct {
	EpisodeID string `json:"episode_id"`
	Tenant    string `json:"tenant"`
	GroupID   string `json:"group_id"`
	TaskType  string `json:"task_type"`
	Source    string `json:"source"`

	Instrumentation        string `json:"instrumentation"`
	InstrumentationVersion string `json:"instrumentation_version"`

	Status    string            `json:"status"`
	StartedAt Int64             `json:"started_at"`
	EndedAt   *Int64            `json:"ended_at"`
	Error     *Error            `json:"error"`
	Raw       map[string]string `json:"raw"`

	Steps []Step `json:"steps"`
}

// Step is the native JSON wire shape for one step.
type Step struct {
	StepIdx   *int32 `json:"step_idx"`
	ParentIdx *int32 `json:"parent_idx"`
	Attempt   int32  `json:"attempt"`
	Kind      string `json:"kind"`
	Role      string `json:"role"`

	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`

	ToolName    string `json:"tool_name"`
	ToolVersion string `json:"tool_version"`
	Model       string `json:"model"`
	Provider    string `json:"provider"`

	Params      *Params      `json:"params"`
	TokenCounts *TokenCounts `json:"token_counts"`
	TokenSpans  []TokenSpan  `json:"token_spans"`
	Trainable   string       `json:"trainable"`

	FinishReason string            `json:"finish_reason"`
	CostUSD      *float64          `json:"cost_usd"`
	StartedAt    Int64             `json:"started_at"`
	LatencyMs    *int32            `json:"latency_ms"`
	Error        *Error            `json:"error"`
	Raw          map[string]string `json:"raw"`
}

type Error struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type Params struct {
	Temperature *float64 `json:"temperature"`
	TopP        *float64 `json:"top_p"`
	MaxTokens   *int32   `json:"max_tokens"`
	Seed        *Int64   `json:"seed"`
	Stop        []string `json:"stop"`
}

type TokenCounts struct {
	Input     *Int64 `json:"input"`
	Output    *Int64 `json:"output"`
	Cached    *Int64 `json:"cached"`
	Reasoning *Int64 `json:"reasoning"`
}

// TokenSpan marks a payload range as model-generated versus tool/context
// output (F-4.4). This is the only ingest path that can carry them, because no
// tracing convention models them — which is exactly why UC-3 exists.
type TokenSpan struct {
	Start     Int64  `json:"start"`
	End       Int64  `json:"end"`
	Trainable string `json:"trainable"`
}

// DecodeEpisodes parses one episode or an array of them (§9.1).
func DecodeEpisodes(body []byte) ([]Episode, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, fmt.Errorf("empty request body")
	}

	if trimmed[0] == '[' {
		var eps []Episode
		if err := json.Unmarshal(body, &eps); err != nil {
			return nil, fmt.Errorf("invalid episode array: %w", err)
		}
		return eps, nil
	}

	var ep Episode
	if err := json.Unmarshal(body, &ep); err != nil {
		return nil, fmt.Errorf("invalid episode: %w", err)
	}
	return []Episode{ep}, nil
}

// ToEnvelopes converts a wire episode into pipeline envelopes.
//
// A native episode arrives already assembled, but it still goes through the
// assembler rather than around it. That keeps one code path for redaction,
// extraction and emission, and means a producer that sends an episode in two
// requests — or sends the same one twice — is handled by the same
// deduplication and settle logic as a span stream, instead of by a second
// implementation that would drift.
func (e Episode) ToEnvelopes(sourceName, defaultTenant string) ([]pipeline.Envelope, error) {
	if e.EpisodeID == "" {
		return nil, fmt.Errorf("episode_id is required")
	}
	if len(e.Steps) == 0 {
		return nil, fmt.Errorf("episode %s has no steps", e.EpisodeID)
	}

	status, err := NormaliseStatus(e.Status)
	if err != nil {
		return nil, fmt.Errorf("episode %s: %w", e.EpisodeID, err)
	}
	// A producer posting a complete episode is asserting it is complete; an
	// unset status means the same thing on this endpoint.
	terminal := status == "" || status == record.StatusComplete

	meta := pipeline.EpisodeMeta{
		TaskType:               e.TaskType,
		GroupID:                e.GroupID,
		Instrumentation:        e.Instrumentation,
		InstrumentationVersion: e.InstrumentationVersion,
		Raw:                    e.Raw,
	}
	if meta.Instrumentation == "" {
		meta.Instrumentation = "native"
	}

	var epErr *record.Error
	if e.Error != nil {
		epErr = &record.Error{Type: e.Error.Type, Message: e.Error.Message}
	}

	out := make([]pipeline.Envelope, 0, len(e.Steps))
	for i, ws := range e.Steps {
		step, err := ws.toRecord(i)
		if err != nil {
			return nil, fmt.Errorf("episode %s step %d: %w", e.EpisodeID, i, err)
		}
		if step.StartedAt == 0 {
			step.StartedAt = int64(e.StartedAt)
		}
		if step.StartedAt == 0 {
			step.StartedAt = receivedNow()
		}

		env := pipeline.Envelope{
			// The producer's episode id is the grouping key, so two
			// requests carrying the same id converge on one episode.
			SessionKey: e.EpisodeID,
			EpisodeID:  e.EpisodeID,
			Source:     sourceName,
			SpanID:     fmt.Sprintf("%s#%d", e.EpisodeID, step.StepIdx),
			Meta:       meta,
			Step:       step,
		}
		if ws.ParentIdx != nil {
			env.ParentSpanID = fmt.Sprintf("%s#%d", e.EpisodeID, *ws.ParentIdx)
		}
		// Only the last step carries the terminal marker and the
		// episode-level error, so a partial POST does not close the
		// episode prematurely.
		if i == len(e.Steps)-1 {
			env.Terminal = terminal
			env.Error = epErr
		}
		out = append(out, env)
	}
	return out, nil
}

func (w Step) toRecord(idx int) (record.Step, error) {
	kind, err := NormaliseKind(w.Kind)
	if err != nil {
		return record.Step{}, err
	}
	if kind == "" {
		kind = record.KindOther
	}

	trainable, err := NormaliseTrainable(w.Trainable)
	if err != nil {
		return record.Step{}, err
	}

	s := record.Step{
		CostUSD:   w.CostUSD,
		StepIdx:   int32(idx),
		Attempt:   w.Attempt,
		Kind:      kind,
		Truncated: w.Truncated,
		Trainable: trainable,
		StartedAt: int64(w.StartedAt),
		LatencyMs: w.LatencyMs,
		Raw:       w.Raw,
	}
	if w.StepIdx != nil {
		s.StepIdx = *w.StepIdx
	}
	if w.ParentIdx != nil {
		s.ParentIdx = w.ParentIdx
	}

	setStr(&s.Role, w.Role)
	setStr(&s.ToolName, w.ToolName)
	setStr(&s.ToolVersion, w.ToolVersion)
	setStr(&s.Model, w.Model)
	setStr(&s.Provider, w.Provider)
	setStr(&s.FinishReason, w.FinishReason)
	setStr(&s.ContentInline, w.Content)

	if w.Error != nil {
		s.Error = &record.Error{Type: w.Error.Type, Message: w.Error.Message}
	}
	if p := w.Params; p != nil {
		s.Params = &record.Params{
			Temperature: p.Temperature,
			TopP:        p.TopP,
			MaxTokens:   p.MaxTokens,
			Stop:        p.Stop,
		}
		if p.Seed != nil {
			seed := int64(*p.Seed)
			s.Params.Seed = &seed
		}
	}
	if t := w.TokenCounts; t != nil {
		s.TokenCounts = &record.TokenCounts{
			Input:     i64(t.Input),
			Output:    i64(t.Output),
			Cached:    i64(t.Cached),
			Reasoning: i64(t.Reasoning),
		}
	}
	for _, ts := range w.TokenSpans {
		tr, err := NormaliseTrainable(ts.Trainable)
		if err != nil {
			return record.Step{}, fmt.Errorf("token_span: %w", err)
		}
		if ts.End < ts.Start {
			return record.Step{}, fmt.Errorf(
				"token_span end (%d) is before start (%d)", ts.End, ts.Start)
		}
		s.TokenSpans = append(s.TokenSpans, record.TokenSpan{
			Start: int64(ts.Start), End: int64(ts.End), Trainable: tr,
		})
	}

	return s, nil
}

func setStr(dst **string, v string) {
	if v != "" {
		v := v
		*dst = &v
	}
}

func i64(v *Int64) *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v)
	return &n
}

func recordError(e *Error) record.Error {
	return record.Error{Type: e.Type, Message: e.Message}
}

// receivedNow stands in for a timestamp the producer did not send.
//
// Leaving it zero is worse than it looks: the episode lands in a 1970-01-01
// partition, fails the conformance suite's timestamp check, and sorts before
// every real record. The collector clock is the best available answer. It is
// the receive time rather than the event time, so for a producer that omits
// timestamps, clock-skew analysis reads zero — which is honest, since there was
// no producer clock to compare against.
func receivedNow() int64 { return time.Now().UnixMicro() }
