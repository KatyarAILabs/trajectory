// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	v1 "github.com/KatyarAILabs/trajectory/gen/go/trajectory/v1"
)

// SpanRecord is the JSON shape of one observation for POST /v1/spans (§9.2).
//
// Unlike /v1/episodes, the producer does not hold the whole trajectory: it
// sends steps as they happen, keyed by session, and the collector assembles
// them. This is what an SDK uses for a long-running agent loop, where buffering
// every step until the end would mean losing all of them on a crash.
type SpanRecord struct {
	SessionID string `json:"session_id"`
	// EpisodeID is optional. When set it becomes the stored episode_id, so
	// an SDK can hand its caller an id it can later look up.
	EpisodeID    string `json:"episode_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	Terminal     bool   `json:"terminal"`

	TaskType               string            `json:"task_type"`
	GroupID                string            `json:"group_id"`
	Instrumentation        string            `json:"instrumentation"`
	InstrumentationVersion string            `json:"instrumentation_version"`
	EpisodeError           *Error            `json:"episode_error"`
	EpisodeRaw             map[string]string `json:"episode_raw"`

	Step Step `json:"step"`
}

// DecodeSpans parses one span record or an array of them.
func DecodeSpans(body []byte) ([]SpanRecord, error) {
	t := strings.TrimSpace(string(body))
	if t == "" {
		return nil, fmt.Errorf("empty request body")
	}
	if t[0] == '[' {
		var out []SpanRecord
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, fmt.Errorf("invalid span array: %w", err)
		}
		return out, nil
	}
	var one SpanRecord
	if err := json.Unmarshal(body, &one); err != nil {
		return nil, fmt.Errorf("invalid span: %w", err)
	}
	return []SpanRecord{one}, nil
}

// DecodeSpansProto parses a protobuf SpanBatch, converging on the same
// SpanRecord the JSON path produces.
func DecodeSpansProto(body []byte) ([]SpanRecord, error) {
	var batch v1.SpanBatch
	if err := proto.Unmarshal(body, &batch); err != nil {
		return nil, fmt.Errorf("invalid protobuf SpanBatch: %w", err)
	}

	out := make([]SpanRecord, 0, len(batch.Spans))
	for _, ps := range batch.Spans {
		r := SpanRecord{
			SessionID:              ps.GetSessionId(),
			SpanID:                 ps.GetSpanId(),
			ParentSpanID:           ps.GetParentSpanId(),
			Terminal:               ps.GetTerminal(),
			TaskType:               ps.GetTaskType(),
			GroupID:                ps.GetGroupId(),
			Instrumentation:        ps.GetInstrumentation(),
			InstrumentationVersion: ps.GetInstrumentationVersion(),
			EpisodeRaw:             ps.GetEpisodeRaw(),
		}
		if e := ps.GetEpisodeError(); e != nil {
			r.EpisodeError = &Error{Type: e.GetType(), Message: e.GetMessage()}
		}
		if ps.GetStep() != nil {
			r.Step = stepFromProto(ps.GetStep())
		}
		out = append(out, r)
	}
	return out, nil
}

// ToEnvelope converts one span record for the assembler.
func (r SpanRecord) ToEnvelope(sourceName string) (pipeline.Envelope, error) {
	if r.SessionID == "" {
		// Without a session there is nothing to assemble against, and
		// silently giving each span its own episode would fragment the
		// trajectory the producer thought it was sending.
		return pipeline.Envelope{}, fmt.Errorf("session_id is required")
	}
	if r.SpanID == "" {
		return pipeline.Envelope{}, fmt.Errorf(
			"span_id is required; without it a redelivery cannot be deduplicated")
	}

	step, err := r.Step.toRecord(0)
	if err != nil {
		return pipeline.Envelope{}, err
	}
	if step.StartedAt == 0 {
		step.StartedAt = receivedNow()
	}

	env := pipeline.Envelope{
		SessionKey:   r.SessionID,
		EpisodeID:    r.EpisodeID,
		Source:       sourceName,
		SpanID:       r.SpanID,
		ParentSpanID: r.ParentSpanID,
		Terminal:     r.Terminal,
		Step:         step,
		Meta: pipeline.EpisodeMeta{
			TaskType:               r.TaskType,
			GroupID:                r.GroupID,
			Instrumentation:        r.Instrumentation,
			InstrumentationVersion: r.InstrumentationVersion,
			Raw:                    r.EpisodeRaw,
		},
	}
	if env.Meta.Instrumentation == "" {
		env.Meta.Instrumentation = "native"
	}
	if r.EpisodeError != nil {
		e := recordError(r.EpisodeError)
		env.Error = &e
	}
	return env, nil
}
