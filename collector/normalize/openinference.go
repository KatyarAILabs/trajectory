// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package normalize maps producer conventions onto the canonical step record
// (F-2.1).
//
// Two rules govern everything here:
//
//   - Normalisation is never lossy. Every attribute that is not mapped onto a
//     named field is preserved verbatim in raw (F-2.2).
//   - An unrecognised span kind becomes "other" rather than being dropped
//     (F-2.5). A span this build does not understand is still evidence.
//
// Phase 1 implements the OpenInference subset. OTel GenAI is Phase 2, which is
// why the attribute names live in tables here rather than inline: F-2.4 wants
// them as versioned data, and this is the shape that moves out to a file
// without restructuring the code.
package normalize

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// OpenInference semantic-convention attribute names.
// https://github.com/Arize-ai/openinference/blob/main/spec/semantic_conventions.md
const (
	attrSpanKind       = "openinference.span.kind"
	attrSessionID      = "session.id"
	attrInput          = "input.value"
	attrOutput         = "output.value"
	attrLLMModel       = "llm.model_name"
	attrLLMProvider    = "llm.provider"
	attrLLMInvocParams = "llm.invocation_parameters"
	attrToolName       = "tool.name"
	attrToolVersion    = "tool.version"
	attrToolParams     = "tool.parameters"

	attrTokenInput     = "llm.token_count.prompt"
	attrTokenOutput    = "llm.token_count.completion"
	attrTokenCached    = "llm.token_count.prompt_details.cache_read"
	attrTokenReasoning = "llm.token_count.completion_details.reasoning"

	// Not an OpenInference attribute. Producers that know an episode has
	// ended set it so the assembler can emit immediately rather than
	// waiting out the window (F-3.3).
	attrEpisodeEnd = "episode.end"
)

// spanKinds maps the OpenInference span kind onto the canonical step kind.
// Anything absent from this table becomes KindOther, never a dropped span.
var spanKinds = map[string]string{
	"LLM":       record.KindLLM,
	"CHAIN":     record.KindOther,
	"TOOL":      record.KindTool,
	"RETRIEVER": record.KindRetrieval,
	"EMBEDDING": record.KindOther,
	"AGENT":     record.KindOther,
	"RERANKER":  record.KindOther,
	"GUARDRAIL": record.KindOther,
	"EVALUATOR": record.KindOther,
}

// mapped is every attribute this package consumes into a named field. An
// attribute in this set is not copied into raw; everything else is.
var mapped = func() map[string]bool {
	m := map[string]bool{}
	for _, k := range []string{
		attrSpanKind, attrSessionID, attrInput, attrOutput,
		attrLLMModel, attrLLMProvider, attrLLMInvocParams,
		attrToolName, attrToolVersion, attrToolParams,
		attrTokenInput, attrTokenOutput, attrTokenCached, attrTokenReasoning,
		attrEpisodeEnd,
	} {
		m[k] = true
	}
	return m
}()

// Span is the convention-agnostic input to normalisation. The OTLP source
// flattens pdata into this so that normalisation is testable without
// constructing protobuf trace payloads.
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	StartedAtUS  int64
	EndedAtUS    int64
	Attributes   map[string]string
	// StatusError is set when the span carries a non-OK status.
	StatusError   bool
	StatusMessage string
}

// Kind classifies a span, defaulting to "other" (F-2.5).
func Kind(attrs map[string]string) string {
	raw := strings.ToUpper(strings.TrimSpace(attrs[attrSpanKind]))
	if k, ok := spanKinds[raw]; ok {
		return k
	}
	return record.KindOther
}

// SessionKey resolves the assembly key by trying each configured attribute in
// order and falling back to the trace id (F-3.1).
func SessionKey(s Span, order []string) string {
	for _, k := range order {
		if k == "trace_id" {
			if s.TraceID != "" {
				return s.TraceID
			}
			continue
		}
		if v := strings.TrimSpace(s.Attributes[k]); v != "" {
			return v
		}
	}
	return s.TraceID
}

// Normalize maps one span onto an Envelope.
//
// It never returns an error: a span this build cannot interpret still becomes
// a step, with its attributes intact in raw. Dropping it would lose evidence
// that a later reader, or a later version of this mapper, could use.
func Normalize(s Span, sourceName string, sessionKeyOrder []string) pipeline.Envelope {
	attrs := s.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	step := record.Step{
		Kind:      Kind(attrs),
		StartedAt: s.StartedAtUS,
		// The producer has not told us whether this is trainable. Saying
		// "unknown" is the honest answer; "false" would be a guess that a
		// trainer would silently act on (F-4.4).
		Trainable: record.TrainableUnknown,
		Raw:       unmappedAttrs(attrs),
	}

	if s.EndedAtUS > s.StartedAtUS {
		ms := int32((s.EndedAtUS - s.StartedAtUS) / 1000)
		step.LatencyMs = &ms
	}

	// Payload: input and output are captured verbatim and untruncated
	// (F-4.1). Truncation, if any, is the sink's decision at the configured
	// hard maximum (F-9.7).
	if content := payload(attrs, step.Kind); content != "" {
		step.ContentInline = &content
	}

	if v := attrs[attrLLMModel]; v != "" {
		step.Model = &v
	}
	if v := attrs[attrLLMProvider]; v != "" {
		step.Provider = &v
	}
	if v := attrs[attrToolName]; v != "" {
		step.ToolName = &v
	}
	if v := attrs[attrToolVersion]; v != "" {
		step.ToolVersion = &v
	}

	step.Params = params(attrs)
	step.TokenCounts = tokenCounts(attrs)

	env := pipeline.Envelope{
		SessionKey:   SessionKey(s, sessionKeyOrder),
		Source:       sourceName,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Terminal:     attrs[attrEpisodeEnd] == "true",
		Step:         step,
		Meta: pipeline.EpisodeMeta{
			Instrumentation: "openinference",
			Raw:             map[string]string{},
		},
	}

	if s.StatusError {
		e := record.Error{Type: "span_status_error", Message: s.StatusMessage}
		step.Error = &e
		env.Step = step
		env.Error = &e
	}

	return env
}

// payload builds the step's verbatim content as a JSON envelope.
//
// Structure rather than a concatenated string, for two reasons. A replayer
// needs the prompt and a scorer needs the completion, and they have to stay
// separable. And entity extraction addresses these with JSONPath from config
// ($.args.id, $.result.metadata.order_id per §10), which requires the args
// and result to remain addressable rather than flattened together.
//
// Nothing is truncated or reformatted here: a value that is itself JSON is
// embedded as JSON so paths reach into it, and anything else is embedded as
// the exact string the producer sent (F-4.1).
func payload(attrs map[string]string, kind string) string {
	var inKey, outKey string
	if kind == record.KindTool {
		inKey, outKey = "args", "result"
	} else {
		inKey, outKey = "input", "output"
	}

	in, hasIn := attrs[attrInput]
	if p, ok := attrs[attrToolParams]; ok && kind == record.KindTool {
		// tool.parameters is the more precise source for arguments when
		// a producer sets both.
		in, hasIn = p, true
	}
	out, hasOut := attrs[attrOutput]

	if !hasIn && !hasOut {
		return ""
	}

	doc := map[string]any{}
	if hasIn {
		doc[inKey] = embed(in)
	}
	if hasOut {
		doc[outKey] = embed(out)
	}

	b, err := json.Marshal(doc)
	if err != nil {
		// Marshalling a map of strings and RawMessages cannot fail in
		// practice, but falling back to the raw input is better than
		// losing the payload.
		return in + out
	}
	return string(b)
}

// embed keeps a value addressable by JSONPath when it is JSON, and exact when
// it is not.
func embed(v string) any {
	trimmed := strings.TrimSpace(v)
	if trimmed != "" && (trimmed[0] == '{' || trimmed[0] == '[') && json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	return v
}

// params reads generation parameters. A parameter the producer did not report
// stays nil rather than defaulting: a replayer must be able to tell "the
// source said temperature 0" from "the source said nothing" (F-4.2).
func params(attrs map[string]string) *record.Params {
	raw := attrs[attrLLMInvocParams]
	if raw == "" {
		return nil
	}

	p := parseInvocationParams(raw)
	if p == nil {
		// The attribute was present but unparseable. It is already
		// preserved verbatim in raw, so nothing is lost by declining to
		// guess at its contents.
		return nil
	}
	return p
}

func tokenCounts(attrs map[string]string) *record.TokenCounts {
	var tc record.TokenCounts
	any := false

	for attr, dst := range map[string]**int64{
		attrTokenInput:     &tc.Input,
		attrTokenOutput:    &tc.Output,
		attrTokenCached:    &tc.Cached,
		attrTokenReasoning: &tc.Reasoning,
	} {
		if v, ok := attrs[attr]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				n := n
				*dst = &n
				any = true
			}
		}
	}

	if !any {
		return nil
	}
	return &tc
}

// unmappedAttrs preserves every attribute this build does not interpret
// (F-2.2). A future mapper version, or a human debugging a fidelity gap, reads
// these.
func unmappedAttrs(attrs map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range attrs {
		if !mapped[k] {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
