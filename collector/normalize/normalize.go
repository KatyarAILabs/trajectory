// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package normalize maps producer conventions onto the canonical step record
// (F-2.1).
//
// Two rules govern everything here:
//
//   - Normalisation is never lossy. Every attribute a convention does not
//     consume is preserved verbatim in raw (F-2.2).
//   - An unrecognised span kind becomes "other" rather than being dropped
//     (F-2.5). A span this build does not understand is still evidence.
//
// The convention tables themselves are data files, not compiled logic (F-2.4).
// See collector/normalize/conventions.
package normalize

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Span is the convention-agnostic input to normalisation. Sources flatten
// their wire format into this, so convention mapping is testable without
// constructing protobuf payloads and a new source is not a new mapper.
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	StartedAtUS  int64
	EndedAtUS    int64
	Attributes   map[string]string

	StatusError   bool
	StatusMessage string

	// ScopeName and ScopeVersion identify the instrumentation library, so
	// fidelity gaps stay attributable (F-2.3).
	ScopeName    string
	ScopeVersion string
}

// Normalize maps one span onto an Envelope.
//
// It never returns an error. A span this build cannot interpret still becomes
// a step with its attributes intact in raw; dropping it would lose evidence
// that a later mapper version, or a human debugging a fidelity gap, could use.
func Normalize(s Span, sourceName string, sessionKeyOrder []string, reg *Registry) pipeline.Envelope {
	attrs := s.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	conv := reg.Select(attrs)
	kind := conv.MapKind(attrs)

	step := record.Step{
		Kind:      kind,
		StartedAt: s.StartedAtUS,
		// The producer has not told us whether this is trainable. Saying
		// "unknown" is the honest answer; "false" would be a guess a
		// trainer would silently act on (F-4.4).
		Trainable: record.TrainableUnknown,
		Raw:       conv.Unmapped(attrs),
	}

	if s.EndedAtUS > s.StartedAtUS {
		ms := int32((s.EndedAtUS - s.StartedAtUS) / 1000)
		step.LatencyMs = &ms
	}

	if content := payload(conv, attrs, kind); content != "" {
		step.ContentInline = &content
	}

	setIfPresent(conv, attrs, FModel, &step.Model)
	setIfPresent(conv, attrs, FProvider, &step.Provider)
	setIfPresent(conv, attrs, FToolName, &step.ToolName)
	setIfPresent(conv, attrs, FToolVersion, &step.ToolVersion)
	setIfPresent(conv, attrs, FRole, &step.Role)
	setIfPresent(conv, attrs, FFinishReason, &step.FinishReason)

	if v, ok := conv.Get(attrs, FCost); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			step.CostUSD = &f
		}
	}

	step.Params = params(conv, attrs)
	step.TokenCounts = tokenCounts(conv, attrs)

	env := pipeline.Envelope{
		SessionKey:   SessionKey(s, sessionKeyOrder, conv),
		Source:       sourceName,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Step:         step,
		Meta: pipeline.EpisodeMeta{
			Instrumentation:        conv.Name,
			InstrumentationVersion: conv.Version,
		},
	}

	// The scope, when the producer sets one, is more specific than the
	// convention name and is what a consumer needs to attribute a gap.
	if s.ScopeName != "" {
		env.Meta.Instrumentation = s.ScopeName
	}
	if s.ScopeVersion != "" {
		env.Meta.InstrumentationVersion = s.ScopeVersion
	}

	if v, ok := conv.Get(attrs, FTaskType); ok {
		env.Meta.TaskType = v
	}
	if v, ok := conv.Get(attrs, FGroupID); ok {
		env.Meta.GroupID = v
	}
	if v, ok := conv.Get(attrs, FEpisodeEnd); ok {
		env.Terminal = isTrue(v)
	}

	if s.StatusError {
		e := record.Error{Type: "span_status_error", Message: s.StatusMessage}
		env.Step.Error = &e
		env.Error = &e
	}

	return env
}

// isTrue accepts the spellings producers actually emit for a boolean
// attribute, rather than only the one this codebase would have chosen.
func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "t":
		return true
	}
	return false
}

func setIfPresent(c *Convention, attrs map[string]string, field string, dst **string) {
	if v, ok := c.Get(attrs, field); ok {
		*dst = &v
	}
}

// SessionKey resolves the assembly key (F-3.1).
//
// The configured order wins, because an operator who names an attribute knows
// something the convention tables do not. The convention's own session
// attributes are tried next, and the trace id is the final fallback.
func SessionKey(s Span, order []string, conv *Convention) string {
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
	if v, ok := conv.Get(s.Attributes, FSessionKey); ok {
		return strings.TrimSpace(v)
	}
	return s.TraceID
}

// payload builds the step's verbatim content as a JSON envelope.
//
// Structure rather than a concatenated string, for two reasons. A replayer
// needs the prompt and a scorer needs the completion, and they have to stay
// separable. And entity extraction addresses these with JSONPath from config
// ($.args.id, $.result.metadata.order_id per §10), which requires args and
// result to remain addressable rather than flattened together.
//
// Nothing is truncated or reformatted: a value that is itself JSON is embedded
// as JSON so paths reach into it, and anything else is embedded as the exact
// string the producer sent (F-4.1).
func payload(c *Convention, attrs map[string]string, kind string) string {
	var inKey, outKey string
	if kind == record.KindTool {
		inKey, outKey = "args", "result"
	} else {
		inKey, outKey = "input", "output"
	}

	in, hasIn := c.Get(attrs, FInput)
	if kind == record.KindTool {
		// Tool arguments are the more precise source than a generic
		// input value when a producer sets both.
		if v, ok := c.Get(attrs, FToolArgs); ok {
			in, hasIn = v, true
		}
	}
	out, hasOut := c.Get(attrs, FOutput)

	if !hasIn && !hasOut {
		return ""
	}

	doc := map[string]any{}
	if hasIn {
		doc[inKey] = embedValue(in)
	}
	if hasOut {
		doc[outKey] = embedValue(out)
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return in + out
	}
	return string(b)
}

// embedValue keeps a value addressable by JSONPath when it is JSON, and exact
// when it is not.
func embedValue(v string) any {
	t := strings.TrimSpace(v)
	if t != "" && (t[0] == '{' || t[0] == '[') && json.Valid([]byte(t)) {
		return json.RawMessage(t)
	}
	return v
}

// params reads generation parameters from whichever shape the convention uses:
// one JSON blob (OpenInference) or individual attributes (OTel GenAI).
//
// A parameter the producer did not report stays nil rather than defaulting. A
// replayer must be able to tell "the source said temperature 0" from "the
// source said nothing" (F-4.2); collapsing those produces silently wrong
// replays.
func params(c *Convention, attrs map[string]string) *record.Params {
	var p record.Params
	any := false

	if raw, ok := c.Get(attrs, FInvocationParams); ok {
		if parsed := parseInvocationParams(raw); parsed != nil {
			p = *parsed
			any = true
		}
	}

	// Individual attributes win over the blob: they are the more specific
	// source, and a producer that sets both is telling us the scalar is
	// authoritative.
	if v, ok := c.Get(attrs, FTemperature); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			p.Temperature, any = &f, true
		}
	}
	if v, ok := c.Get(attrs, FTopP); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			p.TopP, any = &f, true
		}
	}
	if v, ok := c.Get(attrs, FMaxTokens); ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			n32 := int32(n)
			p.MaxTokens, any = &n32, true
		}
	}
	if v, ok := c.Get(attrs, FSeed); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			p.Seed, any = &n, true
		}
	}
	if v, ok := c.Get(attrs, FStopSequences); ok {
		if stops := parseStops(v); len(stops) > 0 {
			p.Stop, any = stops, true
		}
	}

	if !any {
		return nil
	}
	return &p
}

// parseStops accepts the two shapes producers use for a list attribute: a JSON
// array, or a comma-separated string.
func parseStops(v string) []string {
	t := strings.TrimSpace(v)
	if strings.HasPrefix(t, "[") {
		var out []string
		if err := json.Unmarshal([]byte(t), &out); err == nil {
			return out
		}
	}
	if t == "" {
		return nil
	}
	parts := strings.Split(t, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func tokenCounts(c *Convention, attrs map[string]string) *record.TokenCounts {
	var tc record.TokenCounts
	any := false

	for _, m := range []struct {
		field string
		dst   **int64
	}{
		{FTokenInput, &tc.Input},
		{FTokenOutput, &tc.Output},
		{FTokenCached, &tc.Cached},
		{FTokenReasoning, &tc.Reasoning},
	} {
		if v, ok := c.Get(attrs, m.field); ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				n := n
				*m.dst = &n
				any = true
			}
		}
	}

	if !any {
		return nil
	}
	return &tc
}
