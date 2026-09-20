// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package normalize

import (
	"encoding/json"
	"testing"

	"github.com/trajectory-project/trajectory/pkg/record"
)

var keyOrder = []string{"session.id", "gen_ai.conversation.id", "trace_id"}

// F-2.5: an unrecognised span kind is retained as "other", never dropped. A
// span this build does not understand is still evidence.
func TestUnknownSpanKindBecomesOther(t *testing.T) {
	for _, in := range []string{"WIDGET", "", "llm ", "Retriever"} {
		got := Kind(map[string]string{attrSpanKind: in})
		if got == "" {
			t.Errorf("Kind(%q) returned empty; every span must have a kind", in)
		}
	}
	if got := Kind(map[string]string{attrSpanKind: "WIDGET"}); got != record.KindOther {
		t.Errorf("Kind(WIDGET) = %q, want %q", got, record.KindOther)
	}
	// Case and surrounding space must not change the classification.
	if got := Kind(map[string]string{attrSpanKind: " retriever "}); got != record.KindRetrieval {
		t.Errorf("Kind(' retriever ') = %q, want %q", got, record.KindRetrieval)
	}
}

// F-2.2: normalisation is never lossy. Every attribute not mapped to a named
// field survives verbatim in raw.
func TestUnmappedAttributesPreservedVerbatim(t *testing.T) {
	s := Span{
		SpanID:  "a",
		TraceID: "t",
		Attributes: map[string]string{
			attrSpanKind:         "LLM",
			attrLLMModel:         "claude-opus-5",
			"vendor.custom.flag": "yes",
			"another.unmapped":   `{"nested":"value"}`,
		},
	}

	env := Normalize(s, "otlp", keyOrder)

	if got := env.Step.Raw["vendor.custom.flag"]; got != "yes" {
		t.Errorf("raw[vendor.custom.flag] = %q, want yes", got)
	}
	if got := env.Step.Raw["another.unmapped"]; got != `{"nested":"value"}` {
		t.Errorf("raw[another.unmapped] = %q, want the value verbatim", got)
	}
	// A mapped attribute must not be duplicated into raw.
	if _, dup := env.Step.Raw[attrLLMModel]; dup {
		t.Error("a mapped attribute was also copied into raw")
	}
}

// F-3.1: the session key falls back through the configured order, ending at
// the trace id.
func TestSessionKeyFallbackOrder(t *testing.T) {
	cases := []struct {
		name  string
		attrs map[string]string
		trace string
		want  string
	}{
		{"prefers session.id", map[string]string{"session.id": "s1", "gen_ai.conversation.id": "c1"}, "tr", "s1"},
		{"falls back to conversation", map[string]string{"gen_ai.conversation.id": "c1"}, "tr", "c1"},
		{"falls back to trace id", map[string]string{}, "tr", "tr"},
		{"ignores blank values", map[string]string{"session.id": "   "}, "tr", "tr"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SessionKey(Span{TraceID: tc.trace, Attributes: tc.attrs}, keyOrder)
			if got != tc.want {
				t.Errorf("SessionKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

// F-4.2: a parameter the producer did not report must stay nil. A replayer has
// to be able to tell "temperature was 0" from "temperature was not reported",
// and defaulting the absent case silently produces wrong replays.
func TestAbsentParamsStayNil(t *testing.T) {
	withTempZero := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			attrSpanKind:       "LLM",
			attrLLMInvocParams: `{"temperature":0}`,
		},
	}, "otlp", keyOrder)

	if withTempZero.Step.Params == nil || withTempZero.Step.Params.Temperature == nil {
		t.Fatal("temperature 0 was reported but did not survive")
	}
	if *withTempZero.Step.Params.Temperature != 0 {
		t.Errorf("temperature = %v, want 0", *withTempZero.Step.Params.Temperature)
	}
	if withTempZero.Step.Params.TopP != nil {
		t.Error("top_p was not reported but is non-nil")
	}

	none := Normalize(Span{
		SpanID:     "a",
		Attributes: map[string]string{attrSpanKind: "LLM"},
	}, "otlp", keyOrder)
	if none.Step.Params != nil {
		t.Errorf("Params = %+v, want nil when nothing was reported", none.Step.Params)
	}
}

// An unparseable invocation-parameters attribute must not invent values, and
// must still be recoverable from raw.
func TestUnparseableParamsDoNotGuess(t *testing.T) {
	env := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			attrSpanKind:       "LLM",
			attrLLMInvocParams: "not json at all",
		},
	}, "otlp", keyOrder)

	if env.Step.Params != nil {
		t.Errorf("Params = %+v, want nil for unparseable input", env.Step.Params)
	}
}

// Tool payloads must be addressable as $.args / $.result, because that is what
// entity extraction expressions in config target.
func TestToolPayloadIsAddressable(t *testing.T) {
	env := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			attrSpanKind:   "TOOL",
			attrToolName:   "zendesk.update_ticket",
			attrToolParams: `{"id":"TKT-1"}`,
			attrOutput:     `{"ok":true}`,
		},
	}, "otlp", keyOrder)

	if env.Step.ContentInline == nil {
		t.Fatal("tool step has no payload")
	}

	var doc struct {
		Args   map[string]any `json:"args"`
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(*env.Step.ContentInline), &doc); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if doc.Args["id"] != "TKT-1" {
		t.Errorf("$.args.id = %v, want TKT-1", doc.Args["id"])
	}
	if doc.Result["ok"] != true {
		t.Errorf("$.result.ok = %v, want true", doc.Result["ok"])
	}
}

// A non-JSON payload must be embedded exactly as the producer sent it, not
// coerced or reformatted (F-4.1).
func TestNonJSONPayloadKeptVerbatim(t *testing.T) {
	const raw = "just some prose, with \"quotes\" and \n newlines"
	env := Normalize(Span{
		SpanID:     "a",
		Attributes: map[string]string{attrSpanKind: "LLM", attrInput: raw},
	}, "otlp", keyOrder)

	var doc struct {
		Input string `json:"input"`
	}
	if err := json.Unmarshal([]byte(*env.Step.ContentInline), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Input != raw {
		t.Errorf("payload round-trip changed the content:\n got %q\nwant %q", doc.Input, raw)
	}
}

// F-4.4: a source that cannot say whether a step is trainable must yield
// "unknown", never a guess.
func TestTrainableDefaultsToUnknown(t *testing.T) {
	env := Normalize(Span{SpanID: "a", Attributes: map[string]string{attrSpanKind: "LLM"}}, "otlp", keyOrder)
	if env.Step.Trainable != record.TrainableUnknown {
		t.Errorf("Trainable = %q, want %q", env.Step.Trainable, record.TrainableUnknown)
	}
}
