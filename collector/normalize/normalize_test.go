// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package normalize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

var keyOrder = []string{"session.id", "gen_ai.conversation.id", "trace_id"}

func registry(t *testing.T) *Registry {
	t.Helper()
	r, err := LoadBuiltins()
	if err != nil {
		t.Fatalf("LoadBuiltins: %v", err)
	}
	return r
}

func TestBuiltinsLoad(t *testing.T) {
	r := registry(t)
	names := r.Names()
	if len(names) < 2 {
		t.Fatalf("loaded %v, want at least openinference and otel-genai", names)
	}

	want := map[string]bool{"openinference": false, "otel-genai": false}
	for _, c := range r.conventions {
		if _, ok := want[c.Name]; ok {
			want[c.Name] = true
		}
		if c.Version == "" {
			t.Errorf("convention %q has no version; gaps must stay attributable (F-2.3)", c.Name)
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("convention %q not loaded", name)
		}
	}
}

// Detection is by attribute presence. A span carrying GenAI attributes must not
// be mapped with the OpenInference table just because it was registered first.
func TestConventionSelection(t *testing.T) {
	r := registry(t)

	cases := []struct {
		name  string
		attrs map[string]string
		want  string
	}{
		{"openinference", map[string]string{"openinference.span.kind": "LLM"}, "openinference"},
		{"genai by operation", map[string]string{"gen_ai.operation.name": "chat"}, "otel-genai"},
		{"genai by system", map[string]string{"gen_ai.system": "anthropic"}, "otel-genai"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.Select(tc.attrs).Name; got != tc.want {
				t.Errorf("Select() = %q, want %q", got, tc.want)
			}
		})
	}

	// A span matching nothing must still normalise, preserving everything.
	unknown := r.Select(map[string]string{"totally.foreign": "x"})
	if unknown == nil {
		t.Fatal("Select returned nil for an unrecognised span")
	}
	if raw := unknown.Unmapped(map[string]string{"totally.foreign": "x"}); raw["totally.foreign"] != "x" {
		t.Error("unrecognised span lost its attributes")
	}
}

// F-2.5: an unrecognised span kind is retained as "other", never dropped.
func TestUnknownKindBecomesOther(t *testing.T) {
	r := registry(t)
	c := r.Select(map[string]string{"openinference.span.kind": "WIDGET"})

	if got := c.MapKind(map[string]string{"openinference.span.kind": "WIDGET"}); got != record.KindOther {
		t.Errorf("MapKind(WIDGET) = %q, want %q", got, record.KindOther)
	}
	// Case and padding vary between producers for what is nominally a
	// closed enum.
	if got := c.MapKind(map[string]string{"openinference.span.kind": " retriever "}); got != record.KindRetrieval {
		t.Errorf("MapKind(' retriever ') = %q, want %q", got, record.KindRetrieval)
	}
}

// F-2.2: normalisation is never lossy.
func TestUnmappedAttributesPreserved(t *testing.T) {
	r := registry(t)
	env := Normalize(Span{
		SpanID: "a", TraceID: "t",
		Attributes: map[string]string{
			"openinference.span.kind": "LLM",
			"llm.model_name":          "claude-opus-5",
			"vendor.custom.flag":      "yes",
			"another.unmapped":        `{"nested":"value"}`,
		},
	}, "otlp", keyOrder, r)

	if got := env.Step.Raw["vendor.custom.flag"]; got != "yes" {
		t.Errorf("raw[vendor.custom.flag] = %q, want yes", got)
	}
	if got := env.Step.Raw["another.unmapped"]; got != `{"nested":"value"}` {
		t.Errorf("raw value not verbatim: %q", got)
	}
	if _, dup := env.Step.Raw["llm.model_name"]; dup {
		t.Error("a consumed attribute was also copied into raw")
	}
}

// OTel GenAI carries each generation parameter as its own attribute rather than
// one JSON blob. Both shapes must produce the same canonical record.
func TestGenAIScalarParams(t *testing.T) {
	r := registry(t)
	env := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			"gen_ai.operation.name":         "chat",
			"gen_ai.request.model":          "claude-opus-5",
			"gen_ai.provider.name":          "anthropic",
			"gen_ai.request.temperature":    "0.7",
			"gen_ai.request.max_tokens":     "2048",
			"gen_ai.request.seed":           "42",
			"gen_ai.request.stop_sequences": `["END","STOP"]`,
			"gen_ai.usage.input_tokens":     "1500",
			"gen_ai.usage.output_tokens":    "300",
		},
	}, "otlp", keyOrder, r)

	if env.Step.Kind != record.KindLLM {
		t.Errorf("kind = %q, want llm", env.Step.Kind)
	}
	if env.Step.Model == nil || *env.Step.Model != "claude-opus-5" {
		t.Errorf("model = %v", env.Step.Model)
	}
	p := env.Step.Params
	if p == nil {
		t.Fatal("params nil despite scalar attributes being present")
	}
	if p.Temperature == nil || *p.Temperature != 0.7 {
		t.Errorf("temperature = %v, want 0.7", p.Temperature)
	}
	if p.Seed == nil || *p.Seed != 42 {
		t.Errorf("seed = %v, want 42 — without it the step cannot be replayed", p.Seed)
	}
	if len(p.Stop) != 2 || p.Stop[0] != "END" {
		t.Errorf("stop = %v, want [END STOP]", p.Stop)
	}
	tc := env.Step.TokenCounts
	if tc == nil || tc.Input == nil || *tc.Input != 1500 {
		t.Errorf("token counts = %+v", tc)
	}
}

// A scalar attribute is more specific than the JSON blob, so it wins when a
// producer sets both.
func TestScalarParamsOverrideBlob(t *testing.T) {
	r := registry(t)
	c := r.Select(map[string]string{"gen_ai.operation.name": "chat"})

	p := params(c, map[string]string{
		"gen_ai.operation.name":      "chat",
		"gen_ai.request.temperature": "0.9",
	})
	if p == nil || p.Temperature == nil || *p.Temperature != 0.9 {
		t.Errorf("temperature = %v, want 0.9", p)
	}
}

// F-4.2: absent parameters stay nil. A replayer must distinguish "temperature
// was 0" from "temperature was not reported".
func TestAbsentParamsStayNil(t *testing.T) {
	r := registry(t)

	zero := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			"openinference.span.kind":   "LLM",
			"llm.invocation_parameters": `{"temperature":0}`,
		},
	}, "otlp", keyOrder, r)
	if zero.Step.Params == nil || zero.Step.Params.Temperature == nil {
		t.Fatal("an explicitly reported temperature of 0 was lost")
	}
	if *zero.Step.Params.Temperature != 0 {
		t.Errorf("temperature = %v, want 0", *zero.Step.Params.Temperature)
	}
	if zero.Step.Params.TopP != nil {
		t.Error("top_p was never reported but is non-nil")
	}

	none := Normalize(Span{
		SpanID:     "a",
		Attributes: map[string]string{"openinference.span.kind": "LLM"},
	}, "otlp", keyOrder, r)
	if none.Step.Params != nil {
		t.Errorf("Params = %+v, want nil", none.Step.Params)
	}
}

// Tool payloads must be addressable as $.args / $.result, because that is what
// entity-extraction expressions in config target.
func TestToolPayloadAddressable(t *testing.T) {
	r := registry(t)
	env := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			"openinference.span.kind": "TOOL",
			"tool.name":               "zendesk.update_ticket",
			"tool.parameters":         `{"id":"TKT-1"}`,
			"output.value":            `{"ok":true}`,
		},
	}, "otlp", keyOrder, r)

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
		t.Errorf("$.result.ok = %v", doc.Result["ok"])
	}
}

func TestNonJSONPayloadVerbatim(t *testing.T) {
	r := registry(t)
	const raw = "prose with \"quotes\" and \n newlines"
	env := Normalize(Span{
		SpanID:     "a",
		Attributes: map[string]string{"openinference.span.kind": "LLM", "input.value": raw},
	}, "otlp", keyOrder, r)

	var doc struct {
		Input string `json:"input"`
	}
	if err := json.Unmarshal([]byte(*env.Step.ContentInline), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Input != raw {
		t.Errorf("payload changed:\n got %q\nwant %q", doc.Input, raw)
	}
}

// F-3.1: configured order wins, then the convention's own session attributes,
// then the trace id.
func TestSessionKeyFallback(t *testing.T) {
	r := registry(t)
	conv := r.Select(map[string]string{"openinference.span.kind": "LLM"})

	cases := []struct {
		name  string
		attrs map[string]string
		trace string
		want  string
	}{
		{"configured wins", map[string]string{"session.id": "s1"}, "tr", "s1"},
		{"falls through", map[string]string{"gen_ai.conversation.id": "c1"}, "tr", "c1"},
		{"trace id last", map[string]string{}, "tr", "tr"},
		{"blank ignored", map[string]string{"session.id": "   "}, "tr", "tr"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SessionKey(Span{TraceID: tc.trace, Attributes: tc.attrs}, keyOrder, conv)
			if got != tc.want {
				t.Errorf("SessionKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

// F-4.4: a source that cannot say must yield "unknown", never a guess.
func TestTrainableDefaultsToUnknown(t *testing.T) {
	r := registry(t)
	env := Normalize(Span{SpanID: "a", Attributes: map[string]string{"openinference.span.kind": "LLM"}},
		"otlp", keyOrder, r)
	if env.Step.Trainable != record.TrainableUnknown {
		t.Errorf("Trainable = %q, want unknown", env.Step.Trainable)
	}
}

// Producers spell a boolean attribute several ways; a terminal marker must not
// be missed because the producer wrote "1".
func TestTerminalMarkerSpellings(t *testing.T) {
	r := registry(t)
	for _, v := range []string{"true", "TRUE", "1", "yes"} {
		env := Normalize(Span{
			SpanID:     "a",
			Attributes: map[string]string{"openinference.span.kind": "LLM", "episode.end": v},
		}, "otlp", keyOrder, r)
		if !env.Terminal {
			t.Errorf("episode.end=%q was not read as terminal", v)
		}
	}
	for _, v := range []string{"false", "0", "", "no"} {
		env := Normalize(Span{
			SpanID:     "a",
			Attributes: map[string]string{"openinference.span.kind": "LLM", "episode.end": v},
		}, "otlp", keyOrder, r)
		if env.Terminal {
			t.Errorf("episode.end=%q was wrongly read as terminal", v)
		}
	}
}

// F-2.4: an operator can override a builtin mapping without a release.
func TestLoadDirOverridesBuiltin(t *testing.T) {
	r := registry(t)
	before := len(r.conventions)

	dir := t.TempDir()
	override := `
name: openinference
version: "9.9.9-local"
detect: [openinference.span.kind]
kind:
  attribute: openinference.span.kind
  default: other
  values:
    LLM: llm
fields:
  model: [my.custom.model.attr]
`
	if err := os.WriteFile(filepath.Join(dir, "openinference.yaml"), []byte(override), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.LoadDir(dir); err != nil {
		t.Fatalf("LoadDir: %v", err)
	}

	if len(r.conventions) != before {
		t.Errorf("override added a convention (%d -> %d) instead of replacing one",
			before, len(r.conventions))
	}

	env := Normalize(Span{
		SpanID: "a",
		Attributes: map[string]string{
			"openinference.span.kind": "LLM",
			"my.custom.model.attr":    "local-model",
		},
	}, "otlp", keyOrder, r)

	if env.Step.Model == nil || *env.Step.Model != "local-model" {
		t.Errorf("override not applied: model = %v", env.Step.Model)
	}
}

// A mapping naming a field this build does not know is a config error. Silently
// ignoring it would look like working config that quietly drops data.
func TestUnknownCanonicalFieldRejected(t *testing.T) {
	dir := t.TempDir()
	bad := `
name: broken
version: "1"
detect: [x]
kind: {attribute: x, default: other, values: {}}
fields:
  not_a_real_field: [some.attr]
`
	if err := os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}

	r := registry(t)
	err := r.LoadDir(dir)
	if err == nil {
		t.Fatal("a mapping with an unknown field was accepted")
	}
	if !contains(err.Error(), "not_a_real_field") {
		t.Errorf("error does not name the offending field: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
