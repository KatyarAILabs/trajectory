// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// Every mapping shipped in mappings/ must compile. A shipped mapping that fails
// at startup is a broken feature nobody finds until a partner turns it on.
func TestShippedMappingsLoad(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("..", "..", "..", "mappings", "*.yaml"))
	if len(files) < 3 {
		t.Fatalf("found %d mapping files, want litellm, portkey and helicone", len(files))
	}
	for _, f := range files {
		m, err := LoadMapping(f)
		if err != nil {
			t.Errorf("%s: %v", filepath.Base(f), err)
			continue
		}
		if len(m.compiled["span_id"]) == 0 {
			t.Errorf("%s has no span_id path; gateway retries would duplicate steps", m.Name)
		}
	}
}

func litellm(t *testing.T) *Receiver {
	t.Helper()
	m, err := LoadMapping(filepath.Join("..", "..", "..", "mappings", "litellm.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return New(Options{Name: "litellm", Mapping: m})
}

// A representative LiteLLM success callback maps onto the canonical step.
func TestLiteLLMCallback(t *testing.T) {
	r := litellm(t)

	body := `{
	  "litellm_call_id": "call-1",
	  "model": "claude-opus-5",
	  "messages": [{"role":"user","content":"refund order 77"}],
	  "response": {
	    "model": "claude-opus-5",
	    "choices": [{"finish_reason":"stop","message":{"content":"done"}}],
	    "usage": {"prompt_tokens": 120, "completion_tokens": 8}
	  },
	  "optional_params": {"temperature": 0.2, "seed": 11},
	  "litellm_params": {"custom_llm_provider":"anthropic",
	                     "metadata":{"session_id":"sess-9","task_type":"refund"}},
	  "start_time": 1758362400.0,
	  "end_time": 1758362401.5,
	  "cache_hit": false
	}`

	envs, err := r.Map([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d envelopes", len(envs))
	}
	e := envs[0]

	if e.SessionKey != "sess-9" || e.SpanID != "call-1" {
		t.Errorf("session=%q span=%q", e.SessionKey, e.SpanID)
	}
	if e.Step.Kind != record.KindLLM {
		t.Errorf("kind = %q", e.Step.Kind)
	}
	if e.Step.Model == nil || *e.Step.Model != "claude-opus-5" {
		t.Errorf("model = %v", e.Step.Model)
	}
	if e.Step.Params == nil || e.Step.Params.Seed == nil || *e.Step.Params.Seed != 11 {
		t.Errorf("seed not captured: %+v", e.Step.Params)
	}
	if e.Step.TokenCounts == nil || *e.Step.TokenCounts.Input != 120 {
		t.Errorf("tokens = %+v", e.Step.TokenCounts)
	}
	if e.Step.FinishReason == nil || *e.Step.FinishReason != "stop" {
		t.Errorf("finish_reason = %v", e.Step.FinishReason)
	}
	// Epoch seconds, recognised by magnitude and converted to micros.
	if e.Step.StartedAt != 1758362400_000000 {
		t.Errorf("started_at = %d", e.Step.StartedAt)
	}
	if e.Step.LatencyMs == nil || *e.Step.LatencyMs != 1500 {
		t.Errorf("latency = %v", e.Step.LatencyMs)
	}
	if e.Meta.TaskType != "refund" {
		t.Errorf("task_type = %q", e.Meta.TaskType)
	}
	if !strings.Contains(*e.Step.ContentInline, "refund order 77") {
		t.Errorf("payload lost the prompt: %s", *e.Step.ContentInline)
	}
	// §9.3: unmapped scalar fields land in raw.
	if e.Step.Raw["cache_hit"] != "false" {
		t.Errorf("unmapped field not in raw: %v", e.Step.Raw)
	}
}

// A field whose name suggests a credential never lands in raw, even
// unmapped: gateways put API keys in callbacks more often than they should.
func TestCredentialLookingFieldsNotKept(t *testing.T) {
	r := litellm(t)
	envs, err := r.Map([]byte(`{"litellm_call_id":"c","api_key":"sk-live-123","user_api_key_hash":"h"}`))
	if err != nil {
		t.Fatal(err)
	}
	for k := range envs[0].Step.Raw {
		if strings.Contains(k, "key") {
			t.Errorf("credential-looking field %q was kept in raw", k)
		}
	}
}

func TestMissingSpanIDRejected(t *testing.T) {
	r := litellm(t)
	if _, err := r.Map([]byte(`{"model":"x"}`)); err == nil {
		t.Fatal("a callback with no id was accepted; a gateway retry would duplicate it")
	}
}

// A call with no session becomes a one-step episode rather than being lost.
func TestNoSessionFallsBackToCallID(t *testing.T) {
	r := litellm(t)
	envs, err := r.Map([]byte(`{"litellm_call_id":"only-call"}`))
	if err != nil {
		t.Fatal(err)
	}
	if envs[0].SessionKey != "only-call" {
		t.Errorf("session = %q, want the call id", envs[0].SessionKey)
	}
}

func TestBatchOfCallbacks(t *testing.T) {
	r := litellm(t)
	envs, err := r.Map([]byte(`[{"litellm_call_id":"a"},{"litellm_call_id":"b"}]`))
	if err != nil || len(envs) != 2 {
		t.Fatalf("got %d envelopes, err %v", len(envs), err)
	}
}

func TestInvalidMappingRejected(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field": "name: x\nkind: fixed\nfixed_kind: llm\nfields:\n  not_a_field: [\"$.a\"]\n",
		"bad kind":      "name: x\nkind: fixed\nfixed_kind: banana\nfields: {}\n",
		"bad path":      "name: x\nkind: fixed\nfixed_kind: llm\nfields:\n  model: [\"$[[[\"]\n",
	} {
		if _, err := ParseMapping([]byte(body), name); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestTimestampUnits(t *testing.T) {
	cases := map[string]struct {
		in   any
		want int64
	}{
		"seconds": {float64(1758362400), 1758362400_000000},
		"millis":  {float64(1758362400000), 1758362400_000000},
		"micros":  {float64(1758362400000000), 1758362400_000000},
		"rfc3339": {"2025-09-20T10:00:00Z", 1758362400_000000},
	}
	for name, tc := range cases {
		if got := timeUS(tc.in, true); got != tc.want {
			t.Errorf("%s: got %d want %d", name, got, tc.want)
		}
	}
}
