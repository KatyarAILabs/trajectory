// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
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

// realLiteLLM loads payloads captured from a running LiteLLM 1.102.0 proxy.
// The mapping is tested against what LiteLLM actually sends, because the first
// version of it was written from memory and was wrong about the session, every
// sampling parameter and both timestamps.
func realLiteLLM(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "spec", "testdata", "litellm-standard-logging-payload.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRealLiteLLMPayload(t *testing.T) {
	r := litellm(t)
	envs, err := r.Map(realLiteLLM(t))
	if err != nil {
		t.Fatal(err)
	}

	var llm, tool, failed []pipeline.Envelope
	for _, e := range envs {
		switch {
		case e.Step.Kind == record.KindTool:
			tool = append(tool, e)
		case e.Error != nil:
			failed = append(failed, e)
		default:
			llm = append(llm, e)
		}
	}
	if len(llm) != 2 || len(failed) != 1 {
		t.Fatalf("got %d llm, %d failed, %d tool envelopes; want 2, 1, and at least 1",
			len(llm), len(failed), len(tool))
	}

	first := llm[0]
	// Session: from litellm_session_id, which LiteLLM reports as trace_id.
	if first.SessionKey != "sess-fixture-1" {
		t.Errorf("session = %q; calls of one agent run would not group", first.SessionKey)
	}
	// Sampling parameters live under model_parameters.
	p := first.Step.Params
	if p == nil || p.Seed == nil || *p.Seed != 7 || p.Temperature == nil || *p.Temperature != 0.2 {
		t.Errorf("params = %+v; want seed 7 and temperature 0.2", p)
	}
	if first.Step.StartedAt < 1_700_000_000_000_000 {
		t.Errorf("started_at = %d; startTime was not read", first.Step.StartedAt)
	}
	if first.Step.LatencyMs == nil {
		t.Error("latency not derived from startTime/endTime")
	}
	if first.Step.Provider == nil || *first.Step.Provider != "openai" {
		t.Errorf("provider = %v", first.Step.Provider)
	}
	if first.Step.CostUSD == nil || *first.Step.CostUSD <= 0 {
		t.Errorf("cost = %v", first.Step.CostUSD)
	}
	if first.Step.TokenCounts == nil || *first.Step.TokenCounts.Input != 10 {
		t.Errorf("tokens = %+v", first.Step.TokenCounts)
	}
	if first.Meta.TaskType != "refund" {
		t.Errorf("task_type = %q", first.Meta.TaskType)
	}

	// The caller's IP address and user agent identify a person, not a
	// trajectory, and are excluded from raw.
	for _, k := range []string{"requester_ip_address", "user_agent"} {
		if _, kept := first.Step.Raw[k]; kept {
			t.Errorf("%s was kept in raw", k)
		}
	}

	// A failed call is a failure, not a success with empty output.
	if !strings.Contains(failed[0].Error.Message, "Invalid model name") {
		t.Errorf("error = %q", failed[0].Error.Message)
	}

	// The tool call is rebuilt from history: args the model asked for, and
	// the result fed back, addressable as $.args / $.result.
	if len(tool) != 1 {
		t.Fatalf("got %d tool steps, want 1", len(tool))
	}
	ts := tool[0]
	if ts.SpanID != "tool:call_abc123" || *ts.Step.ToolName != "zendesk_update_ticket" {
		t.Errorf("tool step = %s %v", ts.SpanID, ts.Step.ToolName)
	}
	for _, want := range []string{`"args":{"id":"TKT-77"`, `"result":{"ok":true`} {
		if !strings.Contains(*ts.Step.ContentInline, want) {
			t.Errorf("tool payload missing %s: %s", want, *ts.Step.ContentInline)
		}
	}
	if ts.Step.ToolVersion != nil {
		t.Error("a tool version was invented; the gateway never sees one")
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
