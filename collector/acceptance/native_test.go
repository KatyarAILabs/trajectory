// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/service"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// UC-3 end to end: the native API is the only path that can carry token spans,
// trainable masks and tool versions, because no tracing convention models them.
// It is the highest-fidelity source and the reference implementation of the
// format, so it gets the same on-disk scrutiny as the OTLP path.
func TestNativeAPIEndToEnd(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")
	t.Setenv("CC_SDK_TOKEN", "test-token")

	cfg := testConfig(lake)
	src := config.Source{Name: "sdk", Type: "native"}
	src.HTTP.Listen = "127.0.0.1:44321"
	src.Auth.Type = "bearer"
	src.Auth.TokenEnv = "CC_SDK_TOKEN"
	cfg.Sources = []config.Source{src}
	cfg.Entities = []config.Entity{
		{Tool: "stripe.create_refund", Keys: map[string]string{"order_id": "$.result.metadata.order_id"}},
	}

	svc, err := service.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitForListener(t, "127.0.0.1:44321")

	body := []byte(`{
      "episode_id":"ep-native-1","tenant":"acme","task_type":"refund","group_id":"rollout-3",
      "status":"STATUS_COMPLETE","started_at":"1758362400000000",
      "steps":[
        {"kind":"KIND_LLM","step_idx":0,"started_at":1758362400000000,
         "model":"claude-opus-5",
         "content":"{\"input\":\"refund for carol@example.com\",\"output\":\"ok\"}",
         "params":{"temperature":0.2,"seed":"99"},
         "token_counts":{"input":"1200","output":"40"},
         "trainable":"TRAINABLE_TRUE",
         "token_spans":[{"start":0,"end":30,"trainable":"false"},
                        {"start":30,"end":34,"trainable":"true"}]},
        {"kind":"tool","step_idx":1,"parent_idx":0,"started_at":1758362401000000,
         "tool_name":"stripe.create_refund","tool_version":"4.1.0",
         "content":"{\"args\":{\"amount\":500},\"result\":{\"metadata\":{\"order_id\":\"ORD-77\"}}}"}
      ]}`)

	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:44321/v1/episodes", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v1/episodes: status %d, want 202", resp.StatusCode)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = svc.Shutdown(context.Background())

	episodes := readTable[record.Episode](t, lake, record.TableEpisodes)
	steps := readTable[record.Step](t, lake, record.TableSteps)

	if len(episodes) != 1 {
		t.Fatalf("got %d episodes, want 1", len(episodes))
	}
	ep := episodes[0]

	if ep.Status != record.StatusComplete {
		t.Errorf("status = %q, want complete", ep.Status)
	}
	// F-3.6: a producer-supplied group_id is carried; the collector never
	// invents one.
	if ep.GroupID == nil || *ep.GroupID != "rollout-3" {
		t.Errorf("group_id = %v, want rollout-3", ep.GroupID)
	}
	if ep.TaskType == nil || *ep.TaskType != "refund" {
		t.Errorf("task_type = %v, want refund", ep.TaskType)
	}

	if len(steps) != 2 {
		t.Fatalf("got %d steps, want 2", len(steps))
	}

	var llm, tool record.Step
	for _, s := range steps {
		switch s.Kind {
		case record.KindLLM:
			llm = s
		case record.KindTool:
			tool = s
		}
	}

	// F-4.4: token spans and the trainable mask survive. Nothing else can
	// carry these, which is the whole reason UC-3 exists.
	if llm.Trainable != record.TrainableTrue {
		t.Errorf("trainable = %q, want true", llm.Trainable)
	}
	if len(llm.TokenSpans) != 2 {
		t.Fatalf("got %d token spans, want 2", len(llm.TokenSpans))
	}
	if llm.TokenSpans[0].Trainable != record.TrainableFalse {
		t.Errorf("span 0 trainable = %q, want false", llm.TokenSpans[0].Trainable)
	}
	if llm.TokenSpans[1].Start != 30 || llm.TokenSpans[1].End != 34 {
		t.Errorf("span 1 = %+v", llm.TokenSpans[1])
	}

	// F-4.2: the seed arrived as a JSON string, per protojson's int64
	// encoding, and must decode to a number.
	if llm.Params == nil || llm.Params.Seed == nil || *llm.Params.Seed != 99 {
		t.Errorf("seed = %v, want 99", llm.Params)
	}
	if llm.TokenCounts == nil || llm.TokenCounts.Input == nil || *llm.TokenCounts.Input != 1200 {
		t.Errorf("token counts = %+v", llm.TokenCounts)
	}
	if tool.ToolVersion == nil || *tool.ToolVersion != "4.1.0" {
		t.Errorf("tool_version = %v, want 4.1.0", tool.ToolVersion)
	}

	// F-4.6: this episode has everything, so every fidelity flag is true.
	// An episode captured through the native API is the one case where
	// that should hold.
	if f := ep.Fidelity; f == nil || !f.HasParams || !f.HasTokenSpans || !f.HasToolVersions {
		t.Errorf("fidelity = %+v, want all true for a fully-specified native episode", ep.Fidelity)
	}

	// Redaction still applies to the native path.
	assertNoLeak(t, lake, "carol@example.com")

	// F-6: extraction reaches into the tool result.
	found := false
	for _, k := range ep.EntityKeys {
		if k.Name == "order_id" {
			found = true
			if k.Value != "ORD-77" {
				t.Errorf("order_id = %q, want ORD-77", k.Value)
			}
		}
	}
	if !found {
		t.Errorf("no order_id extracted; keys = %+v", ep.EntityKeys)
	}
}

// F-12.1: a source with a configured token rejects requests without it. The
// rejection must not distinguish absent from incorrect, which would be a probe
// oracle.
func TestNativeAPIRequiresToken(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")
	t.Setenv("CC_SDK_TOKEN", "test-token")

	cfg := testConfig(lake)
	src := config.Source{Name: "sdk", Type: "native"}
	src.HTTP.Listen = "127.0.0.1:44322"
	src.Auth.Type = "bearer"
	src.Auth.TokenEnv = "CC_SDK_TOKEN"
	cfg.Sources = []config.Source{src}

	svc, err := service.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitForListener(t, "127.0.0.1:44322")
	defer func() { cancel(); <-done }()

	body := `{"episode_id":"e1","steps":[{"kind":"llm"}]}`

	for _, tc := range []struct{ name, header string }{
		{"absent", ""},
		{"wrong", "Bearer nope"},
		{"malformed", "test-token"},
	} {
		req, _ := http.NewRequest(http.MethodPost,
			"http://127.0.0.1:44322/v1/episodes", bytes.NewReader([]byte(body)))
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s token: status %d, want 401", tc.name, resp.StatusCode)
		}
	}
}

// §9.4: a malformed outcome is refused with the field that is missing, rather
// than stored as a row that could never join to anything.
func TestOutcomesEndpointValidates(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")

	cfg := testConfig(lake)
	src := config.Source{Name: "sdk", Type: "native"}
	src.HTTP.Listen = "127.0.0.1:44323"
	cfg.Sources = []config.Source{src}

	svc, err := service.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitForListener(t, "127.0.0.1:44323")
	defer func() { cancel(); <-done }()

	resp, err := http.Post("http://127.0.0.1:44323/v1/outcomes", "application/json",
		bytes.NewReader([]byte(`{"entity_key":"x","kind":"k","occurred_at":"2026-09-21T00:00:00Z"}`)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d, want 400", resp.StatusCode)
	}
	if !bytes.Contains(body, []byte("entity_name")) {
		t.Errorf("error does not name the missing field: %s", body)
	}
}
