// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/service"
	"github.com/trajectory-project/trajectory/collector/sink/lake"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// TestDoD1 is the Phase 1 exit criterion.
//
// A recorded OpenInference span stream, posted to the OTLP/HTTP endpoint,
// produces episodes and steps Parquet plus externalised blobs on local disk,
// from which the episode can be reconstructed in order with payloads verbatim
// and a seeded email present only as its HMAC token.
//
// Everything asserted below is a requirement, not a preference. If this test
// passes, the walking skeleton has touched every stage of the pipeline for
// real: receive, normalise, assemble, redact, extract, externalise, write.
func TestDoD1(t *testing.T) {
	root := t.TempDir()
	lake := filepath.Join(root, "lake")

	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")
	cfg := testConfig(lake)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := service.New(cfg, log)
	if err != nil {
		t.Fatalf("service.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	addr := "http://" + cfg.Sources[0].HTTP.Listen + "/v1/traces"
	waitForListener(t, cfg.Sources[0].HTTP.Listen)

	// Post the fixture as OTLP/protobuf, exactly as an instrumented app
	// would.
	req := ptraceotlp.NewExportRequestFromTraces(OpenInferenceFixture())
	body, err := req.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(addr, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post traces: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post traces: status %d", resp.StatusCode)
	}

	// Shut down, which drains assembly and flushes the sink.
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("service.Run: %v", err)
	}
	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// --- Reconstruct from what is actually on disk ---

	episodes := readTable[record.Episode](t, lake, record.TableEpisodes)
	steps := readTable[record.Step](t, lake, record.TableSteps)
	blobs := readTable[record.Blob](t, lake, record.TableBlobs)

	if len(episodes) != 1 {
		t.Fatalf("got %d episodes, want 1", len(episodes))
	}
	ep := episodes[0]

	if ep.Status != record.StatusComplete {
		t.Errorf("status = %q, want %q (the fixture sets episode.end)", ep.Status, record.StatusComplete)
	}
	if ep.Tenant != "acme" {
		t.Errorf("tenant = %q, want acme", ep.Tenant)
	}
	if ep.Source != "otlp" {
		t.Errorf("source = %q, want otlp (F-1.6)", ep.Source)
	}
	if ep.StepCount != 4 {
		t.Errorf("step_count = %d, want 4", ep.StepCount)
	}
	if ep.SchemaVersion == "" {
		t.Error("schema_version is empty (F-10.2)")
	}
	if ep.Instrumentation == nil || *ep.Instrumentation != "openinference-langchain" {
		t.Errorf("instrumentation = %v, want openinference-langchain (F-2.3)", ep.Instrumentation)
	}

	// --- Ordering and tree (F-3.4) ---

	if len(steps) != 4 {
		t.Fatalf("got %d steps, want 4", len(steps))
	}
	for i, s := range steps {
		if s.StepIdx != int32(i) {
			t.Errorf("step %d: StepIdx = %d", i, s.StepIdx)
		}
		if s.EpisodeID != ep.EpisodeID {
			t.Errorf("step %d belongs to episode %q, want %q", i, s.EpisodeID, ep.EpisodeID)
		}
	}
	// The retry: steps 1 and 2 are both tool calls branching from step 0.
	// A collector that flattened retries would show a linear chain here.
	if steps[1].ParentIdx == nil || *steps[1].ParentIdx != 0 {
		t.Errorf("step 1 parent = %v, want 0", steps[1].ParentIdx)
	}
	if steps[2].ParentIdx == nil || *steps[2].ParentIdx != 0 {
		t.Errorf("step 2 parent = %v, want 0 (the retry must branch, not chain)", steps[2].ParentIdx)
	}
	if steps[1].Kind != record.KindTool || steps[2].Kind != record.KindTool {
		t.Errorf("steps 1,2 kinds = %q,%q, want tool,tool", steps[1].Kind, steps[2].Kind)
	}

	// --- Replay fidelity (F-4.2) ---

	if steps[0].Model == nil || *steps[0].Model != "claude-opus-5" {
		t.Errorf("step 0 model = %v", steps[0].Model)
	}
	if steps[0].Params == nil {
		t.Fatal("step 0 has no params; the fixture supplied invocation parameters")
	}
	if steps[0].Params.Seed == nil || *steps[0].Params.Seed != 7 {
		t.Errorf("seed = %v, want 7 — without it the step cannot be replayed", steps[0].Params.Seed)
	}
	if steps[0].Params.Temperature == nil || *steps[0].Params.Temperature != 0.2 {
		t.Errorf("temperature = %v, want 0.2", steps[0].Params.Temperature)
	}
	if steps[0].TokenCounts == nil || steps[0].TokenCounts.Input == nil || *steps[0].TokenCounts.Input != 3200 {
		t.Errorf("input token count = %v, want 3200", steps[0].TokenCounts)
	}
	if steps[1].ToolVersion == nil || *steps[1].ToolVersion != "2.3.1" {
		t.Errorf("tool_version = %v, want 2.3.1", steps[1].ToolVersion)
	}

	// --- Lossless normalisation (F-2.2) ---

	if got := steps[0].Raw["acme.internal.experiment"]; got != "arm-b" {
		t.Errorf("unmapped attribute lost: raw[acme.internal.experiment] = %q, want arm-b", got)
	}

	// --- Blob externalisation and dedup (F-9.3) ---

	if steps[0].ContentRef == nil {
		t.Fatal("step 0 payload is over the blob threshold but was not externalised")
	}
	if steps[0].ContentInline != nil {
		t.Error("step 0 has both content_ref and content_inline; exactly one carries the payload")
	}
	if len(blobs) == 0 {
		t.Fatal("no blob rows written")
	}
	blobPath := filepath.Join(lake, record.TableBlobs, "sha256",
		(*steps[0].ContentRef)[0:2], (*steps[0].ContentRef)[2:4], *steps[0].ContentRef)
	blobBytes, err := os.ReadFile(blobPath)
	if err != nil {
		t.Fatalf("content_ref points at a blob that does not exist: %v", err)
	}
	if !strings.Contains(string(blobBytes), bigPrompt) {
		t.Error("blob does not contain the original prompt verbatim (F-4.1)")
	}

	// --- Redaction (F-5) ---

	// The seeded email must not appear anywhere: not in a Parquet payload,
	// not in a blob, not in the manifest.
	assertNoLeak(t, lake, seededEmail)

	// ...but the tool call that carried it must still be present, with the
	// value tokenized rather than removed.
	toolPayload := payloadOf(t, lake, steps[1])
	if !strings.Contains(toolPayload, "tok_") {
		t.Errorf("email was not tokenized; payload: %.200s", toolPayload)
	}
	if !strings.Contains(toolPayload, "TKT-9001") {
		t.Error("redaction removed the ticket id, which is not sensitive and is needed for the join")
	}

	// The same identifier appears in both tool calls, so both must carry
	// the identical token — that is what makes it joinable (F-5.2).
	retryPayload := payloadOf(t, lake, steps[2])
	if tok1, tok2 := findToken(toolPayload), findToken(retryPayload); tok1 == "" || tok1 != tok2 {
		t.Errorf("the same email tokenized differently across steps: %q vs %q", tok1, tok2)
	}

	// --- Entity extraction (F-6) ---

	var ticket string
	for _, k := range ep.EntityKeys {
		if k.Name == "ticket_id" {
			ticket = k.Value
		}
	}
	if ticket != "TKT-9001" {
		t.Errorf("entity_keys ticket_id = %q, want TKT-9001", ticket)
	}

	// --- Manifest written last and describing the batch (F-9.6) ---

	m := readManifest(t, lake)
	if m.SchemaVersion == "" {
		t.Error("manifest has no schema_version")
	}
	if m.Counts.Episodes != 1 || m.Counts.Steps != 4 {
		t.Errorf("manifest counts = %d episodes / %d steps, want 1/4", m.Counts.Episodes, m.Counts.Steps)
	}
	for _, f := range m.Files {
		if _, err := os.Stat(filepath.Join(lake, f.Path)); err != nil {
			t.Errorf("manifest lists %q but it does not exist: %v", f.Path, err)
		}
	}

	// --- Partitioning (F-9.2) ---

	// task_type comes from the producer's task.type attribute, which the
	// OpenInference mapping consumes. A partition of "unknown" here would
	// mean the convention table stopped reading it.
	want := filepath.Join(lake, record.TableEpisodes, "dt=2026-09-20", "tenant=acme", "task_type=refund")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("expected partition directory %s: %v", want, err)
	}
	if ep.TaskType == nil || *ep.TaskType != "refund" {
		t.Errorf("task_type = %v, want refund", ep.TaskType)
	}
}

// assertNoLeak walks every file under the lake and fails if the secret appears
// in any of them. This is the check that matters: it does not trust the
// pipeline's own accounting, it reads what actually landed on disk.
func assertNoLeak(t *testing.T, root, secret string) {
	t.Helper()

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(b, []byte(secret)) {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("SEEDED SECRET LEAKED into %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

func payloadOf(t *testing.T, root string, s record.Step) string {
	t.Helper()
	if s.ContentInline != nil {
		return *s.ContentInline
	}
	if s.ContentRef == nil {
		return ""
	}
	p := filepath.Join(root, record.TableBlobs, "sha256",
		(*s.ContentRef)[0:2], (*s.ContentRef)[2:4], *s.ContentRef)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	return string(b)
}

func findToken(s string) string {
	i := strings.Index(s, "tok_")
	if i < 0 {
		return ""
	}
	end := i
	for end < len(s) && (s[end] == '_' || isAlnum(s[end])) {
		end++
	}
	return s[i:end]
}

func isAlnum(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// readTable reads every Parquet file for a table, in path order.
func readTable[T any](t *testing.T, root, table string) []T {
	t.Helper()

	var out []T
	dir := filepath.Join(root, table)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return err
		}
		rows, err := parquet.ReadFile[T](path)
		if err != nil {
			return err
		}
		out = append(out, rows...)
		return nil
	})
	if err != nil {
		t.Fatalf("read table %s: %v", table, err)
	}
	return out
}

func readManifest(t *testing.T, root string) lake.Manifest {
	t.Helper()

	var found string
	_ = filepath.Walk(filepath.Join(root, "manifests"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".json") {
			found = path
		}
		return nil
	})
	if found == "" {
		t.Fatal("no manifest written; a reader would see no batch at all (F-9.6)")
	}

	b, err := os.ReadFile(found)
	if err != nil {
		t.Fatal(err)
	}
	var m lake.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	return m
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		resp, err := http.Post("http://"+addr+"/v1/traces", "application/x-protobuf", strings.NewReader(""))
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("source never started listening on %s", addr)
}

func testConfig(lakeDir string) *config.Config {
	cfg := &config.Config{
		SchemaVersion: "0.1.0",
		Tenant:        "acme",
	}
	src := config.Source{Name: "otlp", Type: "otlp"}
	src.HTTP.Listen = "127.0.0.1:44318"
	cfg.Sources = []config.Source{src}

	cfg.Assembly = config.Assembly{
		SessionKey:          []string{"session.id", "trace_id"},
		Window:              5 * time.Minute,
		SettleAfterTerminal: 50 * time.Millisecond,
		MaxInFlight:         100,
	}

	cfg.Redaction.Default = "deny"
	cfg.Redaction.OnError = "quarantine"
	cfg.Redaction.Allow = []string{"steps[*].content_inline", "steps[*].raw"}
	cfg.Redaction.Tokenization.KeyEnv = "CC_HMAC_KEY"
	rule := config.Rule{ID: "email", Action: "tokenize"}
	rule.Match.Regex = `[\w.+-]+@[\w-]+\.[\w.]+`
	cfg.Redaction.Rules = []config.Rule{rule}

	cfg.Entities = []config.Entity{
		{Tool: "zendesk.update_ticket", Keys: map[string]string{"ticket_id": "$.args.id"}},
	}

	cfg.Buffer.Dir = filepath.Join(filepath.Dir(lakeDir), "buffer")
	cfg.Buffer.MaxBytes = 64 << 20
	cfg.Buffer.SegmentBytes = 1 << 20
	cfg.Buffer.MaxAttempts = 3
	cfg.Buffer.RetryBaseDelay = time.Millisecond
	cfg.Buffer.RetryMaxDelay = 5 * time.Millisecond

	cfg.Sinks = []config.Sink{{
		Name: "local", Type: "fs", Dir: lakeDir,
		PartitionBy:        []string{"dt", "tenant", "task_type"},
		BlobThresholdBytes: 8192,
		MaxPayloadBytes:    8 << 20,
	}}
	return cfg
}
