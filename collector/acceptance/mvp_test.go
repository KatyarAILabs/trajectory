// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/collector/service"
	v1 "github.com/KatyarAILabs/trajectory/gen/go/trajectory/v1"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// runService starts a service and returns a stop function that drains it.
func runService(t *testing.T, cfg *config.Config) (*service.Service, func()) {
	t.Helper()
	svc, err := service.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("service.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	return svc, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		_ = svc.Shutdown(context.Background())
	}
}

func nativeSource(listen string) config.Source {
	s := config.Source{Name: "sdk", Type: "native"}
	s.HTTP.Listen = listen
	return s
}

// F-1.2: the native API accepts protobuf, and it lands exactly as JSON would.
func TestNativeProtobufIngest(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44331")}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44331")

	seed := int64(42)
	body, err := proto.Marshal(&v1.EpisodeBatch{Episodes: []*v1.EpisodeWithSteps{{
		Episode: &v1.Episode{
			EpisodeId: "ep-proto", Tenant: "acme",
			Status: v1.Status_STATUS_COMPLETE, StartedAt: 1758362400000000,
		},
		Steps: []*v1.Step{{
			StepIdx: 0, Kind: v1.Kind_KIND_LLM, Trainable: v1.Trainable_TRAINABLE_TRUE,
			ContentInline: strPtr(`{"input":"hi","output":"there"}`),
			Params:        &v1.Params{Seed: &seed},
			StartedAt:     1758362400000000,
		}},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post("http://127.0.0.1:44331/v1/episodes", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	stop()

	steps := readTable[record.Step](t, lake, record.TableSteps)
	if len(steps) != 1 {
		t.Fatalf("got %d steps", len(steps))
	}
	if steps[0].Params == nil || steps[0].Params.Seed == nil || *steps[0].Params.Seed != 42 {
		t.Errorf("seed lost through protobuf: %+v", steps[0].Params)
	}
	if steps[0].Trainable != record.TrainableTrue {
		t.Errorf("trainable = %q", steps[0].Trainable)
	}
}

func strPtr(s string) *string { return &s }

// §9.2: /v1/spans is partial emit. Steps sent in separate requests are
// assembled into one episode by session, which is what an SDK relies on.
func TestSpansAreAssembledAcrossRequests(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44332")}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44332")

	post := func(span, parent string, terminal bool) {
		b, _ := json.Marshal(map[string]any{
			"session_id": "sess-x", "span_id": span, "parent_span_id": parent,
			"terminal": terminal, "task_type": "refund",
			"step": map[string]any{"kind": "llm", "content": `{"input":"x"}`,
				"started_at": time.Now().UnixMicro()},
		})
		resp, err := http.Post("http://127.0.0.1:44332/v1/spans", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("span %s: status %d", span, resp.StatusCode)
		}
	}
	post("a", "", false)
	post("b", "a", false)
	post("c", "b", true)
	stop()

	eps := readTable[record.Episode](t, lake, record.TableEpisodes)
	if len(eps) != 1 {
		t.Fatalf("three requests produced %d episodes, want 1", len(eps))
	}
	if eps[0].StepCount != 3 || eps[0].Status != record.StatusComplete {
		t.Errorf("episode = %d steps, status %q; want 3, complete", eps[0].StepCount, eps[0].Status)
	}
}

// F-7.3: a source over its rate limit gets 429, not 503, and the shed is
// counted so an operator can see which producer is responsible.
func TestQuotaAnswers429(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	src := nativeSource("127.0.0.1:44333")
	src.RateLimit.RecordsPerSecond = 1
	src.RateLimit.Burst = 2
	cfg.Sources = []config.Source{src}

	svc, stop := runService(t, cfg)
	defer stop()
	waitForListener(t, "127.0.0.1:44333")

	codes := map[int]int{}
	for i := 0; i < 6; i++ {
		b := fmt.Sprintf(`{"session_id":"s%d","span_id":"x","step":{"kind":"llm"}}`, i)
		resp, err := http.Post("http://127.0.0.1:44333/v1/spans", "application/json", strings.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		codes[resp.StatusCode]++
	}
	if codes[http.StatusTooManyRequests] == 0 {
		t.Fatalf("no 429 after exceeding the rate limit: %v", codes)
	}
	if codes[http.StatusAccepted] == 0 {
		t.Errorf("the burst allowance admitted nothing: %v", codes)
	}

	if !strings.Contains(scrape(t, svc), `cc_shed_total{reason="quota",source="sdk"}`) {
		t.Error("quota shed is not visible in cc_shed_total")
	}
}

// F-11.4: a reload swaps policy without dropping in-flight data, and an invalid
// reload leaves the running policy in force.
func TestHotReloadSwapsRedactionPolicy(t *testing.T) {
	dir := t.TempDir()
	lake := filepath.Join(dir, "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Redaction.Rules = nil // start with no rules: emails pass through
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44334")}

	svc, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44334")

	send := func(session, content string) {
		b, _ := json.Marshal(map[string]any{
			"session_id": session, "span_id": "a", "terminal": true,
			"step": map[string]any{"kind": "llm",
				"content": fmt.Sprintf(`{"input":%q}`, content)},
		})
		resp, err := http.Post("http://127.0.0.1:44334/v1/spans", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	// An invalid reload must be refused and change nothing.
	bad := *cfg
	bad.Redaction.Rules = []config.Rule{{ID: "broken", Action: "tokenize",
		Match: config.RuleMatch{Regex: "([unclosed"}}}
	if _, err := svc.Reload(&bad); err == nil {
		t.Fatal("an invalid policy was accepted on reload")
	}

	// A valid reload adds the email rule.
	good := *cfg
	rule := config.Rule{ID: "email", Action: "tokenize"}
	rule.Match.Regex = `[\w.+-]+@[\w-]+\.[\w.]+`
	good.Redaction.Rules = []config.Rule{rule}
	pending, err := svc.Reload(&good)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("a policy-only change reported restart-required sections: %v", pending)
	}

	send("after-reload", "contact dana@example.com")
	stop()

	assertNoLeak(t, lake, "dana@example.com")
	if !strings.Contains(scrape(t, svc), `cc_config_reloads_total{result="error"} 1`) {
		t.Error("the rejected reload was not counted")
	}
}

// A structural change is reported as needing a restart rather than applied.
func TestReloadReportsStructuralChanges(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44335")}
	svc, stop := runService(t, cfg)
	defer stop()

	next := *cfg
	next.Buffer.MaxBytes = cfg.Buffer.MaxBytes * 2
	pending, err := svc.Reload(&next)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != "buffer" {
		t.Errorf("pending = %v, want [buffer]", pending)
	}
}

// F-3.8: episodes lost to an unclean stop are counted on the next start.
func TestLostOnRestartIsCounted(t *testing.T) {
	dir := t.TempDir()
	lake := filepath.Join(dir, "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44336")}

	// Simulate what an unclean stop leaves: a marker saying episodes were
	// in flight, never marked clean.
	os.MkdirAll(cfg.Buffer.Dir, 0o700)
	os.WriteFile(filepath.Join(cfg.Buffer.Dir, "assembly.json"),
		[]byte(`{"in_flight":7,"clean":false,"updated_at":"2026-09-21T10:00:00Z","pid":1}`), 0o600)

	svc, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44336")
	defer stop()

	if !strings.Contains(scrape(t, svc), "cc_assembly_lost_on_restart_total 7") {
		t.Error("in-flight episodes lost to the previous unclean stop were not counted")
	}
}

// A clean shutdown marks assembly clean, so the next start reports no loss.
func TestCleanShutdownReportsNoLoss(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44337")}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44337")
	stop()

	b, err := os.ReadFile(filepath.Join(cfg.Buffer.Dir, "assembly.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"clean":true`) {
		t.Errorf("clean shutdown did not mark assembly clean: %s", b)
	}
}

// F-1.3: a gateway callback, through the shipped LiteLLM mapping, lands as an
// episode.
func TestGatewayWebhookEndToEnd(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	src := config.Source{Name: "litellm", Type: "webhook",
		Mapping: filepath.Join("..", "..", "mappings", "litellm.yaml")}
	src.HTTP.Listen = "127.0.0.1:44338"
	cfg.Sources = []config.Source{src}
	cfg.Entities = []config.Entity{
		{Tool: "zendesk_update_ticket", Keys: map[string]string{"ticket_id": "$.args.id"}},
	}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44338")

	// A real batch captured from LiteLLM 1.102.0's generic_api logger: two
	// calls in one session (one carrying a tool call and its result in its
	// history) and one failed call in another session.
	body, err := os.ReadFile(filepath.Join("..", "..", "spec", "testdata", "litellm-standard-logging-payload.json"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://127.0.0.1:44338/v1/hooks/litellm", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	stop()

	eps := readTable[record.Episode](t, lake, record.TableEpisodes)
	steps := readTable[record.Step](t, lake, record.TableSteps)

	var ok, failed *record.Episode
	for i := range eps {
		if eps[i].Error != nil {
			failed = &eps[i]
		} else {
			ok = &eps[i]
		}
	}
	if len(eps) != 2 || ok == nil || failed == nil {
		t.Fatalf("got %d episodes; want one from the session and one failed", len(eps))
	}
	// Two model calls plus the tool step rebuilt from history.
	if ok.StepCount != 3 {
		t.Errorf("session episode has %d steps, want 3 (2 llm + 1 tool)", ok.StepCount)
	}
	if ok.Instrumentation == nil || *ok.Instrumentation != "gateway-litellm" {
		t.Errorf("instrumentation = %v", ok.Instrumentation)
	}
	// Entity extraction reaches the tool arguments the gateway reconstructed.
	var ticket string
	for _, k := range ok.EntityKeys {
		if k.Name == "ticket_id" {
			ticket = k.Value
		}
	}
	if ticket != "TKT-77" {
		t.Errorf("ticket_id = %q; entity extraction cannot see gateway tool calls", ticket)
	}
	seeded := false
	for _, s := range steps {
		if s.Params != nil && s.Params.Seed != nil && *s.Params.Seed == 7 {
			seeded = true
		}
	}
	if !seeded {
		t.Error("the seed LiteLLM logged did not reach the lake")
	}
	assertNoLeak(t, lake, "jane@example.com")
}

// F-1.4: lines appended to a tailed file become episodes.
func TestFileTailEndToEnd(t *testing.T) {
	dir := t.TempDir()
	lake := filepath.Join(dir, "lake")
	logFile := filepath.Join(dir, "agent.jsonl")
	t.Setenv("CC_HMAC_KEY", "k")

	cfg := testConfig(lake)
	cfg.Sources = []config.Source{{Name: "tail", Type: "file",
		Include: []string{logFile}, StartAt: "beginning", PollInterval: 20 * time.Millisecond}}

	os.WriteFile(logFile, []byte(
		`{"session_id":"f1","span_id":"a","step":{"kind":"llm","content":"{\"input\":\"x\"}"}}`+"\n"+
			`{"session_id":"f1","span_id":"b","parent_span_id":"a","terminal":true,"step":{"kind":"tool","tool_name":"t"}}`+"\n"), 0o600)

	_, stop := runService(t, cfg)
	time.Sleep(300 * time.Millisecond)
	stop()

	eps := readTable[record.Episode](t, lake, record.TableEpisodes)
	if len(eps) != 1 || eps[0].StepCount != 2 {
		t.Fatalf("got %d episodes (%v)", len(eps), eps)
	}
	if eps[0].Source != "tail" {
		t.Errorf("source = %q", eps[0].Source)
	}
}

// F-11.3: a dry run reports what a config would do, without writing.
func TestDryRunWritesNothing(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)

	ep := episodeFor("dry-1", 1)
	content := `{"input":"mail frank@example.com"}`
	ep.Steps[0].ContentInline = &content

	results, err := service.DryRun(cfg, nil)
	if err != nil || len(results) != 0 {
		t.Fatalf("empty dry run: %v %v", results, err)
	}
	results, err = service.DryRun(cfg, []*pipeline.Assembled{ep})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(results[0].Redactions) == 0 {
		t.Fatalf("dry run reported no redaction: %+v", results)
	}
	if strings.Contains(results[0].String(), "frank@example.com") {
		t.Error("the dry-run report quotes a payload value")
	}
	if _, err := os.Stat(filepath.Join(lake, "episodes")); err == nil {
		t.Error("a dry run wrote episodes")
	}
}

func scrape(t *testing.T, svc *service.Service) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
	rec := &respRecorder{header: http.Header{}}
	svc.Metrics().Handler().ServeHTTP(rec, req)
	return rec.body.String()
}

type respRecorder struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func (r *respRecorder) Header() http.Header         { return r.header }
func (r *respRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *respRecorder) WriteHeader(c int)           { r.code = c }

// F-1.7: an oversized request is rejected with a clear error and a metric,
// rather than being buffered into memory.
func TestOversizedRequestRejectedWithMetric(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	src := nativeSource("127.0.0.1:44339")
	src.MaxRequestBytes = 1024
	cfg.Sources = []config.Source{src}

	svc, stop := runService(t, cfg)
	defer stop()
	waitForListener(t, "127.0.0.1:44339")

	big := `{"episode_id":"e","steps":[{"kind":"llm","content":"` + strings.Repeat("x", 4096) + `"}]}`
	resp, err := http.Post("http://127.0.0.1:44339/v1/episodes", "application/json", strings.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	if !strings.Contains(string(msg), "max_request_bytes") {
		t.Errorf("error does not say what limit was hit: %q", msg)
	}

	// The metric is published on the service's reporting tick.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrape(t, svc), `cc_ingest_records_total{result="rejected",source="sdk"} 1`) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Error("the rejection never appeared in cc_ingest_records_total")
}

// F-6.3: extraction coverage is a metric — the leading indicator that a future
// join will fail.
func TestEntityCoverageMetric(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44340")}

	svc, stop := runService(t, cfg)
	defer stop()
	waitForListener(t, "127.0.0.1:44340")

	// One episode touches the configured tool, one does not.
	for _, b := range []string{
		`{"session_id":"k1","span_id":"a","terminal":true,"step":{"kind":"tool","tool_name":"zendesk.update_ticket","content":"{\"args\":{\"id\":\"T-1\"}}"}}`,
		`{"session_id":"k2","span_id":"a","terminal":true,"step":{"kind":"llm","content":"{\"input\":\"x\"}"}}`,
	} {
		resp, err := http.Post("http://127.0.0.1:44340/v1/spans", "application/json", strings.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrape(t, svc), "cc_episodes_without_entity_keys_ratio 0.5") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Error("coverage ratio did not reach 0.5 for one keyed and one unkeyed episode")
}

// §9.1: POST /v1/episodes is idempotent on episode_id. A producer that lost
// the acknowledgement and retries — even after the episode was emitted — must
// not duplicate steps, and the episode keeps the id the producer sent.
func TestNativeEpisodesIdempotentOnEpisodeID(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(lake)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44341")}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44341")

	body := `{"episode_id":"producer-chosen-id","started_at":1758362400000000,"steps":[
	  {"kind":"llm","content":"{\"input\":\"a\"}"},{"kind":"tool","tool_name":"t"}]}`
	post := func() {
		resp, err := http.Post("http://127.0.0.1:44341/v1/episodes", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}

	post()
	time.Sleep(300 * time.Millisecond) // past settle: the episode is emitted
	post()                             // the retry after a lost acknowledgement
	stop()

	eps := readTable[record.Episode](t, lake, record.TableEpisodes)
	steps := readTable[record.Step](t, lake, record.TableSteps)

	if len(eps) != 1 {
		t.Errorf("got %d episode records, want 1 — the retry became a patch", len(eps))
	}
	if len(steps) != 2 {
		t.Errorf("got %d steps, want 2 — the retry duplicated steps", len(steps))
	}
	if len(eps) > 0 && eps[0].EpisodeID != "producer-chosen-id" {
		t.Errorf("episode_id = %q; the producer's id was replaced", eps[0].EpisodeID)
	}
}
