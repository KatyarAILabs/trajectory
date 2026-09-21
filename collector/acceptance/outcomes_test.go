// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/sink/lake"
	"github.com/trajectory-project/trajectory/collector/sink/objstore"
	"github.com/trajectory-project/trajectory/pkg/export"
	"github.com/trajectory-project/trajectory/pkg/join"
	"github.com/trajectory-project/trajectory/pkg/lakeread"
	"github.com/trajectory-project/trajectory/pkg/record"
	"github.com/trajectory-project/trajectory/pkg/scorer"
	"github.com/trajectory-project/trajectory/pkg/scorer/rules"
	"github.com/trajectory-project/trajectory/spec/conformance"
)

// The outcome join, end to end: capture → outcomes → join → score → export.
//
// This is the differentiator, so the test asserts the properties that make it
// one rather than the plumbing: an outcome keyed by a raw email joins to an
// episode that only ever stored that email as a token; last-touch and as-of
// hold; rewards come from rules; and the preference export pairs the rollouts
// that did best and worst on the same task.
func TestOutcomeJoinEndToEnd(t *testing.T) {
	dir := t.TempDir()
	lakeDir := filepath.Join(dir, "lake")
	t.Setenv("CC_HMAC_KEY", "outcome-test-key")

	cfg := testConfig(lakeDir)
	cfg.Sources = []config.Source{nativeSource("127.0.0.1:44360")}
	cfg.Entities = []config.Entity{{
		Tool: "zendesk.update_ticket",
		Keys: map[string]string{"ticket_id": "$.args.id", "requester": "$.args.requester"},
	}}

	_, stop := runService(t, cfg)
	waitForListener(t, "127.0.0.1:44360")
	base := "http://127.0.0.1:44360"

	// Four rollouts in two groups. Each updates a ticket for a customer.
	runs := []struct{ id, group, ticket, email string }{
		{"run-a", "task-1", "TKT-1", "ann@example.com"},
		{"run-b", "task-1", "TKT-2", "bob@example.com"},
		{"run-c", "task-2", "TKT-3", "cat@example.com"},
		{"run-d", "task-2", "TKT-4", "dan@example.com"},
	}
	start := time.Now()
	for _, r := range runs {
		post(t, base+"/v1/episodes", map[string]any{
			"episode_id": r.id, "group_id": r.group, "task_type": "refund",
			"steps": []map[string]any{
				{"kind": "llm", "model": "m", "params": map[string]any{"seed": 1},
					"content": `{"input":"refund please","output":"on it"}`},
				{"kind": "tool", "tool_name": "zendesk.update_ticket", "tool_version": "2.0",
					"content": fmt.Sprintf(`{"args":{"id":%q,"requester":%q},"result":{"ok":true}}`, r.ticket, r.email)},
			},
		})
	}
	time.Sleep(200 * time.Millisecond)

	// Outcomes, as a customer's nightly job would post them — with RAW
	// identifiers. run-a's is keyed by the customer's email, which the lake
	// only ever stored as a token.
	later := start.Add(time.Minute).UTC().Format(time.RFC3339)
	post(t, base+"/v1/outcomes", []map[string]any{
		{"outcome_id": "o1", "entity_name": "requester", "entity_key": "ann@example.com",
			"kind": "refund_status", "value": "completed", "occurred_at": later},
		{"outcome_id": "o2", "entity_name": "ticket_id", "entity_key": "TKT-2",
			"kind": "ticket_status", "value": "reopened", "occurred_at": later},
		{"outcome_id": "o3", "entity_name": "ticket_id", "entity_key": "TKT-3",
			"kind": "refund_status", "value": "completed", "occurred_at": later},
		// A duplicate re-post must count once.
		{"outcome_id": "o3", "entity_name": "ticket_id", "entity_key": "TKT-3",
			"kind": "refund_status", "value": "completed", "occurred_at": later},
	})
	stop()

	// The raw emails reached the collector twice, in episodes and outcomes,
	// and must be nowhere on disk.
	for _, r := range runs {
		assertNoLeak(t, lakeDir, r.email)
	}

	if rep := conformance.Check(mustLoad(t, lakeDir)); !rep.Passed() {
		t.Fatalf("lake fails conformance:\n%s", rep)
	}

	l, err := lakeread.Load(lakeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Outcomes) != 4 {
		t.Fatalf("stored %d outcome rows, want 4 (the duplicate is deduplicated at join time)", len(l.Outcomes))
	}

	asOf := time.Now().Add(48 * time.Hour)
	labels, st := join.Run(l, join.Options{AsOf: asOf, Horizon: time.Hour})
	got := map[string]join.Label{}
	for _, lb := range labels {
		got[lb.EpisodeID] = lb
	}

	// The token-keyed join: a raw email on the outcome met a token in the lake.
	if got["run-a"].Latest["refund_status"] != "completed" {
		t.Errorf("run-a: the outcome keyed by a raw email did not join to its tokenized key; latest=%v",
			got["run-a"].Latest)
	}
	if got["run-b"].Latest["ticket_status"] != "reopened" {
		t.Errorf("run-b latest = %v", got["run-b"].Latest)
	}
	if len(got["run-d"].Outcomes) != 0 {
		t.Errorf("run-d has outcomes it should not: %v", got["run-d"].Outcomes)
	}
	if st.Duplicates != 1 || st.Attributed != 3 || st.Unattributed != 0 {
		t.Errorf("join stats = %+v; want 3 attributed, 1 duplicate", st)
	}
	for _, lb := range labels {
		if lb.LabelStatus != join.StatusFinal {
			t.Errorf("%s is %s, want final", lb.EpisodeID, lb.LabelStatus)
		}
	}

	// Score with rules, and write rewards into the lake the way cc score does.
	sc, err := rules.Parse([]byte(`
verifier_id: refund.resolved
version: "1"
rules:
  - {clause: reopened, when: "latest['ticket_status'] == 'reopened'", reward: 0.0}
  - {clause: refund_completed, when: "latest['refund_status'] == 'completed'", reward: 1.0}
default_reward: 0.5
`), "test")
	if err != nil {
		t.Fatal(err)
	}
	var rewards []record.Reward
	for _, lb := range labels {
		ep, _ := l.Episode(lb.EpisodeID)
		r, ok, err := sc.Score(context.Background(), scorer.Episode{
			Episode: ep.Episode, Latest: lb.Latest, LabelStatus: lb.LabelStatus,
		})
		if err != nil || !ok {
			t.Fatalf("%s: ok=%v err=%v", lb.EpisodeID, ok, err)
		}
		r.At = asOf.UnixMicro()
		rewards = append(rewards, r)
	}
	store, _ := objstore.NewFS(lakeDir)
	sink, _ := lake.New(lake.Options{Name: "score", Store: store})
	sink.WriteRewards(context.Background(), rewards)
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	l, _ = lakeread.Load(lakeDir)
	want := map[string]float64{"run-a": 1, "run-b": 0, "run-c": 1, "run-d": 0.5}
	for _, r := range l.Rewards {
		if r.Reward != want[r.EpisodeID] {
			t.Errorf("%s reward %v, want %v", r.EpisodeID, r.Reward, want[r.EpisodeID])
		}
	}
	if rep := conformance.Check(mustLoad(t, lakeDir)); !rep.Passed() {
		t.Fatalf("lake with rewards fails conformance:\n%s", rep)
	}

	// Preference export: within each task group, best vs worst rollout.
	var buf bytes.Buffer
	pst, err := export.Run(&buf, l, labels, export.Options{Format: export.FormatPreference, RequireFinal: true})
	if err != nil {
		t.Fatal(err)
	}
	if pst.Written != 2 {
		t.Fatalf("wrote %d preference pairs, want 2 (one per group)", pst.Written)
	}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var pair struct {
			GroupID string `json:"group_id"`
			Chosen  struct {
				EpisodeID string `json:"episode_id"`
			} `json:"chosen"`
			Rejected struct {
				EpisodeID string `json:"episode_id"`
			} `json:"rejected"`
		}
		json.Unmarshal([]byte(line), &pair)
		wantPair := map[string][2]string{"task-1": {"run-a", "run-b"}, "task-2": {"run-c", "run-d"}}[pair.GroupID]
		if pair.Chosen.EpisodeID != wantPair[0] || pair.Rejected.EpisodeID != wantPair[1] {
			t.Errorf("%s: chosen %s over %s, want %s over %s", pair.GroupID,
				pair.Chosen.EpisodeID, pair.Rejected.EpisodeID, wantPair[0], wantPair[1])
		}
	}

	// Chat export at a reward bar keeps only the episodes that cleared it.
	buf.Reset()
	one := 1.0
	cst, _ := export.Run(&buf, l, labels, export.Options{Format: export.FormatChat, MinReward: &one, RequireFinal: true})
	if cst.Written != 2 || cst.Dropped["below_min_reward"] != 2 {
		t.Errorf("chat export: written %d, dropped %v; want 2 examples and 2 below the bar", cst.Written, cst.Dropped)
	}
	var ex struct {
		Messages []struct{ Role, Content string } `json:"messages"`
	}
	first := strings.SplitN(buf.String(), "\n", 2)[0]
	if err := json.Unmarshal([]byte(first), &ex); err != nil {
		t.Fatalf("chat example is not JSON: %v", err)
	}
	last := ex.Messages[len(ex.Messages)-1]
	if len(ex.Messages) != 2 || ex.Messages[0].Role != "user" || last.Role != "assistant" || last.Content != "on it" {
		t.Errorf("chat example is not user→assistant: %+v", ex.Messages)
	}
}

func post(t *testing.T, url string, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		var e bytes.Buffer
		e.ReadFrom(resp.Body)
		t.Fatalf("POST %s: status %d: %s", url, resp.StatusCode, e.String())
	}
}

func mustLoad(t *testing.T, dir string) *conformance.Dataset {
	t.Helper()
	d, err := conformance.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
