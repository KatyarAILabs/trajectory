// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package filetail

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
)

func spanLine(session, span string) string {
	return fmt.Sprintf(`{"session_id":%q,"span_id":%q,"step":{"kind":"llm","content":"x"}}`+"\n", session, span)
}

// collect records envelopes. It is locked because Start runs the tailer on
// its own goroutine while the test reads what arrived.
type collect struct {
	mu   sync.Mutex
	envs []pipeline.Envelope
}

func (c *collect) next(_ context.Context, e pipeline.Envelope) error {
	c.mu.Lock()
	c.envs = append(c.envs, e)
	c.mu.Unlock()
	return nil
}

func (c *collect) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.envs)
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(s)
	f.Close()
}

func TestReadsNewLinesOnly(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	appendTo(t, log, spanLine("s1", "a"))

	tl := New(Options{Name: "tail", Include: []string{filepath.Join(dir, "*.jsonl")},
		StartAt: "beginning", StateDir: dir})
	c := &collect{}

	tl.PollOnce(context.Background(), c.next)
	if len(c.envs) != 1 {
		t.Fatalf("first poll: %d envelopes, want 1", len(c.envs))
	}

	appendTo(t, log, spanLine("s1", "b"))
	tl.PollOnce(context.Background(), c.next)
	if len(c.envs) != 2 {
		t.Fatalf("second poll: %d envelopes, want 2 — old lines were re-read or new ones missed", len(c.envs))
	}
}

// A restart must resume from the saved offset. Re-reading the whole file would
// turn every already-emitted line into a patch record and double the lake.
func TestOffsetsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	appendTo(t, log, spanLine("s1", "a")+spanLine("s1", "b"))

	opts := Options{Name: "tail", Include: []string{log}, StartAt: "beginning", StateDir: dir}
	first := New(opts)
	c1 := &collect{}
	first.PollOnce(context.Background(), c1.next)
	first.Shutdown(context.Background())

	appendTo(t, log, spanLine("s1", "c"))

	second := New(opts)
	second.loadState()
	c2 := &collect{}
	second.PollOnce(context.Background(), c2.next)

	if len(c2.envs) != 1 || c2.envs[0].SpanID != "c" {
		t.Errorf("after restart got %d envelopes (%v); want only the new line", len(c2.envs), ids(c2.envs))
	}
}

// A partial line is the writer mid-write. Parsing it would produce a corrupt
// record, so it waits for the next poll.
func TestPartialLineWaits(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	full := spanLine("s1", "a")
	appendTo(t, log, full[:20])

	tl := New(Options{Name: "t", Include: []string{log}, StartAt: "beginning", StateDir: dir})
	c := &collect{}
	tl.PollOnce(context.Background(), c.next)
	if len(c.envs) != 0 {
		t.Fatal("a partial line was parsed")
	}

	appendTo(t, log, full[20:])
	tl.PollOnce(context.Background(), c.next)
	if len(c.envs) != 1 {
		t.Fatalf("completed line not read: %d envelopes", len(c.envs))
	}
}

// Rotation by rename-and-recreate: the new file is read from the start.
func TestRotation(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	appendTo(t, log, spanLine("s1", "a")+spanLine("s1", "b"))

	tl := New(Options{Name: "t", Include: []string{log}, StartAt: "beginning", StateDir: dir})
	c := &collect{}
	tl.PollOnce(context.Background(), c.next)

	os.Rename(log, log+".1")
	appendTo(t, log, spanLine("s2", "z"))
	tl.PollOnce(context.Background(), c.next)

	if last := c.envs[len(c.envs)-1]; last.SpanID != "z" {
		t.Errorf("rotated file not read from the start; last span = %q", last.SpanID)
	}
}

// Kubernetes container stdout is CRI-wrapped, and interleaved with ordinary
// log lines that are not records.
func TestCRIUnwrapAndNonJSONSkipped(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "pod.log")
	appendTo(t, log,
		"2026-09-21T10:00:00.123456789Z stdout F "+spanLine("s1", "a")+
			"2026-09-21T10:00:01.000000000Z stderr F starting worker pool\n"+
			"plain text noise\n")

	tl := New(Options{Name: "t", Include: []string{log}, StartAt: "beginning", StateDir: dir})
	c := &collect{}
	tl.PollOnce(context.Background(), c.next)

	if len(c.envs) != 1 || c.envs[0].SessionKey != "s1" {
		t.Fatalf("got %v, want the one CRI-wrapped record", ids(c.envs))
	}
	if _, skipped := tl.Stats(); skipped != 2 {
		t.Errorf("skipped = %d, want 2 non-record lines counted", skipped)
	}
}

// Default start position is the end, so pointing the tailer at old logs does
// not silently backfill months of history.
func TestStartAtEndSkipsExistingContent(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	appendTo(t, log, spanLine("old", "a"))

	tl := New(Options{Name: "t", Include: []string{log}, StateDir: dir})
	ctx, cancel := context.WithCancel(context.Background())
	c := &collect{}
	done := make(chan struct{})
	go func() { tl.Start(ctx, c.next); close(done) }()

	// Let the first poll establish the end offset, then add a line.
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(filepath.Join(dir, "tail-t.json")); err == nil {
			break
		}
		sleep()
	}
	appendTo(t, log, spanLine("new", "b"))
	for i := 0; i < 50 && c.count() == 0; i++ {
		sleep()
	}
	cancel()
	<-done

	for _, e := range c.envs {
		if e.SessionKey == "old" {
			t.Error("pre-existing content was backfilled despite start_at: end")
		}
	}
	if len(c.envs) == 0 {
		t.Error("the line written after start was never read")
	}
}

// A refused line is retried rather than skipped: the offset does not move past
// data the pipeline did not accept (F-8.4).
func TestRefusedLineIsRetried(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "agent.jsonl")
	appendTo(t, log, spanLine("s1", "a"))

	tl := New(Options{Name: "t", Include: []string{log}, StartAt: "beginning", StateDir: dir})
	refuse := func(context.Context, pipeline.Envelope) error { return fmt.Errorf("backpressure") }
	tl.PollOnce(context.Background(), refuse)

	c := &collect{}
	tl.PollOnce(context.Background(), c.next)
	if len(c.envs) != 1 {
		t.Fatalf("refused line was not retried: %d envelopes", len(c.envs))
	}
}

func ids(es []pipeline.Envelope) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.SessionKey+"/"+e.SpanID)
	}
	return out
}
