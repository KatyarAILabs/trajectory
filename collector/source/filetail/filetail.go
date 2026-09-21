// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package filetail tails JSONL files and container stdout (F-1.4).
//
// It exists for the producer that cannot be changed to send anything: an agent
// that already writes one JSON record per line to a file or to stdout. In
// Kubernetes that stdout lands in /var/log/containers/*.log wrapped in the CRI
// log format, which this package unwraps.
//
// Each line is either a span record (it has session_id) or a whole episode (it
// has episode_id), in the native JSON format.
//
// Offsets are persisted, and only after the pipeline accepted the lines, so
// delivery is at-least-once across restarts (F-8.4). Without persisted offsets
// a restart would re-read every file from the start, and lines arriving after
// their episode was emitted would come back as patch records — a restart would
// quietly double the lake.
package filetail

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/wire"
)

// Options configure a Tailer.
type Options struct {
	Name    string
	Include []string
	// StartAt is "end" or "beginning", and applies only to files that
	// already exist the first time the tailer sees them. Files that appear
	// later are always read from the start — they are new data. The default
	// is "end", so pointing the tailer at a directory of old logs does not
	// silently backfill months of history.
	StartAt      string
	PollInterval time.Duration
	// StateDir holds the offset file. It should be on the same persistent
	// volume as the buffer.
	StateDir string
	// MaxLineBytes bounds one line; a longer one is skipped and counted.
	MaxLineBytes int
}

// Tailer is a file source.
type Tailer struct {
	opts Options

	mu      sync.Mutex
	offsets map[string]fileState
	started bool

	bytes   atomic.Int64
	lines   atomic.Int64
	skipped atomic.Int64
}

type fileState struct {
	Inode  uint64 `json:"inode"`
	Offset int64  `json:"offset"`
}

// New creates a tailer.
func New(opts Options) *Tailer {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.StartAt == "" {
		opts.StartAt = "end"
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = 16 << 20
	}
	return &Tailer{opts: opts, offsets: map[string]fileState{}}
}

func (t *Tailer) Name() string                   { return t.opts.Name }
func (t *Tailer) BytesReceived() int64           { return t.bytes.Load() }
func (t *Tailer) Shutdown(context.Context) error { return t.saveState() }

// Stats reports lines read and skipped.
func (t *Tailer) Stats() (lines, skipped int64) { return t.lines.Load(), t.skipped.Load() }

// Start polls until ctx is cancelled.
func (t *Tailer) Start(ctx context.Context, next pipeline.Next) error {
	fresh := t.loadState() // no state file: this is the first run ever

	// Files present at the first-ever start are subject to StartAt; files
	// discovered later are read from the beginning.
	if fresh && t.opts.StartAt == "end" {
		for _, path := range t.match() {
			if st, ok := statFile(path); ok {
				t.offsets[path] = fileState{Inode: st.inode, Offset: st.size}
			}
		}
		_ = t.saveState()
	}

	ticker := time.NewTicker(t.opts.PollInterval)
	defer ticker.Stop()

	for {
		// A refusal from the pipeline (backpressure) is not fatal: the
		// offset was not advanced, so the lines are retried next poll.
		_ = t.pollOnce(ctx, next)
		select {
		case <-ctx.Done():
			return t.saveState()
		case <-ticker.C:
		}
	}
}

// PollOnce reads everything new across all matched files. Exported for tests
// and for `cc import`-style one-shot use.
func (t *Tailer) PollOnce(ctx context.Context, next pipeline.Next) error {
	return t.pollOnce(ctx, next)
}

func (t *Tailer) pollOnce(ctx context.Context, next pipeline.Next) error {
	for _, path := range t.match() {
		if err := t.readFile(ctx, path, next); err != nil {
			return err
		}
	}
	return t.saveState()
}

func (t *Tailer) match() []string {
	seen := map[string]bool{}
	var out []string
	for _, pat := range t.opts.Include {
		files, _ := filepath.Glob(pat)
		for _, f := range files {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	sort.Strings(out)
	return out
}

type stat struct {
	inode uint64
	size  int64
}

func statFile(path string) (stat, bool) {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return stat{}, false
	}
	var ino uint64
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		ino = uint64(sys.Ino)
	}
	return stat{inode: ino, size: info.Size()}, true
}

// readFile reads complete lines after the saved offset.
func (t *Tailer) readFile(ctx context.Context, path string, next pipeline.Next) error {
	st, ok := statFile(path)
	if !ok {
		return nil
	}

	t.mu.Lock()
	state, known := t.offsets[path]
	t.mu.Unlock()

	// Rotation: a new inode at the same path, or a file that shrank
	// (copytruncate), means start over from the beginning of the new data.
	if known && (state.Inode != st.inode || st.size < state.Offset) {
		state = fileState{Inode: st.inode}
	}
	if !known {
		state = fileState{Inode: st.inode}
	}
	if st.size == state.Offset {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	if _, err := f.Seek(state.Offset, io.SeekStart); err != nil {
		return nil
	}

	r := bufio.NewReaderSize(f, 64<<10)
	offset := state.Offset

	for {
		line, _ := r.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			if err := t.handleLine(ctx, line, next); err != nil {
				// Refused: stop here without advancing past this
				// line, so it is retried.
				t.setOffset(path, fileState{Inode: st.inode, Offset: offset})
				return err
			}
			offset += int64(len(line))
			continue
		}
		// A trailing partial line is left for the next poll: the writer
		// has not finished it, and parsing it now would produce a
		// corrupt record.
		break
	}

	t.setOffset(path, fileState{Inode: st.inode, Offset: offset})
	return nil
}

func (t *Tailer) setOffset(path string, s fileState) {
	t.mu.Lock()
	t.offsets[path] = s
	t.mu.Unlock()
}

func (t *Tailer) handleLine(ctx context.Context, raw []byte, next pipeline.Next) error {
	t.bytes.Add(int64(len(raw)))

	line := strings.TrimSpace(string(raw))
	if line == "" {
		return nil
	}
	if len(line) > t.opts.MaxLineBytes {
		t.skipped.Add(1)
		return nil
	}

	line = unwrapCRI(line)
	if line == "" || line[0] != '{' {
		// Not JSON: ordinary log output interleaved with records on the
		// same stream, which is normal for stdout. Skipped, counted.
		t.skipped.Add(1)
		return nil
	}

	var probe struct {
		SessionID string `json:"session_id"`
		EpisodeID string `json:"episode_id"`
	}
	if err := json.Unmarshal([]byte(line), &probe); err != nil {
		t.skipped.Add(1)
		return nil
	}

	var envs []pipeline.Envelope
	switch {
	case probe.SessionID != "":
		recs, err := wire.DecodeSpans([]byte(line))
		if err != nil {
			t.skipped.Add(1)
			return nil
		}
		env, err := recs[0].ToEnvelope(t.opts.Name)
		if err != nil {
			t.skipped.Add(1)
			return nil
		}
		envs = append(envs, env)
	case probe.EpisodeID != "":
		eps, err := wire.DecodeEpisodes([]byte(line))
		if err != nil {
			t.skipped.Add(1)
			return nil
		}
		es, err := eps[0].ToEnvelopes(t.opts.Name, "")
		if err != nil {
			t.skipped.Add(1)
			return nil
		}
		envs = es
	default:
		t.skipped.Add(1)
		return nil
	}

	for _, env := range envs {
		if err := next(ctx, env); err != nil {
			return err
		}
	}
	t.lines.Add(1)
	return nil
}

// unwrapCRI strips the Kubernetes CRI log prefix:
//
//	2026-09-21T10:00:00.000000000Z stdout F {"session_id":...}
//
// A line that is not in that format is returned unchanged. Partial CRI lines
// (P) are not reassembled; an agent emitting records longer than the runtime's
// line limit should use the native API instead.
func unwrapCRI(line string) string {
	parts := strings.SplitN(line, " ", 4)
	if len(parts) == 4 && (parts[1] == "stdout" || parts[1] == "stderr") &&
		(parts[2] == "F" || parts[2] == "P") {
		if _, err := time.Parse(time.RFC3339Nano, parts[0]); err == nil {
			return strings.TrimSpace(parts[3])
		}
	}
	return line
}

func (t *Tailer) statePath() string {
	if t.opts.StateDir == "" {
		return ""
	}
	return filepath.Join(t.opts.StateDir, "tail-"+safe(t.opts.Name)+".json")
}

func safe(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == ':' {
			return '_'
		}
		return r
	}, s)
}

// loadState reads saved offsets. Returns true when there was none.
func (t *Tailer) loadState() bool {
	p := t.statePath()
	if p == "" {
		return true
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := json.Unmarshal(b, &t.offsets); err != nil {
		t.offsets = map[string]fileState{}
		return true
	}
	return false
}

func (t *Tailer) saveState() error {
	p := t.statePath()
	if p == "" {
		return nil
	}
	t.mu.Lock()
	b, err := json.Marshal(t.offsets)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("filetail: save offsets: %w", err)
	}
	return os.Rename(tmp, p)
}
