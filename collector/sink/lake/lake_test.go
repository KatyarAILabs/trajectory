// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package lake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/sink/objstore"
	"github.com/trajectory-project/trajectory/pkg/record"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newSink(t *testing.T, tweak func(*Options)) (*Sink, string, *clock) {
	t.Helper()
	dir := t.TempDir()
	fs, err := objstore.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	clk := &clock{t: time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)}
	n := 0
	o := Options{
		Name: "t", Store: fs, Now: clk.now,
		NewID: func() string { n++; return fmt.Sprintf("b%03d", n) },
	}
	if tweak != nil {
		tweak(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir, clk
}

func ep(id string, payload string) *pipeline.Assembled {
	return &pipeline.Assembled{
		Episode: record.Episode{EpisodeID: id, Tenant: "acme", Source: "t",
			Status: record.StatusComplete, StepCount: 1,
			StartedAt: time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC).UnixMicro()},
		Steps: []record.Step{{EpisodeID: id, Kind: record.KindLLM,
			ContentInline: &payload, Trainable: record.TrainableUnknown}},
	}
}

func files(t *testing.T, dir, table string) int {
	t.Helper()
	n := 0
	filepath.Walk(filepath.Join(dir, table), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
			n++
		}
		return nil
	})
	return n
}

// F-9.5: rows accumulate until a partition reaches the size target, so files
// trend toward a compaction-friendly size instead of one object per flush.
func TestRollsBySize(t *testing.T) {
	s, dir, _ := newSink(t, func(o *Options) {
		o.TargetFileBytes = 4000
		o.RollInterval = time.Hour
	})
	ctx := context.Background()

	s.Write(ctx, []*pipeline.Assembled{ep("a", "small")})
	s.FlushDue(ctx)
	if n := files(t, dir, record.TableEpisodes); n != 0 {
		t.Fatalf("wrote %d files below the size target", n)
	}

	for i := 0; i < 10; i++ {
		s.Write(ctx, []*pipeline.Assembled{ep(fmt.Sprintf("b%d", i), strings.Repeat("x", 500))})
	}
	s.FlushDue(ctx)
	if n := files(t, dir, record.TableEpisodes); n != 1 {
		t.Fatalf("got %d episode files after crossing the size target, want 1", n)
	}
	if s.PendingRows() != 0 {
		t.Errorf("%d rows left pending after the partition rolled", s.PendingRows())
	}
}

// F-9.5: a quiet partition still lands within the roll interval, so a
// low-traffic tenant's data is not held indefinitely.
func TestRollsByTime(t *testing.T) {
	s, dir, clk := newSink(t, func(o *Options) {
		o.TargetFileBytes = 1 << 30
		o.RollInterval = 15 * time.Minute
	})
	ctx := context.Background()

	s.Write(ctx, []*pipeline.Assembled{ep("a", "small")})
	clk.t = clk.t.Add(10 * time.Minute)
	s.FlushDue(ctx)
	if files(t, dir, record.TableEpisodes) != 0 {
		t.Fatal("rolled before the interval elapsed")
	}

	clk.t = clk.t.Add(6 * time.Minute)
	s.FlushDue(ctx)
	if files(t, dir, record.TableEpisodes) != 1 {
		t.Fatal("did not roll once the interval elapsed")
	}
}

// F-9.7: a payload over the hard maximum is truncated with an explicit marker,
// and the rest of the trajectory is kept rather than the record failing.
func TestTruncatesWithMarker(t *testing.T) {
	s, _, _ := newSink(t, func(o *Options) {
		o.MaxPayloadBytes = 100
		o.BlobThresholdBytes = 1000
	})
	e := ep("a", strings.Repeat("y", 500))
	if err := s.Write(context.Background(), []*pipeline.Assembled{e}); err != nil {
		t.Fatalf("an oversized payload failed the record: %v", err)
	}
	st := e.Steps[0]
	if !st.Truncated {
		t.Error("truncated flag not set")
	}
	if st.ContentInline == nil || len(*st.ContentInline) != 100 {
		t.Errorf("payload length = %v, want 100", st.ContentInline)
	}
	if s.Stats().Truncated != 1 {
		t.Errorf("truncation not counted")
	}
}

// F-9.3: an identical payload is stored once.
func TestBlobDedup(t *testing.T) {
	s, dir, _ := newSink(t, func(o *Options) { o.BlobThresholdBytes = 10 })
	big := strings.Repeat("the same system prompt ", 50)
	for i := 0; i < 5; i++ {
		s.Write(context.Background(), []*pipeline.Assembled{ep(fmt.Sprintf("e%d", i), big)})
	}
	var n int
	filepath.Walk(filepath.Join(dir, "blobs", "sha256"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n++
		}
		return nil
	})
	if n != 1 {
		t.Errorf("stored %d copies of one payload, want 1", n)
	}
	if st := s.Stats(); st.BlobsNew != 1 || st.BlobsDeduped != 4 {
		t.Errorf("stats = %+v, want 1 new and 4 deduped", st)
	}
}

// A producer-supplied task_type must not escape the prefix.
func TestPartitionValueCannotEscape(t *testing.T) {
	s, _, _ := newSink(t, nil)
	e := ep("a", "x")
	tt := "../../etc/passwd"
	e.Episode.TaskType = &tt
	key := s.partitionKey(e.Episode)
	if strings.Contains(key, "..") {
		t.Errorf("partition key %q contains a traversal", key)
	}
}
