// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KatyarAILabs/trajectory/collector/buffer"
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/collector/sink/lake"
	"github.com/KatyarAILabs/trajectory/collector/sink/objstore"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// flakyStore fails a configurable number of writes, then succeeds. It stands in
// for the sink outage that the whole buffer exists to survive.
type flakyStore struct {
	inner objstore.Store

	mu        sync.Mutex
	failUntil time.Time
	failing   atomic.Bool
	puts      atomic.Int64
	failed    atomic.Int64
}

func (f *flakyStore) Put(ctx context.Context, key string, body []byte) error {
	f.puts.Add(1)
	if f.failing.Load() {
		f.failed.Add(1)
		return fmt.Errorf("simulated sink outage: 503 from object store")
	}
	return f.inner.Put(ctx, key, body)
}

func (f *flakyStore) Exists(ctx context.Context, key string) (bool, error) {
	if f.failing.Load() {
		return false, fmt.Errorf("simulated sink outage")
	}
	return f.inner.Exists(ctx, key)
}

func (f *flakyStore) Get(ctx context.Context, key string) ([]byte, error) {
	return f.inner.Get(ctx, key)
}
func (f *flakyStore) Describe() string { return "flaky(" + f.inner.Describe() + ")" }
func (f *flakyStore) Close() error     { return f.inner.Close() }

func episodeFor(id string, n int) *pipeline.Assembled {
	ep := &pipeline.Assembled{
		Episode: record.Episode{
			EpisodeID: id, Tenant: "acme", Source: "test",
			Status: record.StatusComplete, StepCount: int32(n),
			StartedAt: time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC).UnixMicro(),
		},
	}
	for i := 0; i < n; i++ {
		content := fmt.Sprintf("payload for %s step %d", id, i)
		ep.Steps = append(ep.Steps, record.Step{
			EpisodeID: id, StepIdx: int32(i), Kind: record.KindLLM,
			ContentInline: &content, Trainable: record.TrainableUnknown,
			StartedAt: ep.Episode.StartedAt,
		})
	}
	return ep
}

// G-4: no acknowledged data is lost when a sink is unreachable and the process
// is then killed. This is the property the whole buffer exists for, and it is
// the one a platform engineer will actually test before trusting the thing.
func TestChaosSinkOutageThenRestart(t *testing.T) {
	dir := t.TempDir()
	bufDir := filepath.Join(dir, "buffer")
	lakeDir := filepath.Join(dir, "lake")

	const total = 50

	// --- Phase 1: the sink is down. Everything must land in the buffer. ---

	b, err := buffer.Open(buffer.Options{
		Dir: bufDir, MaxBytes: 64 << 20, SegmentBytes: 8 << 10, MaxAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	fs, err := objstore.NewFS(lakeDir)
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyStore{inner: fs}
	flaky.failing.Store(true)

	sink, err := lake.New(lake.Options{Name: "chaos", Store: flaky, TargetFileBytes: 1})
	if err == nil {
		t.Log("sink constructed despite the store failing; schema descriptors will retry later")
	}
	if sink == nil {
		// Schema descriptors are written at construction, and the store
		// is down, so this is the expected path. Build it against a
		// working store and swap the failure on afterwards.
		flaky.failing.Store(false)
		sink, err = lake.New(lake.Options{Name: "chaos", Store: flaky, TargetFileBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		flaky.failing.Store(true)
	}

	send := func(ctx context.Context, payload []byte) error {
		var ep pipeline.Assembled
		if err := jsonUnmarshal(payload, &ep); err != nil {
			return err
		}
		if err := sink.Write(ctx, []*pipeline.Assembled{&ep}); err != nil {
			return err
		}
		return sink.Flush(ctx)
	}

	d := buffer.NewDeliverer(b, send, buffer.DeliveryOptions{
		MaxAttempts: 2, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		DeadLetterDir: filepath.Join(dir, "dlq"),
	})

	for i := 0; i < total; i++ {
		payload := jsonMarshal(t, episodeFor(fmt.Sprintf("ep-%03d", i), 2))
		if err := b.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Delivery attempts fail. The sink being down is transient, so records
	// must stay queued rather than being dead-lettered.
	_, _ = d.DrainOnce(context.Background())

	if st := d.Stats(); st.DeadLettered != 0 {
		t.Fatalf("%d records were dead-lettered during a transient outage; "+
			"they should have waited for the sink to return", st.DeadLettered)
	}

	if flaky.failed.Load() == 0 {
		t.Fatal("the simulated outage never fired")
	}
	if b.Bytes() == 0 {
		t.Fatal("the buffer is empty while the sink is down; data was lost")
	}

	// --- Phase 2: simulate kill -9 and restart. ---

	// A killed process releases its flock (the kernel does it) but never
	// runs its shutdown path, so the cursor it had in memory is lost.
	// Closing and then deleting the persisted cursor reproduces both halves
	// faithfully. The torn-segment half of an unclean stop is covered by
	// the buffer package's own tests, where a partial record can be written
	// deliberately.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(bufDir, "state.json")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	b2, err := buffer.Open(buffer.Options{
		Dir: bufDir, MaxBytes: 64 << 20, SegmentBytes: 8 << 10, MaxAge: time.Hour,
	})
	if err != nil {
		t.Fatalf("buffer did not recover after an unclean stop: %v", err)
	}
	defer b2.Close()

	// --- Phase 3: the sink recovers. Everything must arrive. ---

	flaky.failing.Store(false)

	var delivered []string
	send2 := func(ctx context.Context, payload []byte) error {
		var ep pipeline.Assembled
		if err := jsonUnmarshal(payload, &ep); err != nil {
			return err
		}
		delivered = append(delivered, ep.Episode.EpisodeID)
		if err := sink.Write(ctx, []*pipeline.Assembled{&ep}); err != nil {
			return err
		}
		return sink.Flush(ctx)
	}

	d2 := buffer.NewDeliverer(b2, send2, buffer.DeliveryOptions{
		MaxAttempts: 3, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		DeadLetterDir: filepath.Join(dir, "dlq"),
	})
	if _, err := d2.DrainOnce(context.Background()); err != nil {
		t.Fatalf("drain after recovery: %v", err)
	}

	// At-least-once (F-8.4): every episode must appear. Duplicates are
	// permitted by the contract and a reader deduplicates on episode_id, so
	// the assertion is on coverage, not on an exact count.
	seen := map[string]bool{}
	for _, id := range delivered {
		seen[id] = true
	}
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("ep-%03d", i)
		if !seen[id] {
			t.Errorf("%s was lost across the outage and restart", id)
		}
	}

	// And it must actually be on disk, not merely reported.
	episodes := readTable[record.Episode](t, lakeDir, record.TableEpisodes)
	onDisk := map[string]bool{}
	for _, e := range episodes {
		onDisk[e.EpisodeID] = true
	}
	if len(onDisk) != total {
		t.Errorf("%d distinct episodes on disk, want %d", len(onDisk), total)
	}
}

// §12: when the sink is unreachable for long enough to fill the disk, the
// buffer must bound itself and say what it dropped, rather than growing until
// the process is OOM-killed or the volume fills.
func TestChaosBufferBoundedUnderSustainedOutage(t *testing.T) {
	dir := t.TempDir()
	var evictions []string
	var mu sync.Mutex

	b, err := buffer.Open(buffer.Options{
		Dir:          filepath.Join(dir, "buffer"),
		MaxBytes:     32 << 10,
		SegmentBytes: 4 << 10,
		MaxAge:       time.Hour,
		OnEvict: func(reason string, _ int, _ int64) {
			mu.Lock()
			evictions = append(evictions, reason)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Far more data than the buffer can hold, with nothing draining it.
	for i := 0; i < 500; i++ {
		payload := jsonMarshal(t, episodeFor(fmt.Sprintf("ep-%04d", i), 1))
		if err := b.Append(payload); err != nil && err != buffer.ErrFull {
			t.Fatalf("append %d returned an unexpected error: %v", i, err)
		}
	}

	if got := b.Bytes(); got > 32<<10 {
		t.Errorf("buffer grew to %d bytes under sustained outage; bound is %d", got, 32<<10)
	}

	mu.Lock()
	n := len(evictions)
	mu.Unlock()
	if n == 0 {
		t.Error("data was dropped without any eviction being reported; §12 requires it be observable")
	}

	// Backpressure must be asserted so sources start refusing rather than
	// the buffer silently discarding more.
	if !b.UnderBackpressure() {
		t.Error("buffer is at its bound but reports no backpressure")
	}
}

// A store that fails only some writes must not leave a batch half-visible: the
// manifest is written last, so a reader ignoring unmanifested files sees an
// all-or-nothing batch (F-9.4).
func TestChaosPartialWriteLeavesNoVisibleBatch(t *testing.T) {
	dir := t.TempDir()
	fs, err := objstore.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}

	failOn := &failKeyStore{inner: fs, failSubstring: "steps/"}
	sink, err := lake.New(lake.Options{Name: "s", Store: failOn, TargetFileBytes: 1})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := sink.Write(ctx, []*pipeline.Assembled{episodeFor("ep-1", 3)}); err != nil {
		t.Fatal(err)
	}

	err = sink.Flush(ctx)
	if err == nil {
		t.Fatal("flush succeeded despite the steps write failing")
	}

	// No manifest means no batch, whatever stray files exist.
	manifests, _ := filepath.Glob(filepath.Join(dir, "manifests", "*", "*.json"))
	if len(manifests) != 0 {
		t.Errorf("a manifest was written despite a failed data write: %v", manifests)
	}

	// The rows must not have been dropped on the floor either: a retry has
	// to be able to write them.
	if sink.PendingRows() == 0 {
		t.Error("rows were discarded after a failed flush instead of being retained for retry")
	}
}

type failKeyStore struct {
	inner         objstore.Store
	failSubstring string
}

func (f *failKeyStore) Put(ctx context.Context, key string, body []byte) error {
	if f.failSubstring != "" && containsStr(key, f.failSubstring) {
		return fmt.Errorf("simulated failure writing %s", key)
	}
	return f.inner.Put(ctx, key, body)
}
func (f *failKeyStore) Exists(ctx context.Context, key string) (bool, error) {
	return f.inner.Exists(ctx, key)
}
func (f *failKeyStore) Get(ctx context.Context, key string) ([]byte, error) {
	return f.inner.Get(ctx, key)
}
func (f *failKeyStore) Describe() string { return f.inner.Describe() }
func (f *failKeyStore) Close() error     { return f.inner.Close() }

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
