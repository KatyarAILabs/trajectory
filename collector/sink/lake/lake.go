// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lake writes episodes to an object store in the §8 layout.
//
// It is storage-agnostic: local filesystem and S3 are two implementations of
// one Store interface, and everything that determines what a reader sees —
// partitioning, blob fan-out, file rolling, the manifest written last — lives
// here and is identical for both. That is what makes "it worked against MinIO"
// mean something about production.
//
// Two invariants carry the design:
//
//   - A reader never sees a partially written batch (F-9.4). Blobs are written
//     first because a step may reference one; then the data files; then the
//     manifest, which is what makes the batch real to a reader.
//   - Payloads above a threshold become content-addressed blobs, deduplicated
//     by sha256 (F-9.3). A system prompt repeated across a million episodes is
//     stored once.
package lake

import (
	"context"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/sink/objstore"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Options configure a Sink.
type Options struct {
	Name  string
	Store objstore.Store

	// PartitionBy is the directory partitioning under each table prefix
	// (F-9.2).
	PartitionBy []string
	// BlobThresholdBytes is the payload size above which content is
	// externalised (F-9.3).
	BlobThresholdBytes int
	// MaxPayloadBytes truncates with an explicit marker rather than failing
	// the record (F-9.7).
	MaxPayloadBytes int
	// TargetFileBytes is the size a partition's buffered rows must reach
	// before they are written, so files trend toward a compaction-friendly
	// size rather than being one tiny object per flush (F-9.5).
	TargetFileBytes int64
	// RollInterval forces a write even when TargetFileBytes is not reached,
	// so a low-traffic tenant's data still lands in bounded time (F-9.5).
	RollInterval time.Duration
	// Compression is "zstd", "snappy" or "none".
	Compression string

	Now   func() time.Time
	NewID func() string
}

// Sink writes Parquet, blobs and manifests to an object store.
type Sink struct {
	opts Options

	mu      sync.Mutex
	pending map[string]*partition
	// blobsSeen is this process's view of which blobs exist, so a repeated
	// payload is hashed and written once. It is a cache; the store's own
	// Exists is authoritative, which keeps the sink correct across restarts
	// and between two collectors sharing a prefix.
	blobsSeen map[string]bool
	stats     Stats
}

type partition struct {
	key       string
	episodes  []record.Episode
	steps     []record.Step
	bytes     int64
	firstSeen time.Time
}

// Stats are the counters this stage contributes to §11.
type Stats struct {
	FilesWritten    int64
	BytesWritten    int64
	EpisodesWritten int64
	StepsWritten    int64
	BlobsNew        int64
	BlobsDeduped    int64
	Truncated       int64
	Batches         int64
}

// New creates a sink.
func New(opts Options) (*Sink, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("lake: a store is required")
	}
	if opts.BlobThresholdBytes <= 0 {
		opts.BlobThresholdBytes = 8192
	}
	if opts.MaxPayloadBytes <= 0 {
		opts.MaxPayloadBytes = 8 << 20
	}
	if opts.TargetFileBytes <= 0 {
		opts.TargetFileBytes = 256 << 20
	}
	if opts.RollInterval <= 0 {
		opts.RollInterval = 15 * time.Minute
	}
	if len(opts.PartitionBy) == 0 {
		opts.PartitionBy = []string{"dt", "tenant", "task_type"}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewID == nil {
		opts.NewID = func() string { return ulid.Make().String() }
	}

	s := &Sink{
		opts:      opts,
		pending:   map[string]*partition{},
		blobsSeen: map[string]bool{},
	}
	if err := s.writeSchemaDescriptors(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

// Name implements pipeline.Sink.
func (s *Sink) Name() string { return s.opts.Name }

// Describe names the destination, without credentials.
func (s *Sink) Describe() string { return s.opts.Store.Describe() }

// Write buffers episodes, externalising payloads immediately so redaction's
// output is what gets hashed and nothing large sits in memory twice.
func (s *Sink) Write(ctx context.Context, eps []*pipeline.Assembled) error {
	blobs, err := s.externalise(ctx, eps)
	if err != nil {
		return fmt.Errorf("lake: externalise payloads: %w", err)
	}
	if len(blobs) > 0 {
		if err := s.writeBlobTable(ctx, blobs); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.opts.Now()
	for _, ep := range eps {
		key := s.partitionKey(ep.Episode)
		p, ok := s.pending[key]
		if !ok {
			p = &partition{key: key, firstSeen: now}
			s.pending[key] = p
		}
		p.episodes = append(p.episodes, ep.Episode)
		p.steps = append(p.steps, ep.Steps...)
		p.bytes += estimateBytes(ep)
	}
	return nil
}

// Flush writes every partition that is due, and all of them when force is set.
func (s *Sink) Flush(ctx context.Context) error { return s.flush(ctx, true) }

// FlushDue writes only partitions that have reached the size target or the roll
// interval (F-9.5), so files trend toward a compaction-friendly size instead of
// one small object per tick.
func (s *Sink) FlushDue(ctx context.Context) error { return s.flush(ctx, false) }

func (s *Sink) flush(ctx context.Context, force bool) error {
	s.mu.Lock()
	now := s.opts.Now()

	var due []*partition
	for key, p := range s.pending {
		if force || p.bytes >= s.opts.TargetFileBytes || now.Sub(p.firstSeen) >= s.opts.RollInterval {
			due = append(due, p)
			delete(s.pending, key)
		}
	}
	s.mu.Unlock()

	if len(due) == 0 {
		return nil
	}
	sort.Slice(due, func(i, j int) bool { return due[i].key < due[j].key })

	batchID := s.opts.NewID()
	var files []ManifestFile
	var counts ManifestCounts
	minT, maxT := int64(0), int64(0)

	for _, p := range due {
		epKey := path.Join(record.TableEpisodes, p.key, "part-"+batchID+".parquet")
		n, err := s.putParquet(ctx, epKey, p.episodes)
		if err != nil {
			// Put the rows back so a transient store failure does
			// not lose them; the caller will retry the flush.
			s.restore(p)
			return err
		}
		files = append(files, ManifestFile{
			Path: epKey, Rows: int64(len(p.episodes)), Bytes: n, Table: record.TableEpisodes,
		})
		counts.Episodes += int64(len(p.episodes))

		for _, e := range p.episodes {
			if minT == 0 || e.StartedAt < minT {
				minT = e.StartedAt
			}
			if e.StartedAt > maxT {
				maxT = e.StartedAt
			}
		}

		if len(p.steps) > 0 {
			stKey := path.Join(record.TableSteps, p.key, "part-"+batchID+".parquet")
			n, err := s.putParquet(ctx, stKey, p.steps)
			if err != nil {
				s.restore(p)
				return err
			}
			files = append(files, ManifestFile{
				Path: stKey, Rows: int64(len(p.steps)), Bytes: n, Table: record.TableSteps,
			})
			counts.Steps += int64(len(p.steps))
		}
	}

	// The manifest goes last. It is what makes the batch real to a reader,
	// so a crash before this point leaves files that a conforming reader
	// ignores rather than a half-visible batch (F-9.4, §12).
	if err := s.writeManifest(ctx, batchID, now, minT, maxT, files, counts); err != nil {
		return err
	}

	s.mu.Lock()
	for _, f := range files {
		s.stats.FilesWritten++
		s.stats.BytesWritten += f.Bytes
	}
	s.stats.EpisodesWritten += counts.Episodes
	s.stats.StepsWritten += counts.Steps
	s.stats.Batches++
	s.mu.Unlock()

	return nil
}

// restore returns a partition's rows to the pending set after a failed write.
func (s *Sink) restore(p *partition) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.pending[p.key]; ok {
		existing.episodes = append(existing.episodes, p.episodes...)
		existing.steps = append(existing.steps, p.steps...)
		existing.bytes += p.bytes
		return
	}
	s.pending[p.key] = p
}

// Shutdown flushes everything.
func (s *Sink) Shutdown(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		return err
	}
	return s.opts.Store.Close()
}

// Stats returns a snapshot.
func (s *Sink) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// PendingRows reports how many rows are buffered but not yet written, so a
// shutdown path can tell whether it has work to do.
func (s *Sink) PendingRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, p := range s.pending {
		n += len(p.episodes)
	}
	return n
}

// estimateBytes approximates a record's on-disk size for roll decisions. It
// does not need to be exact: it decides when to write, not what to write.
func estimateBytes(ep *pipeline.Assembled) int64 {
	n := int64(512)
	for _, s := range ep.Steps {
		n += 256
		if s.ContentInline != nil {
			n += int64(len(*s.ContentInline))
		}
	}
	return n
}
