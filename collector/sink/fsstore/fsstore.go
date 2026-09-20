// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fsstore writes episodes to a local filesystem in the layout of §8.
//
// Local FS is the Phase 1 sink, but the layout, the manifest and the atomic
// visibility rule are the real ones: an S3 sink is a different writer behind
// the same structure, not a different structure.
//
// Two invariants carry the design:
//
//   - A reader never sees a partially written batch (F-9.4). Data files are
//     written to temporary names and renamed into place, and the manifest —
//     which is what makes a batch real to a reader — is written last.
//   - Payloads above a threshold become content-addressed blobs, deduplicated
//     by sha256 (F-9.3). A system prompt repeated across a million episodes is
//     stored once.
package fsstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/internal/version"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Sink writes Parquet, blobs and manifests under a root directory.
type Sink struct {
	name string
	root string
	cfg  config.Sink

	now   func() time.Time
	newID func() string

	mu      sync.Mutex
	pending []*pipeline.Assembled
	// blobsSeen is this process's view of which blobs already exist, so a
	// repeated payload is hashed and written once. It is a cache, not the
	// source of truth: the on-disk check is authoritative, which keeps the
	// sink correct across restarts.
	blobsSeen map[string]bool
	stats     Stats
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
}

// Manifest is written last and lists everything in the batch (F-9.6). A reader
// that ignores unmanifested files therefore never sees a partial batch.
type Manifest struct {
	BatchID       string         `json:"batch_id"`
	SchemaVersion string         `json:"schema_version"`
	WrittenAt     string         `json:"written_at"`
	Collector     ManifestBuild  `json:"collector"`
	TimeRange     ManifestRange  `json:"time_range"`
	Files         []ManifestFile `json:"files"`
	Counts        ManifestCounts `json:"counts"`
}

type ManifestBuild struct {
	Version  string `json:"version"`
	Instance string `json:"instance"`
}

type ManifestRange struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

type ManifestFile struct {
	Path  string `json:"path"`
	Rows  int64  `json:"rows"`
	Bytes int64  `json:"bytes"`
	Table string `json:"table"`
}

type ManifestCounts struct {
	Episodes     int64 `json:"episodes"`
	Steps        int64 `json:"steps"`
	BlobsNew     int64 `json:"blobs_new"`
	BlobsDeduped int64 `json:"blobs_deduped"`
}

// New creates a filesystem sink rooted at cfg.Dir.
func New(cfg config.Sink) (*Sink, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("fsstore: dir is required")
	}
	s := &Sink{
		name:      cfg.Name,
		root:      cfg.Dir,
		cfg:       cfg,
		now:       time.Now,
		newID:     func() string { return ulid.Make().String() },
		blobsSeen: map[string]bool{},
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return nil, fmt.Errorf("fsstore: create %s: %w", s.root, err)
	}
	if err := s.writeSchemaDescriptors(); err != nil {
		return nil, err
	}
	return s, nil
}

// Name implements pipeline.Sink.
func (s *Sink) Name() string { return s.name }

// Write buffers episodes for the next flush.
func (s *Sink) Write(_ context.Context, eps []*pipeline.Assembled) error {
	s.mu.Lock()
	s.pending = append(s.pending, eps...)
	s.mu.Unlock()
	return nil
}

// Shutdown flushes and stops.
func (s *Sink) Shutdown(ctx context.Context) error { return s.Flush(ctx) }

// Stats returns a snapshot of the stage counters.
func (s *Sink) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// writeSchemaDescriptors publishes the schema alongside the data (§8
// _schema/), so a reader has the contract without fetching the repository.
func (s *Sink) writeSchemaDescriptors() error {
	dir := filepath.Join(s.root, "_schema", "v"+version.Schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, table := range record.Written() {
		path := filepath.Join(dir, table+".txt")
		if err := writeFileAtomic(path, []byte(record.SchemaOf(table).String())); err != nil {
			return fmt.Errorf("fsstore: write schema descriptor %s: %w", path, err)
		}
	}
	return nil
}

// writeFileAtomic writes via a temporary file and renames, so a reader never
// observes a half-written file (F-9.4). Rename within a directory is atomic on
// POSIX filesystems.
func writeFileAtomic(path string, b []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	// fsync before rename: a rename that reaches disk before the contents
	// would expose an empty file after a crash.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// writeParquet marshals rows to Parquet and lands the file atomically.
func writeParquet[T any](path string, rows []T) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	var buf bytesBuffer
	w := parquet.NewGenericWriter[T](&buf,
		// F-10.2: the version is in the file metadata, so a reader can
		// check compatibility without decoding any data.
		parquet.KeyValueMetadata("schema_version", version.Schema),
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := w.Write(rows); err != nil {
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}

	if err := writeFileAtomic(path, buf.Bytes()); err != nil {
		return 0, err
	}
	return int64(buf.Len()), nil
}
