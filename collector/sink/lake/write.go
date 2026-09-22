// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package lake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/internal/version"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Manifest is written last and lists everything in the batch (F-9.6).
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
	Outcomes     int64 `json:"outcomes,omitempty"`
	Rewards      int64 `json:"rewards,omitempty"`
	BlobsNew     int64 `json:"blobs_new"`
	BlobsDeduped int64 `json:"blobs_deduped"`
}

// externalise moves payloads above the threshold into content-addressed blobs
// (F-9.3) and applies the hard truncation limit (F-9.7).
func (s *Sink) externalise(ctx context.Context, eps []*pipeline.Assembled) ([]record.Blob, error) {
	var out []record.Blob
	now := s.opts.Now()

	for _, ep := range eps {
		for i := range ep.Steps {
			step := &ep.Steps[i]
			if step.ContentInline == nil {
				continue
			}
			content := *step.ContentInline

			// A payload beyond the hard maximum is truncated with a
			// marker rather than failing the record: losing one
			// oversized prompt is better than losing the whole
			// trajectory it belongs to (F-9.7).
			if s.opts.MaxPayloadBytes > 0 && len(content) > s.opts.MaxPayloadBytes {
				content = content[:s.opts.MaxPayloadBytes]
				step.Truncated = true
				s.mu.Lock()
				s.stats.Truncated++
				s.mu.Unlock()
			}

			if len(content) <= s.opts.BlobThresholdBytes {
				step.ContentInline = &content
				continue
			}

			sum := sha256.Sum256([]byte(content))
			hash := hex.EncodeToString(sum[:])

			isNew, err := s.putBlob(ctx, hash, []byte(content))
			if err != nil {
				return nil, err
			}
			if isNew {
				out = append(out, record.Blob{
					Sha256:      hash,
					Bytes:       int64(len(content)),
					ContentType: "application/json",
					Encoding:    record.EncodingUTF8,
					FirstSeenAt: now.UnixMicro(),
				})
			}

			// Exactly one of the two carries the payload.
			step.ContentRef = &hash
			step.ContentInline = nil
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Sha256 < out[j].Sha256 })
	return out, nil
}

// BlobKey is the object key for a content-addressed blob, with the two-level
// fan-out of §8. Exported because cc replay and cc inspect resolve refs.
func BlobKey(hash string) string {
	if len(hash) < 4 {
		return path.Join(record.TableBlobs, "sha256", hash)
	}
	return path.Join(record.TableBlobs, "sha256", hash[0:2], hash[2:4], hash)
}

// putBlob writes a blob if it is not already present.
//
// The store's own Exists is authoritative rather than the in-memory cache, so a
// restarted collector does not rewrite every blob it has seen before, and two
// collectors sharing a prefix converge instead of conflicting.
func (s *Sink) putBlob(ctx context.Context, hash string, content []byte) (bool, error) {
	s.mu.Lock()
	cached := s.blobsSeen[hash]
	s.mu.Unlock()

	if cached {
		s.mu.Lock()
		s.stats.BlobsDeduped++
		s.mu.Unlock()
		return false, nil
	}

	key := BlobKey(hash)
	exists, err := s.opts.Store.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	if exists {
		s.mu.Lock()
		s.blobsSeen[hash] = true
		s.stats.BlobsDeduped++
		s.mu.Unlock()
		return false, nil
	}

	if err := s.opts.Store.Put(ctx, key, content); err != nil {
		return false, err
	}

	s.mu.Lock()
	s.blobsSeen[hash] = true
	s.stats.BlobsNew++
	s.mu.Unlock()
	return true, nil
}

func (s *Sink) writeBlobTable(ctx context.Context, blobs []record.Blob) error {
	key := path.Join(record.TableBlobs, "part-"+s.opts.NewID()+".parquet")
	n, err := s.putParquet(ctx, key, blobs)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.stats.FilesWritten++
	s.stats.BytesWritten += n
	s.mu.Unlock()
	return nil
}

// putParquet marshals rows and writes the object.
func (s *Sink) putParquet(ctx context.Context, key string, rows any) (int64, error) {
	var body []byte
	var err error

	switch r := rows.(type) {
	case []record.Episode:
		body, err = marshalParquet(r, s.compression())
	case []record.Step:
		body, err = marshalParquet(r, s.compression())
	case []record.Blob:
		body, err = marshalParquet(r, s.compression())
	case []record.Outcome:
		body, err = marshalParquet(r, s.compression())
	case []record.Reward:
		body, err = marshalParquet(r, s.compression())
	default:
		return 0, fmt.Errorf("lake: unsupported row type %T", rows)
	}
	if err != nil {
		return 0, err
	}
	if len(body) == 0 {
		return 0, nil
	}

	if err := s.opts.Store.Put(ctx, key, body); err != nil {
		return 0, err
	}
	return int64(len(body)), nil
}

func (s *Sink) compression() parquet.WriterOption {
	switch strings.ToLower(s.opts.Compression) {
	case "none", "uncompressed":
		return parquet.Compression(&parquet.Uncompressed)
	case "snappy":
		return parquet.Compression(&parquet.Snappy)
	default:
		// zstd by default: payload volume is the cost that makes
		// customers disable collection (§19), and zstd is the best
		// ratio among the codecs every reader supports.
		return parquet.Compression(&parquet.Zstd)
	}
}

func marshalParquet[T any](rows []T, compression parquet.WriterOption) ([]byte, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf,
		// F-10.2: the version is in the file metadata, so a reader can
		// check compatibility without decoding any data.
		parquet.KeyValueMetadata("schema_version", version.Schema),
		compression,
	)
	if _, err := w.Write(rows); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// partitionKey builds the configured partition directories (F-9.2).
func (s *Sink) partitionKey(ep record.Episode) string {
	var parts []string
	for _, key := range s.opts.PartitionBy {
		switch key {
		case "dt":
			parts = append(parts, "dt="+time.UnixMicro(ep.StartedAt).UTC().Format("2006-01-02"))
		case "tenant":
			parts = append(parts, "tenant="+sanitise(ep.Tenant))
		case "task_type":
			v := "unknown"
			if ep.TaskType != nil && *ep.TaskType != "" {
				v = *ep.TaskType
			}
			parts = append(parts, "task_type="+sanitise(v))
		}
	}
	return path.Join(parts...)
}

// sanitise keeps producer-supplied values from escaping the prefix.
//
// task_type is free-form and producer-supplied (§7.1), so a value containing a
// slash or ".." would otherwise write outside the configured prefix. The store
// refuses traversal as a backstop; this keeps keys well-formed in the first
// place, which also matters for S3 where there is no filesystem to refuse.
func sanitise(v string) string {
	if v == "" {
		return "unknown"
	}
	repl := strings.NewReplacer("/", "_", "\\", "_", "..", "_", "=", "_", "\x00", "_")
	out := repl.Replace(v)
	if len(out) > 128 {
		out = out[:128]
	}
	if out == "" || out == "." {
		return "unknown"
	}
	return out
}

func (s *Sink) writeManifest(ctx context.Context, batchID string, now time.Time,
	minT, maxT int64, files []ManifestFile, counts ManifestCounts) error {

	s.mu.Lock()
	counts.BlobsNew = s.stats.BlobsNew
	counts.BlobsDeduped = s.stats.BlobsDeduped
	s.mu.Unlock()

	host, _ := os.Hostname()
	m := Manifest{
		BatchID:       batchID,
		SchemaVersion: version.Schema,
		WrittenAt:     now.UTC().Format(time.RFC3339),
		Collector:     ManifestBuild{Version: version.Collector, Instance: host},
		TimeRange: ManifestRange{
			Min: time.UnixMicro(minT).UTC().Format(time.RFC3339),
			Max: time.UnixMicro(maxT).UTC().Format(time.RFC3339),
		},
		Files:  files,
		Counts: counts,
	}

	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	key := path.Join("manifests", "dt="+now.UTC().Format("2006-01-02"), batchID+".json")
	return s.opts.Store.Put(ctx, key, body)
}

// writeSchemaDescriptors publishes the schema alongside the data (§8 _schema/),
// so a reader has the contract without fetching the repository.
func (s *Sink) writeSchemaDescriptors(ctx context.Context) error {
	for _, table := range record.Written() {
		key := path.Join("_schema", "v"+version.Schema, table+".txt")
		if err := s.opts.Store.Put(ctx, key, []byte(record.SchemaOf(table).String())); err != nil {
			return fmt.Errorf("lake: write schema descriptor %s: %w", key, err)
		}
	}
	return nil
}
