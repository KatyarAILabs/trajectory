// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package fsstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/internal/version"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Flush writes every pending episode as one batch.
//
// Order is the contract (F-9.4). Blobs first, because a step may reference one
// and a reader following a content_ref must never find nothing. Then the
// Parquet data files. The manifest last, because it is what makes the batch
// visible: a reader that ignores unmanifested files cannot observe a partial
// write, even if the process dies midway.
func (s *Sink) Flush(_ context.Context) error {
	s.mu.Lock()
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	batchID := s.newID()
	now := s.now().UTC()

	// 1. Externalise payloads. This mutates the steps, replacing oversized
	//    content_inline with a content_ref, so it must happen before the
	//    steps are marshalled.
	blobs, err := s.externalise(pending, now)
	if err != nil {
		return fmt.Errorf("fsstore: externalise payloads: %w", err)
	}

	// 2. Data files, grouped by partition.
	files, counts, err := s.writeTables(pending, batchID)
	if err != nil {
		return fmt.Errorf("fsstore: write tables: %w", err)
	}

	if len(blobs) > 0 {
		path := filepath.Join(s.root, record.TableBlobs,
			fmt.Sprintf("part-%s.parquet", batchID))
		n, err := writeParquet(path, blobs)
		if err != nil {
			return fmt.Errorf("fsstore: write blobs table: %w", err)
		}
		files = append(files, ManifestFile{
			Path: s.rel(path), Rows: int64(len(blobs)), Bytes: n, Table: record.TableBlobs,
		})
	}

	// 3. Manifest last.
	if err := s.writeManifest(batchID, now, pending, files, counts); err != nil {
		return fmt.Errorf("fsstore: write manifest: %w", err)
	}

	s.mu.Lock()
	for _, f := range files {
		s.stats.FilesWritten++
		s.stats.BytesWritten += f.Bytes
	}
	s.stats.EpisodesWritten += counts.Episodes
	s.stats.StepsWritten += counts.Steps
	s.mu.Unlock()

	return nil
}

// externalise moves payloads above the threshold into content-addressed blobs
// (F-9.3) and applies the hard truncation limit (F-9.7).
func (s *Sink) externalise(eps []*pipeline.Assembled, now time.Time) ([]record.Blob, error) {
	var out []record.Blob

	for _, ep := range eps {
		for i := range ep.Steps {
			step := &ep.Steps[i]
			if step.ContentInline == nil {
				continue
			}
			content := *step.ContentInline

			// A payload beyond the hard maximum is truncated with an
			// explicit marker rather than failing the record: losing
			// one oversized prompt is better than losing the whole
			// trajectory it belongs to (F-9.7).
			if s.cfg.MaxPayloadBytes > 0 && len(content) > s.cfg.MaxPayloadBytes {
				content = content[:s.cfg.MaxPayloadBytes]
				step.Truncated = true
				s.mu.Lock()
				s.stats.Truncated++
				s.mu.Unlock()
			}

			if len(content) <= s.cfg.BlobThresholdBytes {
				step.ContentInline = &content
				continue
			}

			sum := sha256.Sum256([]byte(content))
			hash := hex.EncodeToString(sum[:])

			isNew, err := s.putBlob(hash, []byte(content))
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
				s.mu.Lock()
				s.stats.BlobsNew++
				s.mu.Unlock()
			} else {
				s.mu.Lock()
				s.stats.BlobsDeduped++
				s.mu.Unlock()
			}

			// Exactly one of the two carries the payload.
			step.ContentRef = &hash
			step.ContentInline = nil
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Sha256 < out[j].Sha256 })
	return out, nil
}

// putBlob writes a blob if it is not already present, using the two-level
// hash fan-out of §8.
//
// The on-disk check is authoritative rather than the in-memory cache, so a
// restarted collector does not rewrite every blob it has seen before, and two
// collectors sharing a prefix converge instead of conflicting.
func (s *Sink) putBlob(hash string, content []byte) (bool, error) {
	path := filepath.Join(s.root, record.TableBlobs, "sha256", hash[0:2], hash[2:4], hash)

	s.mu.Lock()
	cached := s.blobsSeen[hash]
	s.mu.Unlock()
	if cached {
		return false, nil
	}

	if _, err := os.Stat(path); err == nil {
		s.mu.Lock()
		s.blobsSeen[hash] = true
		s.mu.Unlock()
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	if err := writeFileAtomic(path, content); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.blobsSeen[hash] = true
	s.mu.Unlock()
	return true, nil
}

// writeTables writes the episodes and steps tables, one file per partition.
func (s *Sink) writeTables(eps []*pipeline.Assembled, batchID string) ([]ManifestFile, ManifestCounts, error) {
	var files []ManifestFile
	var counts ManifestCounts

	byPartition := map[string][]*pipeline.Assembled{}
	for _, ep := range eps {
		p := s.partitionPath(ep.Episode)
		byPartition[p] = append(byPartition[p], ep)
	}

	// Sorted so a batch writes its partitions in a stable order, which
	// makes the manifest reproducible for a given input.
	parts := make([]string, 0, len(byPartition))
	for p := range byPartition {
		parts = append(parts, p)
	}
	sort.Strings(parts)

	for _, part := range parts {
		group := byPartition[part]

		episodes := make([]record.Episode, 0, len(group))
		var steps []record.Step
		for _, ep := range group {
			episodes = append(episodes, ep.Episode)
			steps = append(steps, ep.Steps...)
		}

		epPath := filepath.Join(s.root, record.TableEpisodes, part,
			fmt.Sprintf("part-%s.parquet", batchID))
		n, err := writeParquet(epPath, episodes)
		if err != nil {
			return nil, counts, err
		}
		files = append(files, ManifestFile{
			Path: s.rel(epPath), Rows: int64(len(episodes)), Bytes: n, Table: record.TableEpisodes,
		})
		counts.Episodes += int64(len(episodes))

		if len(steps) > 0 {
			stPath := filepath.Join(s.root, record.TableSteps, part,
				fmt.Sprintf("part-%s.parquet", batchID))
			n, err := writeParquet(stPath, steps)
			if err != nil {
				return nil, counts, err
			}
			files = append(files, ManifestFile{
				Path: s.rel(stPath), Rows: int64(len(steps)), Bytes: n, Table: record.TableSteps,
			})
			counts.Steps += int64(len(steps))
		}
	}

	return files, counts, nil
}

// partitionPath builds the configured partition directories (F-9.2).
func (s *Sink) partitionPath(ep record.Episode) string {
	var parts []string
	for _, key := range s.cfg.PartitionBy {
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
	return filepath.Join(parts...)
}

// sanitise keeps producer-supplied values from escaping the partition
// directory. task_type is free-form and producer-supplied (§7.1), so a value
// containing a slash or ".." would otherwise write outside the prefix.
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

func (s *Sink) writeManifest(batchID string, now time.Time, eps []*pipeline.Assembled,
	files []ManifestFile, counts ManifestCounts) error {

	minT, maxT := int64(0), int64(0)
	for _, ep := range eps {
		t := ep.Episode.StartedAt
		if minT == 0 || t < minT {
			minT = t
		}
		if t > maxT {
			maxT = t
		}
	}

	s.mu.Lock()
	counts.BlobsNew = s.stats.BlobsNew
	counts.BlobsDeduped = s.stats.BlobsDeduped
	s.mu.Unlock()

	host, _ := os.Hostname()
	m := Manifest{
		BatchID:       batchID,
		SchemaVersion: version.Schema,
		WrittenAt:     now.Format(time.RFC3339),
		Collector:     ManifestBuild{Version: version.Collector, Instance: host},
		TimeRange: ManifestRange{
			Min: time.UnixMicro(minT).UTC().Format(time.RFC3339),
			Max: time.UnixMicro(maxT).UTC().Format(time.RFC3339),
		},
		Files:  files,
		Counts: counts,
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(s.root, "manifests",
		"dt="+now.Format("2006-01-02"), batchID+".json")
	return writeFileAtomic(path, b)
}

func (s *Sink) rel(path string) string {
	r, err := filepath.Rel(s.root, path)
	if err != nil {
		return path
	}
	return r
}
