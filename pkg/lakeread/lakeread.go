// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lakeread reads a trajectory lake from a directory, the way a
// conforming reader must (F-9.4): only files a manifest lists.
//
// That rule matters more here than anywhere. The outcome join and the training
// export turn a lake into datasets people train on; an unmanifested file is a
// batch a crashed writer never finished, and it must not leak into a training
// set because a glob happened to match it.
//
// Late-arriving steps written as patch records (F-3.5) are merged back into
// their episode, so a reader sees one trajectory per episode_id.
package lakeread

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Episode is one trajectory with its steps in canonical order.
type Episode struct {
	record.Episode
	Steps []record.Step
	// Patched reports whether late steps were merged in from patch records.
	Patched bool
}

// Lake is a loaded lake.
type Lake struct {
	Root     string
	Episodes []*Episode
	Outcomes []record.Outcome
	Rewards  []record.Reward
	// Unmanifested counts Parquet files present but not listed in any
	// manifest, which were skipped. Non-zero usually means a writer crashed
	// mid-batch; the data is not lost, it was never committed.
	Unmanifested int

	byID map[string]*Episode
}

type manifest struct {
	Files []struct {
		Path  string `json:"path"`
		Table string `json:"table"`
	} `json:"files"`
}

// Load reads every manifested file under root.
func Load(root string) (*Lake, error) {
	listed, err := manifestedFiles(root)
	if err != nil {
		return nil, err
	}

	l := &Lake{Root: root, byID: map[string]*Episode{}}

	var patches []record.Episode
	stepsByEp := map[string][]record.Step{}

	for _, f := range listed {
		full := filepath.Join(root, f.path)
		switch f.table {
		case record.TableEpisodes:
			rows, err := parquet.ReadFile[record.Episode](full)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f.path, err)
			}
			for _, e := range rows {
				if e.Status == record.StatusPatched {
					patches = append(patches, e)
					continue
				}
				if _, dup := l.byID[e.EpisodeID]; dup {
					// At-least-once delivery (F-8.4) can land an
					// episode twice. Readers deduplicate on
					// episode_id; the first copy wins.
					continue
				}
				ep := &Episode{Episode: e}
				l.byID[e.EpisodeID] = ep
				l.Episodes = append(l.Episodes, ep)
			}
		case record.TableSteps:
			rows, err := parquet.ReadFile[record.Step](full)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f.path, err)
			}
			for _, s := range rows {
				stepsByEp[s.EpisodeID] = append(stepsByEp[s.EpisodeID], s)
			}
		case record.TableOutcomes:
			rows, err := parquet.ReadFile[record.Outcome](full)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f.path, err)
			}
			l.Outcomes = append(l.Outcomes, rows...)
		case record.TableRewards:
			rows, err := parquet.ReadFile[record.Reward](full)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f.path, err)
			}
			l.Rewards = append(l.Rewards, rows...)
		}
	}

	for id, steps := range stepsByEp {
		ep, ok := l.byID[id]
		if !ok {
			continue
		}
		ep.Steps = dedupeSteps(steps)
	}
	for _, p := range patches {
		if ep, ok := l.byID[p.EpisodeID]; ok {
			ep.Patched = true
		}
	}

	sort.Slice(l.Episodes, func(i, j int) bool {
		if l.Episodes[i].StartedAt != l.Episodes[j].StartedAt {
			return l.Episodes[i].StartedAt < l.Episodes[j].StartedAt
		}
		return l.Episodes[i].EpisodeID < l.Episodes[j].EpisodeID
	})

	l.Unmanifested = countUnmanifested(root, listed)
	return l, nil
}

// Episode returns one episode by id.
func (l *Lake) Episode(id string) (*Episode, bool) {
	ep, ok := l.byID[id]
	return ep, ok
}

// Payload resolves a step's content, inline or from its blob.
func (l *Lake) Payload(s record.Step) (string, error) {
	if s.ContentInline != nil {
		return *s.ContentInline, nil
	}
	if s.ContentRef == nil {
		return "", nil
	}
	h := *s.ContentRef
	if len(h) < 4 {
		return "", fmt.Errorf("malformed content_ref %q", h)
	}
	b, err := os.ReadFile(filepath.Join(l.Root, record.TableBlobs, "sha256", h[0:2], h[2:4], h))
	if err != nil {
		return "", fmt.Errorf("blob %s: %w", h[:12], err)
	}
	return string(b), nil
}

// dedupeSteps orders steps by time, then index, and drops exact redeliveries.
// Patch steps carry indices above the original range (F-3.5), so ordering by
// time is what interleaves a late step where it happened.
func dedupeSteps(steps []record.Step) []record.Step {
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].StartedAt != steps[j].StartedAt {
			return steps[i].StartedAt < steps[j].StartedAt
		}
		return steps[i].StepIdx < steps[j].StepIdx
	})
	seen := map[int32]bool{}
	out := steps[:0]
	for _, s := range steps {
		if seen[s.StepIdx] {
			continue
		}
		seen[s.StepIdx] = true
		out = append(out, s)
	}
	return out
}

type listedFile struct{ path, table string }

func manifestedFiles(root string) ([]listedFile, error) {
	var out []listedFile
	seen := map[string]bool{}

	err := filepath.Walk(filepath.Join(root, "manifests"), func(p string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() || !strings.HasSuffix(p, ".json") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var m manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return fmt.Errorf("manifest %s: %w", p, err)
		}
		for _, f := range m.Files {
			if !seen[f.Path] {
				seen[f.Path] = true
				out = append(out, listedFile{f.Path, f.Table})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Blob tables are written before a manifest exists for them and are
	// not needed here: blobs are resolved by hash.
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

func countUnmanifested(root string, listed []listedFile) int {
	known := map[string]bool{}
	for _, f := range listed {
		known[filepath.ToSlash(f.path)] = true
	}
	n := 0
	for _, table := range []string{record.TableEpisodes, record.TableSteps, record.TableOutcomes, record.TableRewards} {
		_ = filepath.Walk(filepath.Join(root, table), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".parquet") {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			if !known[filepath.ToSlash(rel)] {
				n++
			}
			return nil
		})
	}
	return n
}
