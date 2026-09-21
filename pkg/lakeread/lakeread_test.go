// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package lakeread

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/trajectory-project/trajectory/pkg/record"
)

func writeParquet[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	os.MkdirAll(filepath.Dir(path), 0o755)
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	w.Write(rows)
	w.Close()
	os.WriteFile(path, buf.Bytes(), 0o644)
}

func writeManifest(t *testing.T, root, name string, files map[string]string) {
	t.Helper()
	var fs []map[string]string
	for p, table := range files {
		fs = append(fs, map[string]string{"path": p, "table": table})
	}
	b, _ := json.Marshal(map[string]any{"files": fs})
	os.MkdirAll(filepath.Join(root, "manifests", "dt=2026-09-21"), 0o755)
	os.WriteFile(filepath.Join(root, "manifests", "dt=2026-09-21", name), b, 0o644)
}

func ep(id, status string, start int64) record.Episode {
	return record.Episode{EpisodeID: id, Tenant: "t", Source: "s", Status: status, StartedAt: start}
}

// A file no manifest lists is a batch a crashed writer never finished. A
// training set built from the lake must not include it.
func TestIgnoresUnmanifestedFiles(t *testing.T) {
	root := t.TempDir()
	writeParquet(t, filepath.Join(root, "episodes/dt=x/part-a.parquet"), []record.Episode{ep("committed", record.StatusComplete, 1)})
	writeParquet(t, filepath.Join(root, "episodes/dt=x/part-b.parquet"), []record.Episode{ep("half-written", record.StatusComplete, 2)})
	writeManifest(t, root, "a.json", map[string]string{"episodes/dt=x/part-a.parquet": record.TableEpisodes})

	l, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Episodes) != 1 || l.Episodes[0].EpisodeID != "committed" {
		t.Fatalf("loaded %d episodes; an unmanifested file leaked in", len(l.Episodes))
	}
	if l.Unmanifested != 1 {
		t.Errorf("Unmanifested = %d, want 1 — the skip must be visible", l.Unmanifested)
	}
}

// A late step written as a patch (F-3.5) is merged into its episode, and an
// at-least-once duplicate of the episode is read once.
func TestMergesPatchesAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	one := int32(1)
	writeParquet(t, filepath.Join(root, "episodes/p1.parquet"), []record.Episode{
		ep("e1", record.StatusComplete, 10),
		ep("e1", record.StatusComplete, 10), // redelivered
		ep("e1", record.StatusPatched, 50),
	})
	writeParquet(t, filepath.Join(root, "steps/p1.parquet"), []record.Step{
		{EpisodeID: "e1", StepIdx: 0, Kind: record.KindLLM, StartedAt: 10, Trainable: "unknown"},
		{EpisodeID: "e1", StepIdx: 1 << 20, ParentIdx: &one, Kind: record.KindTool, StartedAt: 5, Trainable: "unknown"}, // late, earlier in time
		{EpisodeID: "e1", StepIdx: 0, Kind: record.KindLLM, StartedAt: 10, Trainable: "unknown"},                        // redelivered
	})
	writeManifest(t, root, "m.json", map[string]string{
		"episodes/p1.parquet": record.TableEpisodes, "steps/p1.parquet": record.TableSteps,
	})

	l, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Episodes) != 1 {
		t.Fatalf("got %d episodes, want 1", len(l.Episodes))
	}
	e := l.Episodes[0]
	if !e.Patched {
		t.Error("patch not recognised")
	}
	if len(e.Steps) != 2 {
		t.Fatalf("got %d steps, want 2 (patch merged, duplicate dropped)", len(e.Steps))
	}
	if e.Steps[0].Kind != record.KindTool {
		t.Error("the late step was not ordered by when it happened")
	}
}
