// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/collector/service"
	"github.com/KatyarAILabs/trajectory/importers"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// UC-4, end to end: a design partner evaluates the collector against their own
// recorded export, with no deployment and no instrumentation change.
//
// The assertions are the same ones DoD-1 makes about the live path, because
// that is the promise of the import path: what a partner sees here is what the
// live path will produce.
func TestImportLangfuseEndToEnd(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")

	cfg := testConfig(lake)
	cfg.Entities = []config.Entity{
		{Tool: "zendesk.update_ticket", Keys: map[string]string{"ticket_id": "$.args.id"}},
	}

	svc, err := service.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join("..", "..", "spec", "testdata", "langfuse-export.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	imp, err := importers.Get("langfuse")
	if err != nil {
		t.Fatal(err)
	}

	st, err := svc.RunImport(context.Background(), imp, f, "import:langfuse", false)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if st.Skipped > 0 {
		t.Errorf("%d records skipped: %v", st.Skipped, st.Warnings)
	}
	if st.Spans != 4 {
		t.Errorf("read %d spans, want 4", st.Spans)
	}

	episodes := readTable[record.Episode](t, lake, record.TableEpisodes)
	steps := readTable[record.Step](t, lake, record.TableSteps)

	// The export holds two sessions, so it must produce two episodes — not
	// one merged blob, and not four singletons.
	if len(episodes) != 2 {
		t.Fatalf("got %d episodes, want 2", len(episodes))
	}
	if len(steps) != 4 {
		t.Fatalf("got %d steps, want 4", len(steps))
	}

	// Imported records stay distinguishable from live traffic (F-1.6).
	for _, e := range episodes {
		if e.Source != "import:langfuse" {
			t.Errorf("source = %q, want import:langfuse", e.Source)
		}
		if e.Instrumentation == nil || *e.Instrumentation != "langfuse-import" {
			t.Errorf("instrumentation = %v, want langfuse-import", e.Instrumentation)
		}
	}

	// The seeded email must not survive anywhere on disk.
	assertNoLeak(t, lake, "alice@example.com")

	// The retry branch must survive the import, not be flattened.
	var multi record.Episode
	for _, e := range episodes {
		if e.StepCount == 3 {
			multi = e
		}
	}
	if multi.EpisodeID == "" {
		t.Fatal("no episode with 3 steps; the session did not assemble")
	}

	var branch []record.Step
	for _, s := range steps {
		if s.EpisodeID == multi.EpisodeID {
			branch = append(branch, s)
		}
	}
	tools := 0
	for _, s := range branch {
		if s.Kind == record.KindTool {
			tools++
			if s.ParentIdx == nil {
				t.Error("a tool step lost its parent link on import")
			}
		}
	}
	if tools != 2 {
		t.Errorf("got %d tool steps, want 2 (the call and its retry)", tools)
	}

	// The error from the failed attempt must reach the episode.
	if multi.Error == nil {
		t.Error("the episode carries no error despite a failed tool call")
	}

	// F-6: the business key is extracted, and tokenized (F-6.4).
	var ticket string
	for _, k := range multi.EntityKeys {
		if k.Name == "ticket_id" {
			ticket = k.Value
		}
	}
	if ticket != "TKT-9001" {
		t.Errorf("ticket_id = %q, want TKT-9001", ticket)
	}

	// F-4.6: Langfuse carries no tool versions, and the fidelity flag must
	// say so rather than quietly implying the data is replayable.
	if multi.Fidelity == nil {
		t.Fatal("fidelity not recorded")
	}
	if multi.Fidelity.HasToolVersions {
		t.Error("has_tool_versions = true, but Langfuse exports carry no tool version")
	}
}

// A dry run must read and normalise without writing anything.
func TestImportDryRunWritesNothing(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	t.Setenv("CC_HMAC_KEY", "acceptance-test-key")

	svc, err := service.New(testConfig(lake), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	f, _ := os.Open(filepath.Join("..", "..", "spec", "testdata", "langfuse-export.json"))
	defer f.Close()
	imp, _ := importers.Get("langfuse")

	if _, err := svc.RunImport(context.Background(), imp, f, "import:langfuse", true); err != nil {
		t.Fatal(err)
	}

	var parquet int
	_ = filepath.Walk(lake, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
			parquet++
		}
		return nil
	})
	if parquet != 0 {
		t.Errorf("dry run wrote %d Parquet files", parquet)
	}
	if r := svc.ImportResult(); r.Episodes == 0 {
		t.Error("dry run reported no episodes; it should still normalise and assemble")
	}
}

// Every registered importer must handle an empty and a malformed input without
// panicking. An importer is pointed at whatever a partner happens to have, so
// it meets bad input as a matter of routine.
func TestImportersHandleBadInput(t *testing.T) {
	for _, imp := range importers.All() {
		t.Run(imp.Name(), func(t *testing.T) {
			for _, in := range []string{"", "   ", "not json at all", "{", "[]", "{}"} {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("panicked on input %q: %v", in, r)
						}
					}()
					_, _ = imp.Import(strings.NewReader(in), "test",
						func(pipeline.Envelope) error { return nil })
				}()
			}
		})
	}
}
