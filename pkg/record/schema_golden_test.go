// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden schema files")

// The physical Parquet layout is a published contract (F-10.3). This test is
// what makes F-10.1's additive-only rule mechanical rather than a convention:
// any struct edit that changes column order, type, repetition or logical
// annotation fails here, and the diff shows a reviewer exactly what a reader
// built against the previous version would see.
//
// Regenerate deliberately with: go test ./pkg/record -update
func TestParquetSchemaGolden(t *testing.T) {
	for table := range Tables() {
		t.Run(table, func(t *testing.T) {
			got := SchemaOf(table).String()
			path := filepath.Join("testdata", "schema", table+".txt")

			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("missing golden file %s; run: go test ./pkg/record -update", path)
			}
			if got != string(want) {
				t.Errorf("Parquet schema for %q changed.\n\n--- want (committed) ---\n%s\n--- got ---\n%s\n\n"+
					"Within a major version the schema is additive only (F-10.1). If this change "+
					"appends a field, regenerate with -update. If it reorders, retypes or removes "+
					"one, it breaks every reader built against %s.", table, want, got, "the current version")
			}
		})
	}
}
