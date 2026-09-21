// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import "github.com/parquet-go/parquet-go"

// Table names, which are also the storage prefixes under §8.
const (
	TableEpisodes = "episodes"
	TableSteps    = "steps"
	TableBlobs    = "blobs"

	// Reserved (§7.4): created empty, never written in v1.
	TableOutcomes = "outcomes"
	TableLabels   = "labels"
	TableRewards  = "rewards"
)

// Tables maps each table name to a zero value of its row type. It is the one
// enumeration of the schema surface: the golden schema test, the JSON Schema
// generator and the sink's empty-table creation all range over it, so a new
// table cannot be added in only some of those places.
func Tables() map[string]any {
	return map[string]any{
		TableEpisodes: Episode{},
		TableSteps:    Step{},
		TableBlobs:    Blob{},
		TableOutcomes: Outcome{},
		TableLabels:   Label{},
		TableRewards:  Reward{},
	}
}

// Written names the tables the collector writes. Everything in Tables() but not
// here is reserved surface (§2.3). Spec v0.2 moved outcomes and rewards out of
// reserve; labels stays reserved.
func Written() []string {
	return []string{TableEpisodes, TableSteps, TableBlobs, TableOutcomes, TableRewards}
}

// SchemaOf returns the Parquet schema for a table, panicking on an unknown
// name. The physical layout it produces is pinned by the golden test.
func SchemaOf(table string) *parquet.Schema {
	row, ok := Tables()[table]
	if !ok {
		panic("record: unknown table " + table)
	}
	return parquet.SchemaOf(row)
}
