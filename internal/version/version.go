// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package version holds the schema and build identity stamped onto every
// record and every file the collector writes.
package version

// Schema is the semver of the record schema (F-10.2). It is stamped on every
// record and in Parquet file key-value metadata.
//
// Within a major version, changes are additive only (F-10.1): a reader written
// against X.Y must read X.Z for Z > Y without modification. The golden schema
// test in pkg/record is what enforces that mechanically.
//
// This constant is the source of truth. spec/VERSION mirrors it for tooling
// that cannot import Go, and a test asserts the two agree.
const Schema = "0.1.0"

// Collector is the build version of the binary, overridden at link time.
var Collector = "dev"

// Commit is the build commit, overridden at link time.
var Commit = "unknown"
