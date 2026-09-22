// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package importers backfills historical exports into the same tables as live
// traffic (F-14.3, UC-4).
//
// This is the first thing a pilot uses: a design partner can evaluate the
// collector against their own recorded data before any deployment happens, with
// no cloud account and no instrumentation change.
//
// An importer is a file reader in front of the same normalise → assemble →
// redact → extract → sink chain the live path uses. That is deliberate: a
// second pipeline for imported data would drift, and the whole value of the
// import path is that it proves what the live path will produce.
package importers

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
)

// An Importer reads an export format and emits envelopes.
type Importer interface {
	// Name is the format name used on the command line.
	Name() string
	// Describe is one line of help text.
	Describe() string
	// Import reads r and calls emit for every envelope. The source name is
	// attributed onto every record (F-1.6), tagged so imported data stays
	// distinguishable from live traffic.
	Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error)
}

// Stats summarise one import run.
type Stats struct {
	Records int
	Spans   int
	Skipped int
	// Warnings are per-record problems that did not stop the import. They
	// are capped, because a malformed file should not produce a million
	// lines of output.
	Warnings []string
}

const maxWarnings = 20

// Warn records a non-fatal problem.
//
// An import that silently skipped records would be the worst possible
// behaviour here: a partner would conclude the collector loses data, or worse,
// not notice.
func (s *Stats) Warn(format string, args ...any) {
	s.Skipped++
	if len(s.Warnings) < maxWarnings {
		s.Warnings = append(s.Warnings, fmt.Sprintf(format, args...))
	} else if len(s.Warnings) == maxWarnings {
		s.Warnings = append(s.Warnings, "... further warnings suppressed")
	}
}

var registry = map[string]Importer{}

func register(i Importer) { registry[i.Name()] = i }

// Get returns an importer by name.
func Get(name string) (Importer, error) {
	i, ok := registry[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("unknown import format %q; available: %s",
			name, strings.Join(Names(), ", "))
	}
	return i, nil
}

// Names lists registered formats.
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// All returns every importer, for help output.
func All() []Importer {
	out := make([]Importer, 0, len(registry))
	for _, n := range Names() {
		out = append(out, registry[n])
	}
	return out
}
