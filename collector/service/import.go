// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"fmt"
	"io"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/importers"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// ImportResult summarises what an import produced.
type ImportResult struct {
	Episodes         int64
	Quarantined      int64
	Sampled          int64
	WithoutKeysRatio float64
}

// RunImport feeds an export through the live pipeline and flushes (F-14.3).
//
// It deliberately reuses onEnvelope and onEpisode rather than a parallel path.
// An importer that wrote records by a different route could produce output the
// live path never would, which would make the whole UC-4 evaluation
// misleading — the point of the import path is that it previews the live one.
func (s *Service) RunImport(ctx context.Context, imp importers.Importer, r io.Reader,
	sourceName string, dryRun bool) (importers.Stats, error) {

	s.mu.Lock()
	s.dryRun = dryRun
	s.mu.Unlock()

	st, err := imp.Import(r, sourceName, func(env pipeline.Envelope) error {
		return s.onEnvelope(ctx, env)
	})
	if err != nil {
		return st, err
	}

	// An export is finite and has no terminal markers for episodes the
	// producer never closed, so everything still open is flushed. They are
	// marked timed_out rather than complete: the export ended, which is not
	// the same as the trajectory having finished.
	s.assembler.Flush(record.StatusTimedOut)

	if dryRun {
		return st, nil
	}

	// Episodes are in the buffer, not the sink. Drain it synchronously:
	// `cc import` is a one-shot command, so it must not exit while records
	// are still queued, and a failure here should be reported rather than
	// left for a delivery loop that is about to stop.
	if err := s.buf.Sync(); err != nil {
		return st, fmt.Errorf("sync buffer: %w", err)
	}
	if _, err := s.deliverer.DrainOnce(ctx); err != nil {
		return st, fmt.Errorf("deliver to sink: %w", err)
	}
	if err := s.flushSink(ctx); err != nil {
		return st, fmt.Errorf("flush sink: %w", err)
	}
	return st, nil
}

// ImportResult reports the pipeline-side outcome of the last import.
func (s *Service) ImportResult() ImportResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ImportResult{
		Episodes:         s.emitted,
		Quarantined:      s.quarantined,
		Sampled:          s.sampledOut,
		WithoutKeysRatio: s.extractor.CoverageRatio(),
	}
}
