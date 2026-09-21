// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import "github.com/trajectory-project/trajectory/collector/redact"

// rejectingSource is implemented by sources that refuse requests at the door.
type rejectingSource interface {
	Name() string
	Rejected() int64
}

// bytesSource is implemented by sources that count request bytes.
type bytesSource interface {
	Name() string
	BytesReceived() int64
}

// reportPipelineCounters publishes §11 counters whose authoritative totals
// live inside a stage rather than in the service.
//
// Stages keep cumulative totals; Prometheus counters want increments. The
// service remembers the last total it reported and adds only the difference.
// Several of these metrics were registered in an earlier version and never
// incremented, which is worse than not having them: a dashboard showing zero
// redaction errors reads as reassurance.
func (s *Service) reportPipelineCounters() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastBytes == nil {
		s.lastBytes = map[string]int64{}
	}
	for _, src := range s.sources {
		bs, ok := src.(bytesSource)
		if !ok {
			continue
		}
		total := bs.BytesReceived()
		if d := total - s.lastBytes[bs.Name()]; d > 0 {
			s.metrics.IngestBytes.WithLabelValues(bs.Name()).Add(float64(d))
		}
		s.lastBytes[bs.Name()] = total
	}

	// F-1.7 asks for oversized requests to be rejected "with a clear error
	// and a metric". The sources counted them from the start; until this
	// loop existed, nothing exported the count.
	if s.lastRejected == nil {
		s.lastRejected = map[string]int64{}
	}
	for _, src := range s.sources {
		rs, ok := src.(rejectingSource)
		if !ok {
			continue
		}
		total := rs.Rejected()
		if d := total - s.lastRejected[rs.Name()]; d > 0 {
			s.metrics.IngestRecords.WithLabelValues(rs.Name(), "rejected").Add(float64(d))
		}
		s.lastRejected[rs.Name()] = total
	}

	if s.lastRedactErrors == nil {
		s.lastRedactErrors = map[string]int64{}
	}
	for _, r := range s.redactors() {
		for rule, total := range r.Stats().Errors {
			key := r.Name() + "/" + rule
			if d := total - s.lastRedactErrors[key]; d > 0 {
				s.metrics.RedactionErrors.WithLabelValues(rule).Add(float64(d))
			}
			s.lastRedactErrors[key] = total
		}
	}

	st := s.sink.Stats()
	if d := st.FilesWritten - s.lastFiles; d > 0 {
		s.metrics.FilesWritten.WithLabelValues(s.sink.Name(), "all").Add(float64(d))
	}
	s.lastFiles = st.FilesWritten
	if d := st.BlobsDeduped - s.lastBlobsDeduped; d > 0 {
		s.metrics.BlobsDeduped.Add(float64(d))
	}
	s.lastBlobsDeduped = st.BlobsDeduped
}

// redactors lists every redaction policy in use: the global one, plus any
// per-source overrides (F-5.7).
func (s *Service) redactors() []*redact.Redactor {
	pol := s.policy()
	out := []*redact.Redactor{pol.redactor}
	for _, r := range pol.bySource {
		out = append(out, r)
	}
	return out
}
