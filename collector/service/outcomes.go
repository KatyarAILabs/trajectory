// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KatyarAILabs/trajectory/collector/wire"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// bufferedRecord is what the disk buffer holds. An episode is written as its
// fields at the top level — the shape the buffer has always held, so a buffer
// written by an earlier build still drains — and outcomes under their own key.
type bufferedRecord struct {
	Episode  *record.Episode  `json:"Episode,omitempty"`
	Steps    []record.Step    `json:"Steps,omitempty"`
	Outcomes []record.Outcome `json:"Outcomes,omitempty"`
}

// IngestOutcomes accepts business outcomes for the outcome join (§9.4).
//
// Each key goes through the global redaction rules before anything else, so an
// outcome's key becomes the same token an episode's key became. Then outcomes
// take the same durable path as episodes: into the disk buffer, delivered with
// retry. An outcome that is acknowledged and then lost is as bad as a lost
// trajectory, because the episode it labels becomes unlabelled for good.
func (s *Service) IngestOutcomes(ctx context.Context, source string, in []wire.Outcome) (int, error) {
	pol := s.policy()

	if !pol.quotas.allow(source) {
		s.metrics.Shed.WithLabelValues(source, "quota").Inc()
		return 0, errQuota
	}
	if s.buf.UnderBackpressure() {
		s.metrics.Shed.WithLabelValues(source, "backpressure").Inc()
		return 0, errBackpressure
	}

	now := time.Now().UnixMicro()
	out := make([]record.Outcome, 0, len(in))

	for i, o := range in {
		if err := o.Validate(); err != nil {
			return 0, fmt.Errorf("outcomes[%d]: %w", i, err)
		}
		key, err := pol.redactor.TransformKey(o.EntityKey)
		if err != nil {
			// The key could not be transformed the way episode keys
			// were, so it could never match one. Refusing is better
			// than storing a row that silently joins to nothing.
			return 0, fmt.Errorf("outcomes[%d]: entity_key: %w", i, err)
		}

		rec := record.Outcome{
			EntityKey:  key,
			Kind:       o.Kind,
			Value:      o.Value,
			OccurredAt: int64(o.OccurredAt),
			ObservedAt: int64(o.ObservedAt),
			Source:     o.Source,
		}
		// observed_at is when the collector learned of it. A producer may
		// backfill it from its own records; otherwise it is now.
		if rec.ObservedAt == 0 {
			rec.ObservedAt = now
		}
		if rec.Source == "" {
			rec.Source = source
		}
		name := o.EntityName
		rec.EntityName = &name
		if o.OutcomeID != "" {
			id := o.OutcomeID
			rec.OutcomeID = &id
		}
		out = append(out, rec)
	}

	payload, err := json.Marshal(bufferedRecord{Outcomes: out})
	if err != nil {
		return 0, err
	}
	if err := s.buf.Append(payload); err != nil {
		return 0, errBackpressure
	}

	s.metrics.OutcomesIngested.WithLabelValues(source).Add(float64(len(out)))
	return len(out), nil
}

// LoadOutcomes ingests a file's worth of outcomes and makes them durable before
// returning, for `cc outcomes`. It goes through IngestOutcomes, so a file load
// and a POST produce identical rows.
func (s *Service) LoadOutcomes(ctx context.Context, source string, in []wire.Outcome) (int, error) {
	const chunk = 1000
	total := 0
	for i := 0; i < len(in); i += chunk {
		end := i + chunk
		if end > len(in) {
			end = len(in)
		}
		n, err := s.IngestOutcomes(ctx, source, in[i:end])
		if err != nil {
			return total, err
		}
		total += n
	}

	if err := s.buf.Sync(); err != nil {
		return total, err
	}
	if _, err := s.deliverer.DrainOnce(ctx); err != nil {
		return total, fmt.Errorf("deliver: %w", err)
	}
	return total, s.sink.Flush(ctx)
}
