// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package service wires sources, the assembler, processors and sinks into a
// running collector.
//
// Processor order is fixed here rather than taken from config, because the
// order carries correctness guarantees that an operator should not be able to
// invert by editing a list:
//
//	assemble -> redact -> extract -> sink
//
// Redaction precedes the sink so nothing unredacted is ever written (F-5.3),
// and extraction follows redaction so entity keys are derived from tokenized
// values and stay joinable without being readable (F-6.4).
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/trajectory-project/trajectory/collector/assemble"
	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/extract"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/redact"
	"github.com/trajectory-project/trajectory/collector/sink/fsstore"
	"github.com/trajectory-project/trajectory/collector/source/otlp"
	"github.com/trajectory-project/trajectory/collector/telemetry"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Service is a running collector.
type Service struct {
	cfg     *config.Config
	log     *slog.Logger
	metrics *telemetry.Metrics

	sources   []pipeline.Source
	assembler *assemble.Assembler
	redactor  *redact.Redactor
	extractor *extract.Extractor
	sink      *fsstore.Sink

	// quarantineDir holds records that failed a processor. They are written
	// as JSON so an operator can inspect why, without a query engine.
	quarantineDir string

	mu          sync.Mutex
	emitted     int64
	quarantined int64
	// Last-seen cumulative sink totals, so counters advance by delta
	// rather than being re-set to a running total.
	lastFiles        int64
	lastBlobsDeduped int64
}

// New builds a service from validated config.
func New(cfg *config.Config, log *slog.Logger) (*Service, error) {
	s := &Service{cfg: cfg, log: log, metrics: telemetry.New()}

	sink, err := fsstore.New(cfg.Sinks[0])
	if err != nil {
		return nil, err
	}
	s.sink = sink
	s.quarantineDir = filepath.Join(cfg.Sinks[0].Dir, "quarantine")

	// The HMAC key comes from the environment, never from the config file
	// (F-11.1). Config validation has already checked it is present.
	var key []byte
	if env := cfg.Redaction.Tokenization.KeyEnv; env != "" {
		key = []byte(os.Getenv(env))
	}

	s.redactor, err = redact.New(cfg.Redaction, key, s.onManifest)
	if err != nil {
		return nil, err
	}
	s.extractor, err = extract.New(cfg.Entities)
	if err != nil {
		return nil, err
	}

	s.assembler = assemble.New(assemble.Options{
		Tenant:              cfg.Tenant,
		Window:              cfg.Assembly.Window,
		MaxInFlight:         cfg.Assembly.MaxInFlight,
		SettleAfterTerminal: cfg.Assembly.SettleAfterTerminal,
		NewID:               func() string { return ulid.Make().String() },
	}, s.onEpisode)

	for _, sc := range cfg.Sources {
		s.sources = append(s.sources, otlp.New(otlp.Options{
			Name:            sc.Name,
			Listen:          sc.HTTP.Listen,
			MaxRequestBytes: sc.MaxRequestBytes,
			SessionKeyOrder: cfg.Assembly.SessionKey,
		}))
	}

	if key != nil {
		// A key_id in the log makes a rotation visible in operational
		// history. The key itself is never logged (F-12.2).
		log.Info("tokenization key loaded", "key_id", s.redactor.KeyID())
	}

	return s, nil
}

// Metrics exposes the metric set, for the CLI to serve.
func (s *Service) Metrics() *telemetry.Metrics { return s.metrics }

// Run starts every source and blocks until ctx is cancelled, then shuts down
// gracefully: stop accepting, drain assembly, flush the sink (F-11.5).
func (s *Service) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.sources))

	for _, src := range s.sources {
		wg.Add(1)
		go func(src pipeline.Source) {
			defer wg.Done()
			if err := src.Start(ctx, s.onEnvelope); err != nil {
				errCh <- fmt.Errorf("source %s: %w", src.Name(), err)
			}
		}(src)
	}

	// Expiry ticker: episodes that never receive a terminal marker are
	// emitted at window expiry rather than held forever (F-3.3).
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Periodic flush so data becomes durable without waiting for shutdown.
	flush := time.NewTicker(5 * time.Second)
	defer flush.Stop()

	s.metrics.SetReady(true)
	s.log.Info("collector ready",
		"tenant", s.cfg.Tenant,
		"sources", len(s.sources),
		"sink", s.cfg.Sinks[0].Name)

	for {
		select {
		case <-ctx.Done():
			s.metrics.SetReady(false)
			wg.Wait()
			return s.drain()

		case err := <-errCh:
			s.metrics.SetReady(false)
			return err

		case <-ticker.C:
			s.assembler.Expire()
			s.metrics.EpisodesInFlight.Set(float64(s.assembler.InFlight()))

		case <-flush.C:
			if err := s.flushSink(context.WithoutCancel(ctx)); err != nil {
				s.log.Error("sink flush failed", "error", err)
			}
		}
	}
}

// drain is the graceful-shutdown path: emit everything still in assembly, run
// it through the processors, and flush.
//
// In-flight episodes are marked timed_out rather than complete, because that
// is what they are: the process stopped before their terminal marker arrived.
// Labelling them complete would quietly corrupt a consumer's view of which
// trajectories actually finished.
func (s *Service) drain() error {
	s.assembler.Flush(record.StatusTimedOut)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.flushSink(ctx); err != nil {
		return err
	}
	s.log.Info("shutdown complete",
		"episodes_emitted", s.emitted, "quarantined", s.quarantined)
	return nil
}

func (s *Service) flushSink(ctx context.Context) error {
	start := time.Now()
	err := s.sink.Flush(ctx)
	s.metrics.SinkWriteDuration.WithLabelValues(s.sink.Name()).Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.SinkErrors.WithLabelValues(s.sink.Name(), "flush").Inc()
		return err
	}

	// The sink reports cumulative totals; the metric is a counter, so only
	// the delta since the last flush is added.
	st := s.sink.Stats()
	s.mu.Lock()
	newFiles := st.FilesWritten - s.lastFiles
	newBlobs := st.BlobsDeduped - s.lastBlobsDeduped
	s.lastFiles = st.FilesWritten
	s.lastBlobsDeduped = st.BlobsDeduped
	s.mu.Unlock()

	if newFiles > 0 {
		s.metrics.FilesWritten.WithLabelValues(s.sink.Name(), "all").Add(float64(newFiles))
	}
	if newBlobs > 0 {
		s.metrics.BlobsDeduped.Add(float64(newBlobs))
	}
	return nil
}

// onEnvelope is the Next handed to every source.
func (s *Service) onEnvelope(_ context.Context, env pipeline.Envelope) error {
	s.metrics.IngestRecords.WithLabelValues(env.Source, "accepted").Inc()

	// Clock skew is recorded, never corrected (§20).
	if env.Step.StartedAt > 0 {
		skew := time.Since(time.UnixMicro(env.Step.StartedAt)).Seconds()
		s.metrics.ClockSkew.WithLabelValues(env.Source).Observe(skew)
	}

	s.assembler.Add(env)
	return nil
}

// onEpisode runs the processors and hands the episode to the sink.
//
// A processor error quarantines the episode. It is never passed on: that is
// what fail-closed means (F-5.5), and it is the difference between a
// misconfigured policy being a nuisance and being a breach.
func (s *Service) onEpisode(ep *pipeline.Assembled) {
	ctx := context.Background()

	for _, p := range []pipeline.Processor{s.redactor, s.extractor} {
		if err := p.Process(ctx, ep); err != nil {
			s.quarantine(ep, p.Name(), err)
			return
		}
	}

	s.metrics.EpisodesEmitted.WithLabelValues(ep.Episode.Status).Inc()
	if ep.Episode.Status == record.StatusEvicted {
		s.metrics.Evictions.Inc()
	}
	s.metrics.EntityKeysPerEpisode.Observe(float64(len(ep.Episode.EntityKeys)))
	s.metrics.EpisodesWithoutKeysRatio.Set(s.extractor.CoverageRatio())

	if err := s.sink.Write(ctx, []*pipeline.Assembled{ep}); err != nil {
		s.metrics.SinkErrors.WithLabelValues(s.sink.Name(), "write").Inc()
		s.log.Error("sink write failed", "error", err, "episode_id", ep.Episode.EpisodeID)
		return
	}

	s.mu.Lock()
	s.emitted++
	s.mu.Unlock()
}

// quarantine writes a failed episode aside with the reason (F-12 §12).
//
// The quarantine file contains the episode, because an operator has to be able
// to see what failed to redact in order to fix the policy. It is written under
// the sink root so it inherits the same bucket-level access controls as the
// data, and it is deliberately not logged.
func (s *Service) quarantine(ep *pipeline.Assembled, stage string, cause error) {
	s.metrics.Quarantined.WithLabelValues(stage).Inc()
	s.mu.Lock()
	s.quarantined++
	s.mu.Unlock()

	// The log line names the episode and the stage, never the payload or
	// the error's captured values (F-12.2).
	s.log.Warn("episode quarantined",
		"episode_id", ep.Episode.EpisodeID, "stage", stage, "reason", cause.Error())

	dir := filepath.Join(s.quarantineDir,
		"dt="+time.Now().UTC().Format("2006-01-02"), stage)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		s.log.Error("cannot create quarantine dir", "error", err)
		return
	}

	b, err := json.MarshalIndent(map[string]any{
		"reason":         cause.Error(),
		"stage":          stage,
		"episode":        ep.Episode,
		"steps":          ep.Steps,
		"quarantined_at": time.Now().UTC().Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return
	}

	path := filepath.Join(dir, ep.Episode.EpisodeID+".json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		s.log.Error("cannot write quarantine record", "error", err)
	}
}

// onManifest writes the redaction manifest (F-5.4). It contains rule ids,
// paths and counts, and no payload values.
func (s *Service) onManifest(m redact.Manifest) {
	for _, e := range m.Entries {
		s.metrics.RedactionMatches.WithLabelValues(e.RuleID, e.Action).Add(float64(e.Matches))
	}
}

// Shutdown stops sources and flushes.
func (s *Service) Shutdown(ctx context.Context) error {
	var errs []error
	for _, src := range s.sources {
		if err := src.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.sink.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
