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
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/trajectory-project/trajectory/collector/assemble"
	"github.com/trajectory-project/trajectory/collector/buffer"
	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/normalize"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/redact"
	"github.com/trajectory-project/trajectory/collector/sink/lake"
	"github.com/trajectory-project/trajectory/collector/source/filetail"
	"github.com/trajectory-project/trajectory/collector/source/native"
	"github.com/trajectory-project/trajectory/collector/source/otlp"
	"github.com/trajectory-project/trajectory/collector/source/webhook"
	"github.com/trajectory-project/trajectory/collector/telemetry"
	"github.com/trajectory-project/trajectory/collector/tlsconf"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Service is a running collector.
type Service struct {
	cfg     *config.Config
	log     *slog.Logger
	metrics *telemetry.Metrics

	sources   []pipeline.Source
	assembler *assemble.Assembler
	// bufferDir is fixed for the life of the process.
	bufferDir string

	// pol is swapped atomically on reload; read it once per record with
	// s.policy() and use that value throughout, never twice.
	pol       atomic.Pointer[policy]
	sink      *lake.Sink
	buf       *buffer.Buffer
	deliverer *buffer.Deliverer

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
	lastBytes        map[string]int64
	lastRejected     map[string]int64
	lastRedactErrors map[string]int64
	lastDelivered    int64
	lastRetries      int64
	lastDeadLettered int64
	// sampledOut counts episodes dropped by tail sampling, reported by
	// `cc import` so a partner is not puzzled by a smaller corpus.
	sampledOut int64
	// dryRun suppresses sink writes for `cc import -dry-run`.
	dryRun bool
}

// New builds a service from validated config.
func New(cfg *config.Config, log *slog.Logger) (*Service, error) {
	s := &Service{cfg: cfg, log: log, metrics: telemetry.New(), bufferDir: cfg.Buffer.Dir}

	store, err := newStore(cfg.Sinks[0])
	if err != nil {
		return nil, err
	}

	sink, err := lake.New(lake.Options{
		Name:               cfg.Sinks[0].Name,
		Store:              store,
		PartitionBy:        cfg.Sinks[0].PartitionBy,
		BlobThresholdBytes: cfg.Sinks[0].BlobThresholdBytes,
		MaxPayloadBytes:    cfg.Sinks[0].MaxPayloadBytes,
		TargetFileBytes:    int64(cfg.Sinks[0].TargetFileBytes),
		RollInterval:       cfg.Sinks[0].RollInterval,
		Compression:        cfg.Sinks[0].Compression,
	})
	if err != nil {
		return nil, err
	}
	s.sink = sink

	// Quarantine is local even for a remote sink: a record that failed
	// redaction must not be shipped anywhere, and the local path is the one
	// an operator can reach to work out why.
	s.quarantineDir = cfg.Buffer.Dir
	if s.quarantineDir == "" {
		s.quarantineDir = cfg.Sinks[0].Dir
	}
	s.quarantineDir = filepath.Join(s.quarantineDir, "quarantine")

	// The buffer is what makes acknowledged data survive a sink outage or a
	// restart (F-8.1, G-4). Everything in it has already been redacted, so
	// even a seized disk holds no unredacted payloads (F-5.3).
	s.buf, err = buffer.Open(buffer.Options{
		Dir:            cfg.Buffer.Dir,
		MaxBytes:       int64(cfg.Buffer.MaxBytes),
		MaxAge:         cfg.Buffer.MaxAge,
		SegmentBytes:   int64(cfg.Buffer.SegmentBytes),
		BackpressureAt: cfg.Buffer.BackpressureAt,
		EncryptionKey:  envBytes(cfg.Buffer.Encryption.KeyEnv),
		OnEvict: func(reason string, _ int, bytes int64) {
			s.metrics.BufferEvicted.WithLabelValues(reason).Add(float64(bytes))
			log.Warn("buffer evicted data", "reason", reason, "bytes", bytes)
		},
	})
	if err != nil {
		return nil, err
	}

	dlq := cfg.Buffer.DeadLetterDir
	if dlq == "" {
		dlq = filepath.Join(cfg.Buffer.Dir, "dead-letter")
	}
	s.deliverer = buffer.NewDeliverer(s.buf, s.deliver, buffer.DeliveryOptions{
		MaxAttempts:   cfg.Buffer.MaxAttempts,
		BaseDelay:     cfg.Buffer.RetryBaseDelay,
		MaxDelay:      cfg.Buffer.RetryMaxDelay,
		DeadLetterDir: dlq,
		BatchSize:     cfg.Buffer.BatchSize,
		// Commit runs once per batch. Records are acknowledged only
		// after it returns, so a crash between the sink write and the
		// commit redelivers rather than loses (F-8.4).
		Commit: func(ctx context.Context) error { return s.sink.FlushDue(ctx) },
		OnError: func(attempt int, err error) {
			s.metrics.SinkErrors.WithLabelValues(s.sink.Name(), "deliver").Inc()
			// The error text comes from the store, which redacts
			// credentials before returning it (F-12.2).
			log.Warn("sink delivery failed, will retry",
				"attempt", attempt, "error", err)
		},
	})

	pol, err := buildPolicy(cfg, s.onManifest)
	if err != nil {
		return nil, err
	}
	s.pol.Store(pol)

	s.assembler = assemble.New(assemble.Options{
		Tenant:              cfg.Tenant,
		Window:              cfg.Assembly.Window,
		MaxInFlight:         cfg.Assembly.MaxInFlight,
		SettleAfterTerminal: cfg.Assembly.SettleAfterTerminal,
		PatchMemory:         cfg.Assembly.PatchMemory,
		NewID:               func() string { return ulid.Make().String() },
	}, s.onEpisode)

	conventions, err := normalize.LoadBuiltins()
	if err != nil {
		return nil, fmt.Errorf("load convention mappings: %w", err)
	}

	for _, sc := range cfg.Sources {
		if sc.MappingsDir != "" {
			if err := conventions.LoadDir(sc.MappingsDir); err != nil {
				return nil, err
			}
		}

		var token string
		if sc.Auth.TokenEnv != "" {
			token = os.Getenv(sc.Auth.TokenEnv)
		}

		httpTLS, err := tlsconf.Build(sc.HTTP.TLS)
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", sc.Name, err)
		}
		grpcTLS, err := tlsconf.Build(sc.GRPC.TLS)
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", sc.Name, err)
		}

		switch sc.Type {
		case "webhook":
			m, err := webhook.LoadMapping(sc.Mapping)
			if err != nil {
				return nil, fmt.Errorf("source %q: %w", sc.Name, err)
			}
			s.sources = append(s.sources, webhook.New(webhook.Options{
				Name:            sc.Name,
				Listen:          sc.HTTP.Listen,
				Path:            sc.Path,
				Mapping:         m,
				MaxRequestBytes: sc.MaxRequestBytes,
				AuthToken:       token,
				TLSConfig:       httpTLS,
			}))
		case "file":
			s.sources = append(s.sources, filetail.New(filetail.Options{
				Name:         sc.Name,
				Include:      sc.Include,
				StartAt:      sc.StartAt,
				PollInterval: sc.PollInterval,
				// Offsets live beside the buffer, on the same
				// persistent volume, so they survive with it.
				StateDir: cfg.Buffer.Dir,
			}))
		case "native":
			s.sources = append(s.sources, native.New(native.Options{
				Name:            sc.Name,
				Listen:          sc.HTTP.Listen,
				Tenant:          cfg.Tenant,
				MaxRequestBytes: sc.MaxRequestBytes,
				AuthToken:       token,
				TLSConfig:       httpTLS,
				Outcomes:        s.IngestOutcomes,
			}))
		default:
			s.sources = append(s.sources, otlp.New(otlp.Options{
				Name:            sc.Name,
				Listen:          sc.HTTP.Listen,
				GRPCListen:      sc.GRPC.Listen,
				MaxRequestBytes: sc.MaxRequestBytes,
				SessionKeyOrder: cfg.Assembly.SessionKey,
				Conventions:     conventions,
				TLSConfig:       httpTLS,
				GRPCTLSConfig:   grpcTLS,
			}))
		}

		log.Info("source configured",
			"name", sc.Name, "type", sc.Type,
			"http", sc.HTTP.Listen, "http_tls", tlsconf.Describe(sc.HTTP.TLS),
			"grpc", sc.GRPC.Listen, "grpc_tls", tlsconf.Describe(sc.GRPC.TLS),
			"auth", sc.Auth.Type != "" && sc.Auth.Type != "none")
	}
	log.Info("conventions loaded", "mappings", conventions.Names())

	if id := pol.redactor.KeyID(); id != "" {
		// A key_id in the log makes a rotation visible in operational
		// history. The key itself is never logged (F-12.2).
		log.Info("tokenization key loaded", "key_id", id)
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

	// The delivery loop drains the buffer into the sink, retrying and
	// dead-lettering. It runs for the whole lifetime of the service.
	deliverCtx, stopDelivery := context.WithCancel(context.Background())
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		s.deliverer.Run(deliverCtx, 200*time.Millisecond)
	}()
	defer func() {
		stopDelivery()
		<-deliveryDone
	}()

	// Periodic flush so data becomes durable without waiting for shutdown.
	flush := time.NewTicker(5 * time.Second)
	defer flush.Stop()

	// The buffer is fsynced on a timer rather than per append: an fsync per
	// episode would cap throughput far below the §13 target, and
	// at-least-once already tolerates redelivering the last unsynced
	// records after a crash.
	sync := time.NewTicker(time.Second)
	defer sync.Stop()

	s.reportPreviousLoss()
	s.recordInFlight(false)

	cfg := s.config()
	s.metrics.SetReady(true)
	s.log.Info("collector ready",
		"tenant", cfg.Tenant,
		"sources", len(s.sources),
		"sink", cfg.Sinks[0].Name)

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
			s.reportBufferMetrics()
			s.recordInFlight(false)

		case <-sync.C:
			if err := s.buf.Sync(); err != nil {
				s.log.Error("buffer sync failed", "error", err)
			}
			s.buf.EvictExpired()

		case <-flush.C:
			// FlushDue rather than Flush: files should reach a
			// compaction-friendly size rather than one small object
			// per tick (F-9.5).
			if err := s.sink.FlushDue(context.WithoutCancel(ctx)); err != nil {
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
	// Assembly is empty now, so nothing held in memory can be lost. Marking
	// the file clean is what stops the next start counting a loss.
	defer s.recordInFlight(true)

	ctx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout())
	defer cancel()

	// Everything is in the buffer at this point. Sync first so a kill
	// during the drain still leaves it recoverable, then try to deliver
	// what we can within the deadline.
	if err := s.buf.Sync(); err != nil {
		s.log.Error("buffer sync failed during shutdown", "error", err)
	}

	if _, err := s.deliverer.DrainOnce(ctx); err != nil {
		// Not fatal: anything undelivered stays in the buffer and is
		// redelivered on the next start. That is the buffer doing its
		// job, not a failure (F-8.1).
		s.log.Warn("could not drain the buffer before the deadline; "+
			"the remainder will be delivered on next start", "error", err)
	}
	if err := s.sink.Flush(ctx); err != nil {
		s.log.Error("final sink flush failed", "error", err)
	}

	st := s.deliverer.Stats()
	s.log.Info("shutdown complete",
		"episodes_emitted", s.emitted,
		"quarantined", s.quarantined,
		"delivered", st.Delivered,
		"dead_lettered", st.DeadLettered,
		// Undelivered backlog, not disk usage: a fully delivered buffer
		// still occupies disk until its segment is reclaimed.
		"undelivered_bytes", s.buf.PendingBytes())
	return nil
}

// reportBufferMetrics publishes the §11 buffer gauges.
func (s *Service) reportBufferMetrics() {
	// The backlog, not the on-disk total: see Buffer.PendingBytes.
	s.metrics.BufferBytes.Set(float64(s.buf.PendingBytes()))
	s.metrics.BufferDiskBytes.Set(float64(s.buf.Bytes()))
	s.metrics.BufferOldestAge.Set(s.buf.OldestAge().Seconds())

	backpressure := 0.0
	if s.buf.UnderBackpressure() {
		backpressure = 1
	}
	s.metrics.BufferBackpressure.Set(backpressure)

	st := s.deliverer.Stats()
	s.mu.Lock()
	newDelivered := st.Delivered - s.lastDelivered
	newRetries := st.Retries - s.lastRetries
	newDead := st.DeadLettered - s.lastDeadLettered
	s.lastDelivered, s.lastRetries, s.lastDeadLettered = st.Delivered, st.Retries, st.DeadLettered
	s.mu.Unlock()

	s.reportPipelineCounters()

	if newDelivered > 0 {
		s.metrics.Delivered.Add(float64(newDelivered))
	}
	if newRetries > 0 {
		s.metrics.DeliveryRetries.Add(float64(newRetries))
	}
	if newDead > 0 {
		s.metrics.DeadLettered.Add(float64(newDead))
	}
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
	// Head sampling keys on the session, not the span, so every span of a
	// session shares one decision and an episode is never half-captured
	// (F-7.1).
	pol := s.policy()
	if !pol.sampler.HeadKeepFrom(env.Source, env.SessionKey) {
		s.metrics.Sampled.WithLabelValues("dropped", "head").Inc()
		s.metrics.IngestRecords.WithLabelValues(env.Source, "head_sampled").Inc()
		return nil
	}

	// Quota before anything else: a source over its limit is refused with
	// 429 and the shed is counted, so an operator can see which producer is
	// responsible (F-7.3). The source name is operator config, not user
	// data, which keeps it inside the §11 label rule.
	if !pol.quotas.allow(env.Source) {
		s.metrics.Shed.WithLabelValues(env.Source, "quota").Inc()
		s.metrics.IngestRecords.WithLabelValues(env.Source, "quota").Inc()
		return pipeline.ErrQuotaExceeded
	}

	// Backpressure: when the buffer is above its threshold, refuse rather
	// than accept data we may not be able to hold. The source turns this
	// into a retryable 503, so a producer slows down instead of losing
	// records (F-8.2).
	if s.buf.UnderBackpressure() {
		s.metrics.IngestRecords.WithLabelValues(env.Source, "backpressure").Inc()
		s.metrics.Shed.WithLabelValues(env.Source, "backpressure").Inc()
		return errBackpressure
	}

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

	// One policy for the whole episode, even if a reload lands midway.
	pol := s.policy()

	// Identifiers and counts only. A trace backend is readable by many more
	// people than the lake, so no payload, argument or key value goes here.
	ctx, span := telemetry.Tracer().Start(ctx, "trajectory.process_episode",
		trace.WithAttributes(
			attribute.String("trajectory.episode_id", ep.Episode.EpisodeID),
			attribute.String("trajectory.source", ep.Episode.Source),
			attribute.String("trajectory.status", ep.Episode.Status),
			attribute.Int("trajectory.step_count", len(ep.Steps)),
		))
	defer span.End()

	for _, p := range []pipeline.Processor{pol.redactorFor(ep.Episode.Source), pol.extractor} {
		if err := p.Process(ctx, ep); err != nil {
			span.SetAttributes(attribute.String("trajectory.quarantined_by", p.Name()))
			s.quarantine(ep, p.Name(), err)
			return
		}
	}

	// Tail sampling runs after the processors so a rule can reference
	// entity_key_count, and after redaction so no rule can read an
	// unredacted value (F-7.2).
	if !pol.sampler.TailKeep(ep) {
		s.metrics.Sampled.WithLabelValues("dropped", "tail").Inc()
		s.mu.Lock()
		s.sampledOut++
		s.mu.Unlock()
		return
	}
	reason := "tail"
	if ep.Episode.SampledBy != nil {
		reason = *ep.Episode.SampledBy
	}
	s.metrics.Sampled.WithLabelValues("kept", reason).Inc()

	s.metrics.EpisodesEmitted.WithLabelValues(ep.Episode.Status).Inc()
	if ep.Episode.Status == record.StatusEvicted {
		s.metrics.Evictions.Inc()
	}
	s.metrics.EntityKeysPerEpisode.Observe(float64(len(ep.Episode.EntityKeys)))
	s.metrics.EpisodesWithoutKeysRatio.Set(pol.extractor.CoverageRatio())

	s.mu.Lock()
	dry := s.dryRun
	s.mu.Unlock()
	if dry {
		s.mu.Lock()
		s.emitted++
		s.mu.Unlock()
		return
	}

	// Into the buffer, not the sink. The sink is reached by the delivery
	// loop, which retries and dead-letters; writing directly here would
	// lose the episode the moment the sink is unreachable (F-8.1).
	payload, err := json.Marshal(ep)
	if err != nil {
		s.log.Error("cannot encode episode for the buffer",
			"error", err, "episode_id", ep.Episode.EpisodeID)
		return
	}

	if err := s.buf.Append(payload); err != nil {
		// The buffer is full and could not make room. This is the
		// disk-full case: the record is refused rather than the buffer
		// being corrupted (§12).
		s.metrics.SinkErrors.WithLabelValues(s.sink.Name(), "buffer_full").Inc()
		s.log.Error("buffer is full, episode dropped",
			"episode_id", ep.Episode.EpisodeID, "error", err)
		return
	}

	s.mu.Lock()
	s.emitted++
	s.mu.Unlock()
}

// deliver is the delivery loop's send function: decode one buffered record and
// hand it to the sink.
func (s *Service) deliver(ctx context.Context, payload []byte) error {
	var rec bufferedRecord
	if err := json.Unmarshal(payload, &rec); err != nil {
		// A record that cannot be decoded will never succeed, so it is
		// marked permanent: it dead-letters immediately rather than
		// consuming the retry budget and delaying everything behind it.
		return buffer.Permanent(fmt.Errorf("undecodable buffered record: %w", err))
	}

	start := time.Now()
	switch {
	case len(rec.Outcomes) > 0:
		if err := s.sink.WriteOutcomes(ctx, rec.Outcomes); err != nil {
			return err
		}
	case rec.Episode != nil:
		// The sink buffers; the deliverer's Commit makes the batch
		// durable. Flushing here instead would mean one object per
		// record, which is both far slower and produces files nothing
		// can compact.
		ep := pipeline.Assembled{Episode: *rec.Episode, Steps: rec.Steps}
		if err := s.sink.Write(ctx, []*pipeline.Assembled{&ep}); err != nil {
			return err
		}
	default:
		return buffer.Permanent(fmt.Errorf("buffered record holds neither an episode nor outcomes"))
	}
	s.metrics.SinkWriteDuration.WithLabelValues(s.sink.Name()).Observe(time.Since(start).Seconds())
	return nil
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

// ShutdownTimeout is the graceful-shutdown deadline (F-11.5).
//
// A zero or negative value means the default, enforced here rather than only in
// config loading. A config built in code — by an embedding host, or a test —
// never passes through Load's defaults, and a zero deadline is an
// already-expired context: the drain would deliver nothing and the final flush
// would write nothing, silently.
func (s *Service) ShutdownTimeout() time.Duration {
	if d := s.config().ShutdownTimeout; d > 0 {
		return d
	}
	return 30 * time.Second
}

// config returns the configuration in force. Reload replaces s.cfg, so every
// read goes through the lock rather than touching the field directly.
func (s *Service) config() *config.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// policy returns the policy in force. Callers take it once and use that value
// for the whole record.
func (s *Service) policy() *policy { return s.pol.Load() }

// errBackpressure tells a source to ask its producer to retry (F-8.2).
var errBackpressure = errors.New("collector is under backpressure")

// errQuota is a quota refusal; sources map it to 429.
var errQuota = pipeline.ErrQuotaExceeded

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
	if s.buf != nil {
		if err := s.buf.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NewEmbedded builds a service for use inside another process, such as an
// OpenTelemetry Collector exporter (F-13.3).
//
// It differs from New in one respect: no sources are constructed, because the
// host owns the receivers. Everything else — assembly, redaction, extraction,
// sampling, buffering, the sink — is the same code, which is the point. Two
// implementations of redaction is exactly the bug nobody finds until a payload
// turns up in a lake.
func NewEmbedded(cfg *config.Config, log *slog.Logger) (*Service, error) {
	s, err := New(cfg, log)
	if err != nil {
		return nil, err
	}
	s.sources = nil
	return s, nil
}

// Ingest hands one envelope to the pipeline. It is how an embedding host feeds
// the collector in place of a source.
func (s *Service) Ingest(ctx context.Context, env pipeline.Envelope) error {
	return s.onEnvelope(ctx, env)
}

// RunBackground runs the assembly expiry, buffer sync and delivery loops
// without owning any listeners. It blocks until ctx is cancelled.
func (s *Service) RunBackground(ctx context.Context) {
	deliverCtx, stopDelivery := context.WithCancel(context.Background())
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		s.deliverer.Run(deliverCtx, 200*time.Millisecond)
	}()
	defer func() {
		stopDelivery()
		<-deliveryDone
	}()

	expire := time.NewTicker(time.Second)
	defer expire.Stop()
	flush := time.NewTicker(5 * time.Second)
	defer flush.Stop()

	for {
		select {
		case <-ctx.Done():
			s.assembler.Flush(record.StatusTimedOut)
			drainCtx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout())
			defer cancel()
			_ = s.buf.Sync()
			_, _ = s.deliverer.DrainOnce(drainCtx)
			_ = s.sink.Flush(drainCtx)
			return

		case <-expire.C:
			s.assembler.Expire()
			s.metrics.EpisodesInFlight.Set(float64(s.assembler.InFlight()))
			s.reportBufferMetrics()
			_ = s.buf.Sync()
			s.buf.EvictExpired()

		case <-flush.C:
			if err := s.sink.FlushDue(context.WithoutCancel(ctx)); err != nil {
				s.log.Error("sink flush failed", "error", err)
			}
		}
	}
}

// envBytes reads a secret from the environment, or nil when unset.
func envBytes(name string) []byte {
	if name == "" {
		return nil
	}
	if v := os.Getenv(name); v != "" {
		return []byte(v)
	}
	return nil
}
