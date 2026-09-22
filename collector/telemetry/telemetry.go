// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package telemetry exposes the collector's own metrics and health endpoints
// (F-11.2, §11).
//
// One rule governs every metric here and it is not negotiable: no metric label
// may carry user data (§11). Not a tenant's free-text task_type, not a tool
// name, not a fragment of an argument. A label is a cardinality explosion and
// a data leak at the same time — Prometheus retains it, scrapes ship it off
// the box, and dashboards render it to anyone with read access.
//
// Labels in this package are drawn only from operator-controlled config
// (source names, sink names, rule ids) and from closed enums this code
// defines. That is why, for example, quarantine is labelled by reason rather
// than by the record that failed.
package telemetry

import (
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KatyarAILabs/trajectory/internal/version"
)

// Metrics holds every collector metric from §11 that Phase 1 can move.
type Metrics struct {
	reg *prometheus.Registry

	IngestRecords *prometheus.CounterVec
	IngestBytes   *prometheus.CounterVec

	EpisodesEmitted  *prometheus.CounterVec
	EpisodesInFlight prometheus.Gauge
	Evictions        prometheus.Counter

	RedactionMatches *prometheus.CounterVec
	RedactionErrors  *prometheus.CounterVec
	Quarantined      *prometheus.CounterVec

	EntityKeysPerEpisode     prometheus.Histogram
	EpisodesWithoutKeysRatio prometheus.Gauge

	SinkWriteDuration *prometheus.HistogramVec
	SinkErrors        *prometheus.CounterVec
	FilesWritten      *prometheus.CounterVec
	BlobsDeduped      prometheus.Counter

	// Sampled is labelled by decision and by rule. The rule label is
	// operator-authored config text, not user data, which is what keeps it
	// inside the §11 no-user-data rule.
	Sampled *prometheus.CounterVec
	// OutcomesIngested counts business outcomes accepted for the join.
	OutcomesIngested *prometheus.CounterVec

	// Shed counts records refused before the buffer, by source and reason
	// (quota or backpressure). F-7.3 asks for "a metric for what was shed".
	Shed *prometheus.CounterVec
	// LostOnRestart is the F-3.8 metric: in-flight episodes the previous
	// process held when it stopped uncleanly.
	LostOnRestart prometheus.Counter
	// ConfigReloads counts hot reloads by result (F-11.4, §12).
	ConfigReloads *prometheus.CounterVec

	// Buffer metrics (§11). BufferEvicted is labelled by reason because
	// "the buffer dropped data" and "why" are different operational
	// questions, and the reason determines the fix: more disk, a shorter
	// max_age, or a sink that is actually reachable.
	BufferBytes        prometheus.Gauge
	BufferDiskBytes    prometheus.Gauge
	BufferOldestAge    prometheus.Gauge
	BufferEvicted      *prometheus.CounterVec
	BufferBackpressure prometheus.Gauge
	Delivered          prometheus.Counter
	DeliveryRetries    prometheus.Counter
	DeadLettered       prometheus.Counter

	ClockSkew *prometheus.HistogramVec
	BuildInfo *prometheus.GaugeVec

	ready atomic.Bool
	pprof bool
}

// New registers the metric set.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	// Process and Go runtime metrics. These are what make the §13 memory
	// targets measurable rather than asserted: process_resident_memory_bytes
	// is the number a soak test reads, and go_goroutines is how an
	// unbounded queue shows itself before it becomes an OOM.
	//
	// Neither carries a label derived from user data.
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	m.IngestRecords = counterVec(reg, "cc_ingest_records_total",
		"Records accepted or rejected, by source and result.", "source", "result")
	m.IngestBytes = counterVec(reg, "cc_ingest_bytes_total",
		"Bytes received, by source.", "source")

	m.EpisodesEmitted = counterVec(reg, "cc_episodes_emitted_total",
		"Episodes emitted from assembly, by terminal status.", "status")
	m.EpisodesInFlight = gauge(reg, "cc_episodes_in_flight",
		"Episodes currently open in the assembly buffer.")
	m.Evictions = counter(reg, "cc_assembly_window_evictions_total",
		"Episodes emitted early because the assembly buffer was at capacity.")

	m.RedactionMatches = counterVec(reg, "cc_redaction_matches_total",
		"Redaction rule matches, by rule and action.", "rule_id", "action")
	m.RedactionErrors = counterVec(reg, "cc_redaction_errors_total",
		"Redaction policy evaluation errors, by rule.", "rule_id")
	m.Quarantined = counterVec(reg, "cc_quarantined_total",
		"Records quarantined, by reason.", "reason")

	m.EntityKeysPerEpisode = histogram(reg, "cc_entity_keys_per_episode",
		"Entity keys extracted per episode.",
		[]float64{0, 1, 2, 3, 5, 10, 25})
	m.EpisodesWithoutKeysRatio = gauge(reg, "cc_episodes_without_entity_keys_ratio",
		"Share of episodes with no entity keys. The leading indicator that a future join will fail.")

	m.SinkWriteDuration = histogramVec(reg, "cc_sink_write_duration_seconds",
		"Time to flush a batch to a sink.",
		prometheus.DefBuckets, "sink")
	m.SinkErrors = counterVec(reg, "cc_sink_errors_total",
		"Sink write failures, by sink and code.", "sink", "code")
	m.FilesWritten = counterVec(reg, "cc_files_written_total",
		"Files written, by sink and table.", "sink", "table")
	m.BlobsDeduped = counter(reg, "cc_blobs_deduped_total",
		"Payloads that matched an existing blob and were not rewritten.")

	m.BufferBytes = gauge(reg, "cc_buffer_bytes",
		"Bytes buffered but not yet delivered. This is the backlog.")
	m.BufferDiskBytes = gauge(reg, "cc_buffer_disk_bytes",
		"Bytes the buffer occupies on disk, including delivered records "+
			"whose segment has not yet been reclaimed.")
	m.BufferOldestAge = gauge(reg, "cc_buffer_oldest_age_seconds",
		"Age of the oldest undelivered record in the buffer.")
	m.BufferEvicted = counterVec(reg, "cc_buffer_evicted_bytes_total",
		"Bytes dropped from the buffer, by reason.", "reason")
	m.BufferBackpressure = gauge(reg, "cc_buffer_backpressure",
		"1 when the buffer is above its backpressure threshold.")
	m.Delivered = counter(reg, "cc_delivered_total",
		"Records delivered from the buffer to a sink.")
	m.DeliveryRetries = counter(reg, "cc_delivery_retries_total",
		"Delivery attempts that failed and will be retried.")
	m.DeadLettered = counter(reg, "cc_dead_lettered_total",
		"Records that exhausted their delivery attempts.")

	m.OutcomesIngested = counterVec(reg, "cc_outcomes_ingested_total",
		"Business outcomes accepted for the outcome join, by source.", "source")
	m.Shed = counterVec(reg, "cc_shed_total",
		"Records refused before buffering, by source and reason.", "source", "reason")
	m.LostOnRestart = counter(reg, "cc_assembly_lost_on_restart_total",
		"In-flight episodes the previous process held when it stopped without draining.")
	m.ConfigReloads = counterVec(reg, "cc_config_reloads_total",
		"Configuration reloads, by result.", "result")

	m.Sampled = counterVec(reg, "cc_sampled_total",
		"Sampling decisions, by decision and rule.", "decision", "rule")

	m.ClockSkew = histogramVec(reg, "cc_clock_skew_seconds",
		"Difference between producer and collector clocks. Recorded, never corrected.",
		[]float64{-60, -10, -1, 0, 1, 10, 60, 300}, "source")

	m.BuildInfo = gaugeVec(reg, "cc_build_info",
		"Build identity. Always 1.", "version", "commit", "schema_version")
	m.BuildInfo.WithLabelValues(version.Collector, version.Commit, version.Schema).Set(1)

	return m
}

// EnablePprof exposes Go profiling endpoints on the telemetry listener.
//
// Off by default and opt-in through config. It is genuinely useful — a
// throughput regression is far easier to find with a profile than with a
// guess — but /debug/pprof also exposes command-line arguments and memory
// contents, so it must never be on by accident. The telemetry listener is
// expected to be bound to a private interface.
func (m *Metrics) EnablePprof() { m.pprof = true }

// Handler serves /metrics, /healthz and /readyz (F-11.2).
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))

	if m.pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	// healthz is liveness: the process is running. It deliberately does not
	// consult the sink, because a sink outage must not cause an orchestrator
	// to kill a collector that is correctly buffering through it.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// readyz is readiness: sources are bound and the pipeline can accept.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("starting"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	return mux
}

// SetReady flips readiness once sources are listening.
func (m *Metrics) SetReady(v bool) { m.ready.Store(v) }

// Serve runs the telemetry listener until ctx-driven shutdown via Close.
func (m *Metrics) Serve(listen string) (*http.Server, error) {
	srv := &http.Server{
		Handler:           m.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

func counter(r *prometheus.Registry, name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	r.MustRegister(c)
	return c
}

func counterVec(r *prometheus.Registry, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	r.MustRegister(c)
	return c
}

func gauge(r *prometheus.Registry, name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	r.MustRegister(g)
	return g
}

func gaugeVec(r *prometheus.Registry, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	r.MustRegister(g)
	return g
}

func histogram(r *prometheus.Registry, name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	r.MustRegister(h)
	return h
}

func histogramVec(r *prometheus.Registry, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	r.MustRegister(h)
	return h
}
