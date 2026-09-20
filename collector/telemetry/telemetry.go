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
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/trajectory-project/trajectory/internal/version"
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

	ClockSkew *prometheus.HistogramVec
	BuildInfo *prometheus.GaugeVec

	ready atomic.Bool
}

// New registers the metric set.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

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

	m.ClockSkew = histogramVec(reg, "cc_clock_skew_seconds",
		"Difference between producer and collector clocks. Recorded, never corrected.",
		[]float64{-60, -10, -1, 0, 1, 10, 60, 300}, "source")

	m.BuildInfo = gaugeVec(reg, "cc_build_info",
		"Build identity. Always 1.", "version", "commit", "schema_version")
	m.BuildInfo.WithLabelValues(version.Collector, version.Commit, version.Schema).Set(1)

	return m
}

// Handler serves /metrics, /healthz and /readyz (F-11.2).
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))

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
