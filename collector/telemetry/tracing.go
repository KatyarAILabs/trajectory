// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/trajectory-project/trajectory/internal/version"
)

// TraceConfig configures the collector's own traces (F-11.2).
type TraceConfig struct {
	// Endpoint is an OTLP receiver. Empty disables tracing entirely: no
	// exporter is built and no connection is attempted, because F-12.6
	// allows outbound traffic only to destinations an operator configured.
	Endpoint string
	// Protocol is "grpc" (default) or "http".
	Protocol string
	// Insecure disables TLS to the endpoint, for a collector sidecar on
	// localhost.
	Insecure bool
	// SampleRate is the fraction of operations traced. The default is low
	// because the collector handles every episode an organisation produces,
	// and tracing each one would make the observability of the pipeline
	// cost more than the pipeline.
	SampleRate float64
}

// Tracer is the collector's tracer. It is a no-op until SetupTracing
// installs a real one, so instrumented code never has to check.
var tracer trace.Tracer = noop.NewTracerProvider().Tracer("trajectory")

// Tracer returns the collector's tracer.
func Tracer() trace.Tracer { return tracer }

// SetupTracing installs an OTLP exporter and returns a shutdown function that
// flushes pending spans.
//
// What the collector traces is its own work — assembling, redacting, buffering,
// delivering — and never the content it is working on. Span attributes are
// limited to identifiers and counts (episode_id, step count, rule ids), for the
// same reason no metric label may carry user data (§11): a trace backend is a
// place many more people can read than the lake.
func SetupTracing(ctx context.Context, cfg TraceConfig) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	var exp sdktrace.SpanExporter
	var err error

	switch strings.ToLower(cfg.Protocol) {
	case "", "grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(trimScheme(cfg.Endpoint))}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err = otlptracegrpc.New(ctx, opts...)
	case "http":
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(trimScheme(cfg.Endpoint))}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err = otlptracehttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("telemetry.traces.protocol must be grpc or http, got %q", cfg.Protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("telemetry: build trace exporter: %w", err)
	}

	rate := cfg.SampleRate
	if rate <= 0 {
		rate = 0.01
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", "trajectory-collector"),
		attribute.String("service.version", version.Collector),
		attribute.String("trajectory.schema_version", version.Schema),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("trajectory")

	return tp.Shutdown, nil
}

// trimScheme accepts either "host:port" or "http://host:port", because both
// appear in the wild and the exporters want the bare form.
func trimScheme(endpoint string) string {
	for _, p := range []string{"https://", "http://"} {
		endpoint = strings.TrimPrefix(endpoint, p)
	}
	return strings.TrimSuffix(endpoint, "/")
}
