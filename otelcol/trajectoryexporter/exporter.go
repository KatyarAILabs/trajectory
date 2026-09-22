// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package trajectoryexporter

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/normalize"
	"github.com/KatyarAILabs/trajectory/collector/service"
)

// Exporter runs the trajectory pipeline inside an OTel Collector.
type Exporter struct {
	cfg *config.Config
	svc *service.Service
	reg *normalize.Registry
	log *slog.Logger

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewExporter builds an exporter from validated config.
func NewExporter(cfg *config.Config, log *slog.Logger) (*Exporter, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	reg, err := normalize.LoadBuiltins()
	if err != nil {
		return nil, fmt.Errorf("trajectoryexporter: load conventions: %w", err)
	}

	svc, err := service.NewEmbedded(cfg, log)
	if err != nil {
		return nil, err
	}

	return &Exporter{cfg: cfg, svc: svc, reg: reg, log: log}, nil
}

// Start begins the background delivery loop.
func (e *Exporter) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.done = make(chan struct{})

	go func() {
		defer close(e.done)
		e.svc.RunBackground(runCtx)
	}()

	e.started = true
	return nil
}

// Shutdown drains and stops.
//
// The host collector gives a bounded shutdown window, and anything still
// queued stays in the buffer for the next start rather than being lost — which
// is the whole reason the buffer is on disk (F-8.1).
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return nil
	}
	e.cancel()

	select {
	case <-e.done:
	case <-ctx.Done():
		e.log.Warn("shutdown deadline reached; undelivered records remain buffered " +
			"and will be delivered on next start")
	}

	e.started = false
	return e.svc.Shutdown(ctx)
}

// ConsumeTraces is the exporter entry point.
//
// Spans are flattened and handed to the same normaliser the standalone binary
// uses, so an episode produced here is byte-identical to one produced there
// from the same spans.
func (e *Exporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	order := e.cfg.Assembly.SessionKey
	if len(order) == 0 {
		order = []string{"session.id", "gen_ai.conversation.id", "trace_id"}
	}

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resourceAttrs := attrsToMap(rs.Resource().Attributes())

		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope := ss.Scope()

			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				attrs := make(map[string]string, len(resourceAttrs)+span.Attributes().Len())
				for key, v := range resourceAttrs {
					attrs[key] = v
				}
				for key, v := range attrsToMap(span.Attributes()) {
					attrs[key] = v
				}

				s := normalize.Span{
					TraceID:      span.TraceID().String(),
					SpanID:       span.SpanID().String(),
					Name:         span.Name(),
					StartedAtUS:  int64(span.StartTimestamp()) / 1000,
					EndedAtUS:    int64(span.EndTimestamp()) / 1000,
					Attributes:   attrs,
					ScopeName:    scope.Name(),
					ScopeVersion: scope.Version(),
				}
				if !span.ParentSpanID().IsEmpty() {
					s.ParentSpanID = span.ParentSpanID().String()
				}
				if span.Status().Code() == ptrace.StatusCodeError {
					s.StatusError = true
					s.StatusMessage = span.Status().Message()
				}

				env := normalize.Normalize(s, "otelcol", order, e.reg)
				if err := e.svc.Ingest(ctx, env); err != nil {
					// Returning an error asks the host
					// collector to retry, which is correct:
					// the data was not accepted.
					return err
				}
			}
		}
	}
	return nil
}

// Capabilities reports that this exporter does not mutate the data it is given.
func (e *Exporter) Capabilities() bool { return false }

func attrsToMap(m pcommon.Map) map[string]string {
	out := make(map[string]string, m.Len())
	for k, v := range m.All() {
		out[k] = v.AsString()
	}
	return out
}
