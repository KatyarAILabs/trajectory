// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
)

type capture struct {
	ptraceotlp.UnimplementedGRPCServer
	mu    sync.Mutex
	spans []ptrace.Span
}

func (c *capture) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rss := req.Traces().ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sp := sss.At(j).Spans()
			for k := 0; k < sp.Len(); k++ {
				c.spans = append(c.spans, sp.At(k))
			}
		}
	}
	return ptraceotlp.NewExportResponse(), nil
}

// F-11.2: the collector exports its own traces over OTLP. This stands up a
// real OTLP receiver and checks spans arrive, rather than trusting that the
// exporter was constructed.
func TestTracesReachAnOTLPReceiver(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	cap := &capture{}
	ptraceotlp.RegisterGRPCServer(srv, cap)
	go srv.Serve(ln)
	defer srv.Stop()

	shutdown, err := SetupTracing(context.Background(), TraceConfig{
		Endpoint: ln.Addr().String(), Insecure: true, SampleRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, span := Tracer().Start(context.Background(), "trajectory.process_episode")
	span.SetAttributes(attribute.String("trajectory.episode_id", "ep-1"))
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.spans) == 0 {
		t.Fatal("no spans reached the OTLP receiver")
	}
	if got := cap.spans[0].Name(); got != "trajectory.process_episode" {
		t.Errorf("span name = %q", got)
	}
}

// F-12.6: with no endpoint, nothing is built and nothing connects.
func TestNoEndpointMeansNoExporter(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TraceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
