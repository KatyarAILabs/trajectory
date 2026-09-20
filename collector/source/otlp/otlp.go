// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package otlp receives OTLP traces over HTTP/protobuf (F-1.1).
//
// Phase 1 implements the HTTP/protobuf path only; gRPC is Phase 2. The receiver
// flattens pdata into normalize.Span so that convention mapping is testable
// without constructing protobuf payloads, and so a second convention is a new
// mapper rather than a new receiver.
package otlp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/trajectory-project/trajectory/collector/normalize"
	"github.com/trajectory-project/trajectory/collector/pipeline"
)

// Options configure a Receiver.
type Options struct {
	Name   string
	Listen string
	// MaxRequestBytes rejects oversized requests rather than buffering
	// them (F-1.7).
	MaxRequestBytes int64
	// SessionKeyOrder is the attribute precedence for grouping (F-3.1).
	SessionKeyOrder []string
}

// Receiver is an OTLP/HTTP trace source.
type Receiver struct {
	opts Options
	srv  *http.Server

	accepted atomic.Int64
	rejected atomic.Int64
	spans    atomic.Int64
	bytes    atomic.Int64
}

// New creates a receiver.
func New(opts Options) *Receiver {
	if opts.MaxRequestBytes <= 0 {
		opts.MaxRequestBytes = 16 << 20
	}
	return &Receiver{opts: opts}
}

// Name implements pipeline.Source.
func (r *Receiver) Name() string { return r.opts.Name }

// Stats are the counters this stage contributes to §11.
type Stats struct {
	Accepted int64
	Rejected int64
	Spans    int64
	Bytes    int64
}

// Stats returns a snapshot.
func (r *Receiver) Stats() Stats {
	return Stats{
		Accepted: r.accepted.Load(),
		Rejected: r.rejected.Load(),
		Spans:    r.spans.Load(),
		Bytes:    r.bytes.Load(),
	}
}

// Start serves until ctx is cancelled.
func (r *Receiver) Start(ctx context.Context, next pipeline.Next) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleTraces(next))

	r.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", r.opts.Listen)
	if err != nil {
		return fmt.Errorf("otlp source %q: listen %s: %w", r.opts.Name, r.opts.Listen, err)
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.srv.Shutdown(shutCtx)
	}()

	if err := r.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown implements pipeline.Source.
func (r *Receiver) Shutdown(ctx context.Context) error {
	if r.srv == nil {
		return nil
	}
	return r.srv.Shutdown(ctx)
}

// Addr reports the bound address, for tests that listen on :0.
func (r *Receiver) Addr() string { return r.opts.Listen }

func (r *Receiver) handleTraces(next pipeline.Next) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Reject oversized requests with a clear error and a metric
		// rather than buffering them into memory (F-1.7).
		if req.ContentLength > r.opts.MaxRequestBytes {
			r.rejected.Add(1)
			http.Error(w, fmt.Sprintf("request body exceeds max_request_bytes (%d)",
				r.opts.MaxRequestBytes), http.StatusRequestEntityTooLarge)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, r.opts.MaxRequestBytes))
		if err != nil {
			r.rejected.Add(1)
			// The error text is deliberately generic: echoing a parse
			// failure risks quoting payload bytes into a response and
			// from there into a proxy log (F-12.2).
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		r.bytes.Add(int64(len(body)))

		creq := ptraceotlp.NewExportRequest()
		if err := creq.UnmarshalProto(body); err != nil {
			r.rejected.Add(1)
			http.Error(w, "malformed OTLP protobuf payload", http.StatusBadRequest)
			return
		}

		n, err := r.dispatch(req.Context(), creq.Traces(), next)
		if err != nil {
			// The pipeline declined the data. Tell the producer to
			// retry rather than acknowledging something that was
			// never accepted (F-8.4, §9.1).
			w.Header().Set("Retry-After", "1")
			http.Error(w, "collector cannot accept data", http.StatusServiceUnavailable)
			return
		}

		r.accepted.Add(1)
		r.spans.Add(int64(n))

		resp := ptraceotlp.NewExportResponse()
		b, err := resp.MarshalProto()
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	}
}

// dispatch flattens pdata and hands each span to the pipeline.
func (r *Receiver) dispatch(ctx context.Context, td ptrace.Traces, next pipeline.Next) (int, error) {
	count := 0

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

				// Resource attributes apply to every span under
				// them; span attributes win on conflict because
				// they are more specific.
				attrs := map[string]string{}
				for key, v := range resourceAttrs {
					attrs[key] = v
				}
				for key, v := range attrsToMap(span.Attributes()) {
					attrs[key] = v
				}

				s := normalize.Span{
					TraceID:     span.TraceID().String(),
					SpanID:      span.SpanID().String(),
					Name:        span.Name(),
					StartedAtUS: int64(span.StartTimestamp()) / 1000,
					EndedAtUS:   int64(span.EndTimestamp()) / 1000,
					Attributes:  attrs,
				}
				if !span.ParentSpanID().IsEmpty() {
					s.ParentSpanID = span.ParentSpanID().String()
				}
				if span.Status().Code() == ptrace.StatusCodeError {
					s.StatusError = true
					s.StatusMessage = span.Status().Message()
				}

				env := normalize.Normalize(s, r.opts.Name, r.opts.SessionKeyOrder)
				env.Meta.InstrumentationVersion = scope.Version()
				if name := scope.Name(); name != "" {
					env.Meta.Instrumentation = name
				}

				if err := next(ctx, env); err != nil {
					return count, err
				}
				count++
			}
		}
	}
	return count, nil
}

// attrsToMap flattens pdata attributes to strings.
//
// Everything becomes a string because the canonical record's raw map is
// map<string,string> and normalisation must stay lossless (F-2.2): a typed
// value this build does not map is still recoverable from its string form.
func attrsToMap(m pcommon.Map) map[string]string {
	out := make(map[string]string, m.Len())
	for k, v := range m.All() {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			out[k] = v.Str()
		case pcommon.ValueTypeInt:
			out[k] = strconv.FormatInt(v.Int(), 10)
		case pcommon.ValueTypeDouble:
			out[k] = strconv.FormatFloat(v.Double(), 'g', -1, 64)
		case pcommon.ValueTypeBool:
			out[k] = strconv.FormatBool(v.Bool())
		default:
			out[k] = v.AsString()
		}
	}
	return out
}
