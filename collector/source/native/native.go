// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package native serves the collector's own episode API (§9.1, §9.2).
//
// This is the UC-3 path: an ML engineer with a custom agent loop calls it
// directly, supplying fields auto-instrumentation cannot see — tool versions,
// token spans, trainable masks, group ids. It is the highest-fidelity source
// and the reference implementation of the format.
package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/wire"
)

// Options configure a Receiver.
type Options struct {
	Name            string
	Listen          string
	Tenant          string
	MaxRequestBytes int64
	// AuthToken, when set, is required as a bearer token on every request
	// (F-12.1).
	AuthToken string
}

// Receiver serves the native HTTP API.
type Receiver struct {
	opts Options
	srv  *http.Server

	accepted atomic.Int64
	rejected atomic.Int64
	episodes atomic.Int64
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

// Stats are the counters this source contributes to §11.
type Stats struct {
	Accepted, Rejected, Episodes, Bytes int64
}

// Stats returns a snapshot.
func (r *Receiver) Stats() Stats {
	return Stats{
		Accepted: r.accepted.Load(),
		Rejected: r.rejected.Load(),
		Episodes: r.episodes.Load(),
		Bytes:    r.bytes.Load(),
	}
}

// Start serves until ctx is cancelled.
func (r *Receiver) Start(ctx context.Context, next pipeline.Next) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/episodes", r.auth(r.handleEpisodes(next)))
	mux.HandleFunc("/v1/spans", r.auth(r.handleEpisodes(next)))

	// §9.4: reserved. It exists so a design partner can prove a join by
	// hand without the collector growing a CDC subsystem, but v1 writes no
	// outcomes (N-1, N-2), so it must not silently accept and discard.
	mux.HandleFunc("/v1/outcomes", r.auth(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "the outcomes endpoint is reserved and not implemented in v1; "+
			"the outcomes table is created empty and never written (spec §2.3, §9.4)",
			http.StatusNotImplemented)
	}))

	r.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	ln, err := net.Listen("tcp", r.opts.Listen)
	if err != nil {
		return fmt.Errorf("native source %q: listen %s: %w", r.opts.Name, r.opts.Listen, err)
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

// auth enforces the per-source bearer token when one is configured (F-12.1).
func (r *Receiver) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if r.opts.AuthToken == "" {
			h(w, req)
			return
		}
		if req.Header.Get("Authorization") != "Bearer "+r.opts.AuthToken {
			r.rejected.Add(1)
			// No detail about what was wrong with the token: an error
			// that distinguishes "absent" from "incorrect" is a probe
			// oracle.
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, req)
	}
}

type response struct {
	Accepted int    `json:"accepted"`
	Steps    int    `json:"steps"`
	Message  string `json:"message,omitempty"`
}

func (r *Receiver) handleEpisodes(next pipeline.Next) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Reject oversized requests rather than buffering them (F-1.7).
		if req.ContentLength > r.opts.MaxRequestBytes {
			r.rejected.Add(1)
			http.Error(w, fmt.Sprintf("request body exceeds max_request_bytes (%d)",
				r.opts.MaxRequestBytes), http.StatusRequestEntityTooLarge)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, r.opts.MaxRequestBytes))
		if err != nil {
			r.rejected.Add(1)
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		r.bytes.Add(int64(len(body)))

		eps, err := wire.DecodeEpisodes(body)
		if err != nil {
			r.rejected.Add(1)
			// §9.1 requires a field path on a schema error. The decoder's
			// messages name the episode and step, never the payload
			// value (F-12.2).
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		steps := 0
		for _, ep := range eps {
			envs, err := ep.ToEnvelopes(r.opts.Name, r.opts.Tenant)
			if err != nil {
				r.rejected.Add(1)
				writeJSONError(w, http.StatusBadRequest, err.Error())
				return
			}
			for _, env := range envs {
				if err := next(req.Context(), env); err != nil {
					// The pipeline declined it. Tell the
					// producer to retry rather than
					// acknowledging data never accepted.
					w.Header().Set("Retry-After", "1")
					writeJSONError(w, http.StatusServiceUnavailable,
						"collector cannot accept data")
					return
				}
				steps++
			}
		}

		r.accepted.Add(1)
		r.episodes.Add(int64(len(eps)))

		writeJSON(w, http.StatusAccepted, response{Accepted: len(eps), Steps: steps})
	}
}
