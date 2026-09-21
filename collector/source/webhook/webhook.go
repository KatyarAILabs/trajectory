// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package webhook receives gateway callbacks — LiteLLM, Portkey, Helicone —
// and maps them through a declarative mapping file (F-1.3, §9.3).
//
// This is UC-1: organisation-wide capture with no application change. Its
// limitation belongs in the package doc, because it decides whether this path
// is enough for a given team: a gateway sees model calls only. Tool calls made
// by the application never pass through it, so an episode captured here has the
// model turns and none of the actions between them.
package webhook

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ohler55/ojg/oj"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// Options configure a Receiver.
type Options struct {
	Name            string
	Listen          string
	Path            string
	Mapping         *Mapping
	MaxRequestBytes int64
	AuthToken       string
	TLSConfig       *tls.Config
}

// Receiver serves one gateway callback endpoint.
type Receiver struct {
	opts Options
	srv  *http.Server

	accepted atomic.Int64
	rejected atomic.Int64
	bytes    atomic.Int64
}

// New creates a receiver.
func New(opts Options) *Receiver {
	if opts.MaxRequestBytes <= 0 {
		opts.MaxRequestBytes = 16 << 20
	}
	if opts.Path == "" {
		opts.Path = "/v1/hooks/" + opts.Mapping.Name
	}
	return &Receiver{opts: opts}
}

func (r *Receiver) Name() string         { return r.opts.Name }
func (r *Receiver) BytesReceived() int64 { return r.bytes.Load() }

// Start serves until ctx is cancelled.
func (r *Receiver) Start(ctx context.Context, next pipeline.Next) error {
	mux := http.NewServeMux()
	mux.HandleFunc(r.opts.Path, r.handle(next))

	r.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	ln, err := net.Listen("tcp", r.opts.Listen)
	if err != nil {
		return fmt.Errorf("webhook source %q: listen %s: %w", r.opts.Name, r.opts.Listen, err)
	}
	if r.opts.TLSConfig != nil {
		r.srv.TLSConfig = r.opts.TLSConfig
		ln = tls.NewListener(ln, r.opts.TLSConfig)
	}

	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.srv.Shutdown(c)
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

func (r *Receiver) handle(next pipeline.Next) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.opts.AuthToken != "" && req.Header.Get("Authorization") != "Bearer "+r.opts.AuthToken {
			r.rejected.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if req.ContentLength > r.opts.MaxRequestBytes {
			r.rejected.Add(1)
			http.Error(w, "request body exceeds max_request_bytes", http.StatusRequestEntityTooLarge)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, r.opts.MaxRequestBytes))
		if err != nil {
			r.rejected.Add(1)
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		r.bytes.Add(int64(len(body)))

		envs, err := r.Map(body)
		if err != nil {
			r.rejected.Add(1)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, env := range envs {
			if err := next(req.Context(), env); err != nil {
				w.Header().Set("Retry-After", "1")
				code := http.StatusServiceUnavailable
				if pipeline.IsQuotaExceeded(err) {
					code = http.StatusTooManyRequests
				}
				http.Error(w, "collector cannot accept data", code)
				return
			}
		}

		r.accepted.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":%d}`+"\n", len(envs))
	}
}

// Map converts a callback body — one object or an array — into envelopes.
func (r *Receiver) Map(body []byte) ([]pipeline.Envelope, error) {
	doc, err := oj.Parse(body)
	if err != nil {
		// Generic on purpose: echoing the parser error can quote payload
		// bytes back into a gateway's error log (F-12.2).
		return nil, fmt.Errorf("callback body is not valid JSON")
	}

	var items []any
	if arr, ok := doc.([]any); ok {
		items = arr
	} else {
		items = []any{doc}
	}

	out := make([]pipeline.Envelope, 0, len(items))
	for i, item := range items {
		env, err := r.mapOne(item)
		if err != nil {
			return nil, fmt.Errorf("callback[%d]: %w", i, err)
		}
		// Tool steps first: they happened before the call that consumed
		// their results, and their timestamps say so.
		if r.opts.Mapping.ToolSteps == "openai_messages" {
			if msgs, ok := r.opts.Mapping.get(item, "input"); ok {
				out = append(out, toolStepsFromMessages(msgs, env)...)
			}
		}
		out = append(out, env)
	}
	return out, nil
}

func (r *Receiver) mapOne(doc any) (pipeline.Envelope, error) {
	m := r.opts.Mapping

	spanID := str(m.get(doc, "span_id"))
	if spanID == "" {
		return pipeline.Envelope{}, fmt.Errorf(
			"mapping %s found no span_id; a gateway retry would duplicate this call", m.Name)
	}

	// A gateway call with no session is still a trajectory of one step.
	// Grouping by its own id keeps it rather than dropping it (F-2.5's
	// spirit: evidence is kept).
	session := str(m.get(doc, "session_key"))
	if session == "" {
		session = spanID
	}

	step := record.Step{
		Kind:      m.FixedKind,
		Trainable: record.TrainableUnknown,
		StartedAt: timeUS(m.get(doc, "started_at")),
		Raw:       unmappedRoots(doc, m.roots),
	}
	if end := timeUS(m.get(doc, "ended_at")); end > step.StartedAt && step.StartedAt > 0 {
		ms := int32((end - step.StartedAt) / 1000)
		step.LatencyMs = &ms
	}

	setStr(&step.Model, str(m.get(doc, "model")))
	setStr(&step.Provider, str(m.get(doc, "provider")))
	setStr(&step.FinishReason, str(m.get(doc, "finish_reason")))

	var p record.Params
	hasParams := false
	if v, ok := num(m.get(doc, "temperature")); ok {
		p.Temperature, hasParams = &v, true
	}
	if v, ok := num(m.get(doc, "top_p")); ok {
		p.TopP, hasParams = &v, true
	}
	if v, ok := num(m.get(doc, "max_tokens")); ok {
		n := int32(v)
		p.MaxTokens, hasParams = &n, true
	}
	if v, ok := num(m.get(doc, "seed")); ok {
		n := int64(v)
		p.Seed, hasParams = &n, true
	}
	if hasParams {
		step.Params = &p
	}

	if v, ok := num(m.get(doc, "cost")); ok {
		step.CostUSD = &v
	}

	var tc record.TokenCounts
	hasTokens := false
	if v, ok := num(m.get(doc, "token_input")); ok {
		n := int64(v)
		tc.Input, hasTokens = &n, true
	}
	if v, ok := num(m.get(doc, "token_output")); ok {
		n := int64(v)
		tc.Output, hasTokens = &n, true
	}
	if hasTokens {
		step.TokenCounts = &tc
	}

	payload := map[string]any{}
	if v, ok := m.get(doc, "input"); ok {
		payload["input"] = v
	}
	if v, ok := m.get(doc, "output"); ok {
		payload["output"] = v
	}
	if len(payload) > 0 {
		b, err := json.Marshal(payload)
		if err == nil {
			s := string(b)
			step.ContentInline = &s
		}
	}

	// A failed call is a failed step and a failed episode. Without this a
	// gateway error looked exactly like a success, and "keep errors" tail
	// sampling would have had nothing to keep.
	var callErr *record.Error
	if msg := str(m.get(doc, "error")); msg != "" && msg != "None" {
		if len(msg) > 500 {
			msg = msg[:500]
		}
		e := record.Error{Type: "gateway_error", Message: msg}
		step.Error = &e
		callErr = &e
	}

	// A gateway cannot know when an agent run ends, so an episode would
	// otherwise close only at window expiry, marked timed_out. A caller that
	// does know can say so on its last call.
	terminal := false
	switch v := firstValue(m.get(doc, "episode_end")).(type) {
	case bool:
		terminal = v
	case string:
		terminal = v == "true" || v == "1"
	}

	return pipeline.Envelope{
		SessionKey: session,
		Source:     r.opts.Name,
		SpanID:     spanID,
		Terminal:   terminal,
		Error:      callErr,
		Step:       step,
		Meta: pipeline.EpisodeMeta{
			TaskType:               str(m.get(doc, "task_type")),
			GroupID:                str(m.get(doc, "group_id")),
			Instrumentation:        "gateway-" + m.Name,
			InstrumentationVersion: m.Version,
		},
	}, nil
}

func str(v any, ok bool) string {
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func num(v any, ok bool) (float64, bool) {
	if !ok || v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return t, true
	case int64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	}
	return 0, false
}

func setStr(dst **string, s string) {
	if s != "" {
		*dst = &s
	}
}

// timeUS normalises the timestamp spellings gateways actually send: epoch
// seconds, milliseconds or microseconds as a number, or an RFC 3339 string.
// The magnitude decides the unit, because a gateway will not say.
func timeUS(v any, ok bool) int64 {
	if !ok || v == nil {
		return 0
	}
	if s, isStr := v.(string); isStr {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UnixMicro()
			}
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			v = f
		} else {
			return 0
		}
	}
	f, ok := num(v, true)
	if !ok {
		return 0
	}
	switch {
	case f < 1e11: // seconds
		return int64(f * 1e6)
	case f < 1e14: // milliseconds
		return int64(f * 1e3)
	default: // microseconds
		return int64(f)
	}
}

// unmappedRoots keeps scalar top-level fields the mapping did not read, so
// nothing a gateway sends is silently discarded (§9.3). Redaction then decides
// what survives, exactly as for any other raw attribute.
func unmappedRoots(doc any, consumed map[string]bool) map[string]string {
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]string{}
	for k, v := range obj {
		if consumed[k] {
			continue
		}
		switch v.(type) {
		case map[string]any, []any:
			// Nested structure is not flattened into raw: raw is a
			// string map, and a stringified blob of arbitrary gateway
			// internals is more likely to carry something sensitive
			// than to be useful.
			continue
		}
		if s := str(v, true); s != "" && !strings.Contains(k, "key") && !strings.Contains(k, "token") {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Rejected reports cumulative refused requests — oversized, malformed or
// unauthorised — for cc_ingest_records_total{result="rejected"} (F-1.7).
func (r *Receiver) Rejected() int64 { return r.rejected.Load() }

func firstValue(v any, ok bool) any {
	if !ok {
		return nil
	}
	return v
}
