// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package importers

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/KatyarAILabs/trajectory/collector/normalize"
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
)

func init() { register(&Phoenix{}) }

// Phoenix imports an Arize Phoenix spans export.
//
// Phoenix spans already carry OpenInference attributes, so this importer does
// almost nothing itself: it unwraps the export envelope and hands the
// attributes to the same convention mapper the live OTLP path uses. An
// importer that reimplemented the mapping would drift from it, and the point of
// the import path is to show a partner what the live path will produce.
type Phoenix struct{}

func (*Phoenix) Name() string { return "phoenix" }
func (*Phoenix) Describe() string {
	return "Arize Phoenix spans export; reuses the OpenInference mapping"
}

type phoenixSpan struct {
	Context struct {
		TraceID string `json:"trace_id"`
		SpanID  string `json:"span_id"`
	} `json:"context"`
	SpanID        string         `json:"span_id"`
	TraceID       string         `json:"trace_id"`
	ParentID      string         `json:"parent_id"`
	Name          string         `json:"name"`
	StartTime     string         `json:"start_time"`
	EndTime       string         `json:"end_time"`
	StatusCode    string         `json:"status_code"`
	StatusMessage string         `json:"status_message"`
	Attributes    map[string]any `json:"attributes"`
}

func (*Phoenix) Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error) {
	var st Stats

	body, err := io.ReadAll(r)
	if err != nil {
		return st, fmt.Errorf("read: %w", err)
	}
	raws, err := decodeLoose(body, "spans", "data")
	if err != nil {
		return st, err
	}

	reg, err := normalize.LoadBuiltins()
	if err != nil {
		return st, err
	}

	for i, raw := range raws {
		var ps phoenixSpan
		if err := json.Unmarshal(raw, &ps); err != nil {
			st.Warn("record %d: %v", i, err)
			continue
		}

		traceID := firstNonEmpty(ps.TraceID, ps.Context.TraceID)
		spanID := firstNonEmpty(ps.SpanID, ps.Context.SpanID)
		if traceID == "" && spanID == "" {
			st.Warn("record %d: no trace_id or span_id", i)
			continue
		}

		start, ok := parseTime(ps.StartTime)
		if !ok {
			st.Warn("record %d: unparseable start_time %q", i, ps.StartTime)
			continue
		}
		end, _ := parseTime(ps.EndTime)

		s := normalize.Span{
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: ps.ParentID,
			Name:         ps.Name,
			StartedAtUS:  start,
			EndedAtUS:    end,
			// Phoenix nests attributes; the mapper wants them flat,
			// which is also how they arrive over OTLP.
			Attributes:   flattenAttrs("", ps.Attributes),
			ScopeName:    "phoenix-import",
			ScopeVersion: "unknown",
		}
		if strings.EqualFold(ps.StatusCode, "ERROR") {
			s.StatusError = true
			s.StatusMessage = ps.StatusMessage
		}

		env := normalize.Normalize(s, sourceName,
			[]string{"session.id", "gen_ai.conversation.id", "trace_id"}, reg)

		if err := emit(env); err != nil {
			return st, err
		}
		st.Spans++
	}

	st.Records = st.Spans
	return st, nil
}

// flattenAttrs turns Phoenix's nested attribute object into the dotted keys the
// OpenInference convention names, so `{"llm":{"model_name":"x"}}` becomes
// `llm.model_name=x`.
func flattenAttrs(prefix string, in map[string]any) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		switch t := v.(type) {
		case map[string]any:
			for nk, nv := range flattenAttrs(key, t) {
				out[nk] = nv
			}
		case string:
			out[key] = t
		case float64:
			// Phoenix writes integers as JSON numbers; rendering
			// 3200 as "3200" rather than "3200.000000" keeps token
			// counts parseable.
			if t == float64(int64(t)) {
				out[key] = strconv.FormatInt(int64(t), 10)
			} else {
				out[key] = strconv.FormatFloat(t, 'g', -1, 64)
			}
		case bool:
			out[key] = strconv.FormatBool(t)
		case nil:
			// Nothing to record.
		default:
			b, err := json.Marshal(t)
			if err == nil {
				out[key] = string(b)
			}
		}
	}
	return out
}
