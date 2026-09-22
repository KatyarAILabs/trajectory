// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package extract pulls business keys out of tool arguments and results (F-6).
//
// What it deliberately does not do is as important as what it does: it does not
// resolve, validate or join the keys it finds (F-6.2). The collector has no
// opinion about whether a ticket id corresponds to a real ticket. It records
// what the trajectory touched and stops there.
//
// It runs after redaction, over tokenized values (F-6.4). That is what lets a
// key stay joinable without being readable: two episodes that touched the same
// order carry the same token, and neither carries the order number.
package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
)

// Extractor applies per-tool key expressions to an episode's steps.
type Extractor struct {
	// byTool maps a tool name to its compiled key expressions.
	byTool map[string][]keyExpr

	mu    sync.Mutex
	stats Stats
}

type keyExpr struct {
	name string
	expr jp.Expr
	// src is the original expression text, for error messages. Never the
	// extracted value.
	src string
}

// Stats are the counters this stage contributes to §11.
//
// EpisodesWithoutKeys is the leading indicator that a future join will fail.
// It is reported from day one because the alternative is discovering months
// later that an entire corpus is unjoinable (§19).
type Stats struct {
	Episodes            int64
	EpisodesWithoutKeys int64
	KeysExtracted       int64
}

// New compiles the entity configuration.
func New(entities []config.Entity) (*Extractor, error) {
	e := &Extractor{byTool: map[string][]keyExpr{}}

	for _, ent := range entities {
		// Sorted so the resulting entity_keys order is deterministic
		// rather than map-iteration order; episodes must be
		// byte-identical across runs.
		names := make([]string, 0, len(ent.Keys))
		for name := range ent.Keys {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			src := ent.Keys[name]
			expr, err := jp.ParseString(src)
			if err != nil {
				return nil, fmt.Errorf("entity %q key %q: cannot parse JSONPath %q: %w",
					ent.Tool, name, src, err)
			}
			e.byTool[ent.Tool] = append(e.byTool[ent.Tool], keyExpr{name: name, expr: expr, src: src})
		}
	}
	return e, nil
}

// Name implements pipeline.Processor.
func (e *Extractor) Name() string { return "extract" }

// Process extracts entity keys onto the episode.
//
// A step whose payload does not parse, or whose expression matches nothing, is
// not an error: producers vary, and a missing key is a coverage problem to be
// surfaced by a metric, not a reason to quarantine a trajectory that is
// otherwise complete.
func (e *Extractor) Process(_ context.Context, ep *pipeline.Assembled) error {
	var keys []record.EntityKey

	for _, s := range ep.Steps {
		if s.ToolName == nil || s.ContentInline == nil {
			continue
		}
		exprs, ok := e.byTool[*s.ToolName]
		if !ok {
			continue
		}

		doc, err := oj.ParseString(*s.ContentInline)
		if err != nil {
			continue
		}

		for _, ke := range exprs {
			for _, got := range ke.expr.Get(doc) {
				v := stringify(got)
				if v == "" {
					continue
				}
				keys = append(keys, record.EntityKey{Name: ke.name, Value: v})
			}
		}
	}

	keys = dedupe(keys)
	ep.Episode.EntityKeys = keys

	e.mu.Lock()
	e.stats.Episodes++
	e.stats.KeysExtracted += int64(len(keys))
	if len(keys) == 0 {
		e.stats.EpisodesWithoutKeys++
	}
	e.mu.Unlock()

	return nil
}

// stringify renders an extracted value. Numbers are rendered through JSON so
// an id that arrived as 12345 and one that arrived as "12345" produce the same
// key — otherwise the same entity would fail to join with itself.
func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		s := string(b)
		// Trim the quotes JSON adds around scalars so the stored value
		// is the id itself.
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
		return s
	}
}

// dedupe removes repeated keys and sorts, so the same trajectory always yields
// the same entity_keys regardless of step order.
func dedupe(keys []record.EntityKey) []record.EntityKey {
	if len(keys) == 0 {
		// An empty slice rather than nil: the column is a list and a
		// reader should see [] rather than null for "we looked and
		// found none".
		return []record.EntityKey{}
	}

	seen := map[record.EntityKey]bool{}
	out := make([]record.EntityKey, 0, len(keys))
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Value < out[j].Value
	})
	return out
}

// Stats returns a snapshot of the stage counters.
func (e *Extractor) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// CoverageRatio is cc_episodes_without_entity_keys_ratio (§11).
func (e *Extractor) CoverageRatio() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stats.Episodes == 0 {
		return 0
	}
	return float64(e.stats.EpisodesWithoutKeys) / float64(e.stats.Episodes)
}
