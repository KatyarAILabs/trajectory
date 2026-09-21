// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package sample implements head and tail sampling (F-7).
//
// One invariant governs the whole package: **sampling never splits an
// episode.** The unit of sampling is the assembled episode, never the span. A
// half-sampled trajectory is worse than no trajectory — it looks complete to a
// reader and trains a model on a truncated history.
//
// Head sampling therefore keys on the session, not the span, so every span of
// a session shares one decision. Tail sampling runs after assembly, where the
// unit is already whole.
package sample

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
)

// Reasons recorded in Episode.sampled_by (F-7.4), so a consumer can reason
// about the bias in what it is reading.
const (
	ReasonHeadKept = "head:kept"
	ReasonTailRule = "tail:"
	ReasonTailRate = "tail:rate"
)

// Sampler makes head and tail decisions.
type Sampler struct {
	headRate float64

	keepIf    []compiledRule
	otherwise float64

	mu    sync.Mutex
	stats Stats
}

type compiledRule struct {
	src     string
	program cel.Program
}

// Stats are the counters this stage contributes to §11.
type Stats struct {
	HeadKept    int64
	HeadDropped int64
	TailKept    map[string]int64
	TailDropped int64
	// RuleErrors counts tail expressions that failed to evaluate. These
	// keep the episode: a broken sampling rule must not silently delete
	// data, which is the opposite trade-off from a broken redaction rule.
	RuleErrors int64
}

// New compiles a sampling policy.
func New(cfg config.Sampling) (*Sampler, error) {
	s := &Sampler{
		headRate:  1.0,
		otherwise: 1.0,
		stats:     Stats{TailKept: map[string]int64{}},
	}
	if cfg.Head.Rate != nil {
		s.headRate = *cfg.Head.Rate
	}
	if cfg.Tail.OtherwiseRate != nil {
		s.otherwise = *cfg.Tail.OtherwiseRate
	}

	env, err := celEnv()
	if err != nil {
		return nil, err
	}

	for _, src := range cfg.Tail.KeepIf {
		ast, iss := env.Compile(src)
		if iss.Err() != nil {
			return nil, fmt.Errorf("sampling.tail.keep_if %q: %w", src, iss.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf(
				"sampling.tail.keep_if %q evaluates to %s, not bool", src, ast.OutputType())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("sampling.tail.keep_if %q: %w", src, err)
		}
		s.keepIf = append(s.keepIf, compiledRule{src: src, program: prg})
	}

	return s, nil
}

// celEnv declares the variables a tail expression may reference.
//
// The surface is deliberately narrow: episode-level fields only. Exposing step
// payloads here would make a sampling rule a place to accidentally read
// unredacted content, and tail rules run before nothing — they run after
// redaction, but a narrow surface keeps that from mattering.
func celEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("status", cel.StringType),
		cel.Variable("has_error", cel.BoolType),
		cel.Variable("step_count", cel.IntType),
		cel.Variable("task_type", cel.StringType),
		cel.Variable("source", cel.StringType),
		cel.Variable("tenant", cel.StringType),
		cel.Variable("group_id", cel.StringType),
		cel.Variable("entity_key_count", cel.IntType),
		cel.Variable("raw", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("duration_ms", cel.IntType),
	)
}

// HeadKeep decides whether to admit a session, before assembly (F-7.1).
//
// The decision is a deterministic hash of the session key rather than a random
// draw, so every span of a session gets the same answer no matter which
// replica or goroutine handles it. A random draw per span would admit some
// spans and reject others, producing exactly the split episodes this package
// exists to prevent.
func (s *Sampler) HeadKeep(sessionKey string) bool {
	if s.headRate >= 1.0 {
		s.mu.Lock()
		s.stats.HeadKept++
		s.mu.Unlock()
		return true
	}
	if s.headRate <= 0 {
		s.mu.Lock()
		s.stats.HeadDropped++
		s.mu.Unlock()
		return false
	}

	keep := hashUnit(sessionKey) < s.headRate

	s.mu.Lock()
	if keep {
		s.stats.HeadKept++
	} else {
		s.stats.HeadDropped++
	}
	s.mu.Unlock()
	return keep
}

// hashUnit maps a key deterministically into [0,1).
//
// FNV-1a alone is not good enough here. Its avalanche in the high bits is weak
// for the near-identical keys real session ids tend to be, and taking the top
// bits of a raw FNV sum produced a measured 6.8% keep rate for a configured 10%
// — an operator would silently collect two thirds of the data they asked for.
// The splitmix64 finalizer fixes the distribution for a couple of nanoseconds.
func hashUnit(key string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	v := mix64(h.Sum64())

	// The top 53 bits are the exactly-representable range of a float64
	// mantissa, so the distribution has no gaps.
	return float64(v>>11) / float64(uint64(1)<<53)
}

// mix64 is the splitmix64 finalizer: a bijection with good avalanche, so
// similar inputs land far apart.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// TailKeep decides whether to keep an assembled episode (F-7.2), and records
// why on the episode (F-7.4).
//
// Errors and human-edited episodes are always kept, because they are the ones a
// verifier or a reviewer will actually want and they are rare enough that
// keeping all of them costs little.
func (s *Sampler) TailKeep(ep *pipeline.Assembled) bool {
	vars := episodeVars(ep)

	for _, rule := range s.keepIf {
		out, _, err := rule.program.Eval(vars)
		if err != nil {
			// A broken sampling rule keeps the episode. The failure
			// mode of a sampling bug must be too much data, never
			// silent deletion.
			s.mu.Lock()
			s.stats.RuleErrors++
			s.mu.Unlock()
			s.mark(ep, "tail:rule_error")
			return true
		}
		if out == types.True {
			s.mu.Lock()
			s.stats.TailKept[rule.src]++
			s.mu.Unlock()
			s.mark(ep, ReasonTailRule+rule.src)
			return true
		}
	}

	if s.otherwise >= 1.0 {
		s.mu.Lock()
		s.stats.TailKept[ReasonTailRate]++
		s.mu.Unlock()
		s.mark(ep, ReasonTailRate)
		return true
	}
	if s.otherwise <= 0 {
		s.mu.Lock()
		s.stats.TailDropped++
		s.mu.Unlock()
		return false
	}

	// Keyed on the episode id so the decision is reproducible: replaying
	// the same corpus yields the same sample.
	if hashUnit(ep.Episode.EpisodeID) < s.otherwise {
		s.mu.Lock()
		s.stats.TailKept[ReasonTailRate]++
		s.mu.Unlock()
		s.mark(ep, ReasonTailRate)
		return true
	}

	s.mu.Lock()
	s.stats.TailDropped++
	s.mu.Unlock()
	return false
}

func (s *Sampler) mark(ep *pipeline.Assembled, reason string) {
	r := reason
	ep.Episode.SampledBy = &r
}

func episodeVars(ep *pipeline.Assembled) map[string]any {
	e := ep.Episode

	var duration int64
	if e.EndedAt != nil && *e.EndedAt > e.StartedAt {
		duration = (*e.EndedAt - e.StartedAt) / 1000
	}

	raw := e.Raw
	if raw == nil {
		raw = map[string]string{}
	}

	return map[string]any{
		"status":           e.Status,
		"has_error":        e.Error != nil,
		"step_count":       int64(e.StepCount),
		"task_type":        deref(e.TaskType),
		"source":           e.Source,
		"tenant":           e.Tenant,
		"group_id":         deref(e.GroupID),
		"entity_key_count": int64(len(e.EntityKeys)),
		"raw":              lenientMap{types.NewStringStringMap(types.DefaultTypeAdapter, raw)},
		"duration_ms":      duration,
	}
}

// lenientMap makes an absent key evaluate to the empty string instead of an
// error.
//
// This exists because the spec's own example config (§10) writes
// raw['human_edited'] == 'true', and most episodes do not carry that key. With
// CEL's default map semantics that expression errors on every ordinary
// episode, and since a rule error keeps the episode, an operator's tail policy
// would silently degrade into "keep everything" — the exact opposite of what
// they configured, and invisible except as an unexpectedly large bill.
//
// A missing key means the condition is not met. It is not a broken policy.
type lenientMap struct {
	traits.Mapper
}

func (m lenientMap) Find(key ref.Val) (ref.Val, bool) {
	if v, found := m.Mapper.Find(key); found {
		return v, true
	}
	return types.String(""), true
}

func (m lenientMap) Get(key ref.Val) ref.Val {
	v, _ := m.Find(key)
	return v
}

// Contains is left honest, so `'k' in raw` still distinguishes present from
// absent. Use that for a presence test; indexing is the lenient one.
func (m lenientMap) Contains(key ref.Val) ref.Val {
	return m.Mapper.Contains(key)
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// Name implements pipeline.Processor for the tail stage.
func (s *Sampler) Name() string { return "sample" }

// Process implements pipeline.Processor. It never returns an error: a sampling
// decision is not a failure, and dropping is signalled by the caller checking
// TailKeep instead.
func (s *Sampler) Process(_ context.Context, ep *pipeline.Assembled) error {
	_ = ep
	return nil
}

// Stats returns a snapshot of the stage counters.
func (s *Sampler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Stats{
		HeadKept:    s.stats.HeadKept,
		HeadDropped: s.stats.HeadDropped,
		TailDropped: s.stats.TailDropped,
		RuleErrors:  s.stats.RuleErrors,
		TailKept:    make(map[string]int64, len(s.stats.TailKept)),
	}
	for k, v := range s.stats.TailKept {
		out.TailKept[k] = v
	}
	return out
}
