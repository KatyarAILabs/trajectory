// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package rules is a declarative Scorer: rewards from outcomes, written as
// CEL rules in a YAML file rather than as code.
//
// Most outcome-based rewards are a short ordered list — "reopened scores 0,
// refund completed scores 1, otherwise nothing" — and a team should be able to
// read, review and version that list without reading Go. The first rule that
// matches decides the reward; its name is recorded as the clause, so every
// reward in the lake can be traced to the rule that produced it.
package rules

import (
	"context"
	"fmt"
	"os"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"gopkg.in/yaml.v3"

	"github.com/trajectory-project/trajectory/pkg/record"
	"github.com/trajectory-project/trajectory/pkg/scorer"
)

// Config is the YAML shape of a rules scorer.
type Config struct {
	VerifierID string `yaml:"verifier_id"`
	Version    string `yaml:"version"`
	// RequireFinal scores only episodes whose observation horizon has
	// closed. On by default: scoring a provisional label rewards whatever
	// has not been complained about yet.
	RequireFinal *bool  `yaml:"require_final"`
	Rules        []Rule `yaml:"rules"`
	// DefaultReward applies when no rule matches. Unset means the scorer
	// does not apply, which is different from a reward of zero.
	DefaultReward *float64 `yaml:"default_reward"`
}

// Rule is one clause.
type Rule struct {
	Clause string  `yaml:"clause"`
	When   string  `yaml:"when"`
	Reward float64 `yaml:"reward"`
}

// Scorer implements scorer.Scorer.
type Scorer struct {
	cfg      Config
	programs []cel.Program
}

var _ scorer.Scorer = (*Scorer)(nil)

// Load reads and compiles a rules file.
func Load(path string) (*Scorer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b, path)
}

// Parse compiles rules YAML. Every expression is compiled up front and must be
// boolean, so a typo fails here rather than scoring a whole lake wrongly.
func Parse(b []byte, name string) (*Scorer, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("scorer %s: %w", name, err)
	}
	if cfg.VerifierID == "" || cfg.Version == "" {
		return nil, fmt.Errorf("scorer %s: verifier_id and version are required; "+
			"they are recorded on every reward so two versions never overwrite each other", name)
	}
	if len(cfg.Rules) == 0 && cfg.DefaultReward == nil {
		return nil, fmt.Errorf("scorer %s: no rules and no default_reward; it would score nothing", name)
	}
	if cfg.RequireFinal == nil {
		t := true
		cfg.RequireFinal = &t
	}

	env, err := cel.NewEnv(
		cel.Variable("latest", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("outcome_count", cel.IntType),
		cel.Variable("label_status", cel.StringType),
		cel.Variable("episode_status", cel.StringType),
		cel.Variable("has_error", cel.BoolType),
		cel.Variable("step_count", cel.IntType),
		cel.Variable("task_type", cel.StringType),
	)
	if err != nil {
		return nil, err
	}

	s := &Scorer{cfg: cfg}
	seen := map[string]bool{}
	for i, r := range cfg.Rules {
		if r.Clause == "" {
			return nil, fmt.Errorf("scorer %s: rules[%d]: clause is required; it names the rule on every reward", name, i)
		}
		if seen[r.Clause] {
			return nil, fmt.Errorf("scorer %s: clause %q is used twice", name, r.Clause)
		}
		seen[r.Clause] = true

		ast, iss := env.Compile(r.When)
		if iss.Err() != nil {
			return nil, fmt.Errorf("scorer %s: rule %q: %w", name, r.Clause, iss.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf("scorer %s: rule %q evaluates to %s, not bool", name, r.Clause, ast.OutputType())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, err
		}
		s.programs = append(s.programs, prg)
	}
	return s, nil
}

func (s *Scorer) ID() string         { return s.cfg.VerifierID }
func (s *Scorer) Version() string    { return s.cfg.Version }
func (s *Scorer) RequireFinal() bool { return *s.cfg.RequireFinal }

// Score applies the first matching rule.
func (s *Scorer) Score(_ context.Context, ep scorer.Episode) (record.Reward, bool, error) {
	if s.RequireFinal() && ep.LabelStatus != "final" {
		return record.Reward{}, false, nil
	}

	latest := ep.Latest
	if latest == nil {
		latest = map[string]string{}
	}
	taskType := ""
	if ep.Episode.TaskType != nil {
		taskType = *ep.Episode.TaskType
	}
	vars := map[string]any{
		"latest":         lenientMap{types.NewStringStringMap(types.DefaultTypeAdapter, latest)},
		"outcome_count":  int64(ep.OutcomeCount),
		"label_status":   ep.LabelStatus,
		"episode_status": ep.Episode.Status,
		"has_error":      ep.Episode.Error != nil,
		"step_count":     int64(ep.Episode.StepCount),
		"task_type":      taskType,
	}

	r := record.Reward{
		EpisodeID:       ep.Episode.EpisodeID,
		VerifierID:      s.cfg.VerifierID,
		VerifierVersion: s.cfg.Version,
	}

	for i, prg := range s.programs {
		out, _, err := prg.Eval(vars)
		if err != nil {
			// Unlike sampling, a scoring error is not "keep and move
			// on": a wrong reward trains the wrong behaviour. The
			// episode is not scored, and the caller counts it.
			return record.Reward{}, false, fmt.Errorf("rule %q: %w", s.cfg.Rules[i].Clause, err)
		}
		if out == types.True {
			rule := s.cfg.Rules[i]
			r.Reward = rule.Reward
			r.Clauses = map[string]float64{rule.Clause: rule.Reward}
			return r, true, nil
		}
	}

	if s.cfg.DefaultReward != nil {
		r.Reward = *s.cfg.DefaultReward
		r.Clauses = map[string]float64{"default": *s.cfg.DefaultReward}
		return r, true, nil
	}
	return record.Reward{}, false, nil
}

// lenientMap reads an absent kind as the empty string, so `latest['x'] == 'y'`
// is false for an episode with no x outcome rather than an evaluation error —
// the same reasoning as sampling's raw map (docs/PLAN.md B.2).
type lenientMap struct{ traits.Mapper }

func (m lenientMap) Find(key ref.Val) (ref.Val, bool) {
	if v, ok := m.Mapper.Find(key); ok {
		return v, true
	}
	return types.String(""), true
}

func (m lenientMap) Get(key ref.Val) ref.Val {
	v, _ := m.Find(key)
	return v
}

func (m lenientMap) Contains(key ref.Val) ref.Val { return m.Mapper.Contains(key) }
