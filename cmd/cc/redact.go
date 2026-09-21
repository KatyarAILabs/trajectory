// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/redact"
	"github.com/trajectory-project/trajectory/collector/wire"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// cmdRedact implements `cc redact --test` (F-14.5, MUST).
//
// A security reviewer has to be able to see what a policy does before it runs
// in production, and an operator has to be able to check a change without
// deploying it. The output names rule ids, field paths and counts — never a
// payload value, for the same reason the manifest does not (F-5.4).
func cmdRedact(args []string) int {
	fs := flag.NewFlagSet("redact", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to the config file")
	test := fs.Bool("test", false, "evaluate policies against a sample file and print what would change")
	showValues := fs.Bool("show-redacted-values", false,
		"also print the redacted payloads (they contain no secrets by definition, but review the output before sharing it)")
	_ = fs.Parse(args)

	if !*test {
		fmt.Fprintln(os.Stderr, "cc redact: --test is required (it is the only mode)")
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "cc redact: -config is required")
		return 2
	}

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "cc redact --test: a sample file is required")
		return 2
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	var key []byte
	if env := cfg.Redaction.Tokenization.KeyEnv; env != "" {
		key = []byte(os.Getenv(env))
	}

	r, err := redact.New(cfg.Redaction, key, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc redact: %v\n", err)
		return 1
	}

	body, err := os.ReadFile(rest[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc redact: %v\n", err)
		return 1
	}

	eps, err := wire.DecodeEpisodes(body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc redact: %s is not a native episode file: %v\n", rest[0], err)
		fmt.Fprintln(os.Stderr, "  expected one JSON episode, or an array of them")
		return 1
	}

	fmt.Printf("policy from %s\n", *cfgPath)
	fmt.Printf("  default          %s\n", cfg.Redaction.Default)
	fmt.Printf("  on_error         %s\n", cfg.Redaction.OnError)
	if cfg.Redaction.MetadataOnly {
		fmt.Printf("  metadata_only    true  (every payload is discarded)\n")
	}
	fmt.Printf("  allow paths      %d\n", len(cfg.Redaction.Allow))
	fmt.Printf("  deny paths       %d\n", len(cfg.Redaction.Deny))
	fmt.Printf("  rules            %d\n", len(cfg.Redaction.Rules))
	if key == nil && usesTokenize(cfg.Redaction) {
		fmt.Printf("\n  WARNING: a rule uses action tokenize but %s is unset.\n",
			cfg.Redaction.Tokenization.KeyEnv)
		fmt.Printf("           In production this fails closed and quarantines the record.\n")
	}
	fmt.Println()

	totals := map[string]int{}
	quarantined := 0

	for i, we := range eps {
		ep, err := toAssembled(we)
		if err != nil {
			fmt.Printf("episode %d: cannot read: %v\n", i, err)
			continue
		}

		entries, err := r.Preview(ep)
		if err != nil {
			// This is what would happen in production: the record
			// never reaches a sink (F-5.5).
			quarantined++
			fmt.Printf("episode %s: WOULD BE QUARANTINED\n  %v\n\n", ep.Episode.EpisodeID, err)
			continue
		}

		// logcheck:allow — prints a step count, never step content.
		fmt.Printf("episode %s (%d steps)\n", ep.Episode.EpisodeID, len(ep.Steps))
		if len(entries) == 0 {
			fmt.Printf("  no changes\n\n")
			continue
		}
		for _, e := range entries {
			fmt.Printf("  %-28s %-22s %s x%d\n", e.Path, e.RuleID, e.Action, e.Matches)
			totals[e.RuleID] += e.Matches
		}

		if *showValues {
			// Applied to a copy, so the input file is never touched.
			applied := ep
			if err := r.Process(context.Background(), applied); err == nil {
				fmt.Printf("\n  after redaction:\n")
				for j, s := range applied.Steps {
					if s.ContentInline != nil {
						// logcheck:allow — post-redaction content, printed to
						// the operator's terminal only behind an explicit
						// opt-in flag. This is the whole purpose of
						// --show-redacted-values: proving to a reviewer that
						// the policy did what they expect. It is stdout, not
						// the collector's logs, and the values have already
						// been through the policy being tested.
						fmt.Printf("    step %d: %s\n", j, truncate(*s.ContentInline, 160))
					}
				}
			}
		}
		fmt.Println()
	}

	fmt.Printf("totals across %d episode(s)\n", len(eps))
	for _, id := range sortedCountKeys(totals) {
		fmt.Printf("  %-24s %d\n", id, totals[id])
	}
	if quarantined > 0 {
		fmt.Printf("\n%d episode(s) would be quarantined and never written.\n", quarantined)
		return 1
	}
	return 0
}

func usesTokenize(c config.Redaction) bool {
	for _, r := range c.Rules {
		if r.Action == redact.ActionTokenize {
			return true
		}
	}
	return false
}

// toAssembled converts a wire episode into the shape a processor operates on,
// without going through the assembler: `cc redact --test` is about the policy,
// not about grouping.
func toAssembled(we wire.Episode) (*pipeline.Assembled, error) {
	envs, err := we.ToEnvelopes("redact-test", "")
	if err != nil {
		return nil, err
	}

	ep := &pipeline.Assembled{
		Episode: record.Episode{
			EpisodeID: we.EpisodeID,
			Tenant:    we.Tenant,
			Raw:       we.Raw,
		},
	}
	for _, env := range envs {
		ep.Steps = append(ep.Steps, env.Step)
	}
	return ep, nil
}

func sortedCountKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("... (%d bytes total)", len(s))
}
