// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/collector/service"
	"github.com/KatyarAILabs/trajectory/collector/wire"
)

// cmdOutcomes loads business outcomes from a file (§9.4).
//
// The same path as POST /v1/outcomes: keys go through the redaction rules so
// they match episode keys, and rows go through the buffer to the sink.
func cmdOutcomes(args []string) int {
	fs := flag.NewFlagSet("outcomes", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to the config file")
	from := fs.String("from", "", "a CSV with a header row, or JSON/JSONL outcomes")
	source := fs.String("source", "outcomes-file", "source name recorded on the rows")
	_ = fs.Parse(args)

	if *cfgPath == "" || *from == "" {
		fmt.Fprintln(os.Stderr, "cc outcomes: -config and -from are required")
		return 2
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	outs, err := readOutcomeFile(*from)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc outcomes: %v\n", err)
		return 1
	}

	svc, err := service.New(cfg, newLogger(cfg.Telemetry.LogLevel))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc outcomes: %v\n", err)
		return 1
	}
	defer svc.Shutdown(context.Background())

	n, err := svc.LoadOutcomes(context.Background(), *source, outs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc outcomes: %v\n", err)
		return 1
	}

	kinds := map[string]int{}
	for _, o := range outs {
		kinds[o.EntityName+"/"+o.Kind]++
	}
	fmt.Printf("loaded %d outcomes from %s\n", n, *from)
	for _, k := range sortedCountKeys(kinds) {
		fmt.Printf("  %-36s %d\n", k, kinds[k])
	}
	return 0
}

func readOutcomeFile(path string) ([]wire.Outcome, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if strings.HasSuffix(strings.ToLower(path), ".csv") {
		return wire.ReadOutcomesCSV(f)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	t := strings.TrimSpace(string(b))
	if strings.HasPrefix(t, "[") || !strings.Contains(t, "\n") {
		return wire.DecodeOutcomes(b)
	}
	var out []wire.Outcome
	for i, line := range strings.Split(t, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		one, err := wire.DecodeOutcomes([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		out = append(out, one...)
	}
	return out, nil
}
