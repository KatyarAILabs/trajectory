// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/service"
	"github.com/trajectory-project/trajectory/importers"
)

// cmdImport implements F-14.3 / UC-4: backfill a historical export into the
// same tables as live traffic.
//
// This is the first thing a pilot uses. It runs the whole pipeline — normalise,
// assemble, redact, extract, sink — with a file reader in place of a listener,
// so what a partner sees here is what the live path will produce.
func cmdImport(args []string) int {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to the config file")
	from := fs.String("from", "", "path to the export file, or - for stdin")
	sourceName := fs.String("source", "", "source name recorded on imported records (default: import:<format>)")
	dryRun := fs.Bool("dry-run", false, "read and normalise, but write nothing")
	_ = fs.Parse(args)

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintf(os.Stderr, "cc import: a format is required\n\n%s", formatHelp())
		return 2
	}
	format := rest[0]

	imp, err := importers.Get(format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc import: %v\n", err)
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "cc import: -config is required")
		return 2
	}
	if *from == "" {
		fmt.Fprintln(os.Stderr, "cc import: -from is required")
		return 2
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	in, closeIn, err := openInput(*from)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc import: %v\n", err)
		return 1
	}
	defer closeIn()

	name := *sourceName
	if name == "" {
		// Imported records stay distinguishable from live traffic
		// (F-1.6), which matters when a partner compares a backfill
		// against what their running system produces.
		name = "import:" + imp.Name()
	}

	log := newLogger(cfg.Telemetry.LogLevel)
	svc, err := service.New(cfg, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc import: %v\n", err)
		return 1
	}

	ctx := context.Background()
	stats, err := svc.RunImport(ctx, imp, in, name, *dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc import: %v\n", err)
		return 1
	}

	printImportResult(imp.Name(), stats, svc, *dryRun)
	return 0
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

func printImportResult(format string, st importers.Stats, svc *service.Service, dryRun bool) {
	fmt.Printf("imported %s\n", format)
	fmt.Printf("  records read      %d\n", st.Records)
	fmt.Printf("  spans emitted     %d\n", st.Spans)

	r := svc.ImportResult()
	fmt.Printf("  episodes built    %d\n", r.Episodes)
	if r.Quarantined > 0 {
		fmt.Printf("  quarantined       %d  (see the quarantine prefix under the sink)\n", r.Quarantined)
	}
	if r.Sampled > 0 {
		fmt.Printf("  dropped by sampling %d\n", r.Sampled)
	}

	if st.Skipped > 0 {
		fmt.Printf("\n  %d record(s) were skipped:\n", st.Skipped)
		for _, w := range st.Warnings {
			fmt.Printf("    %s\n", w)
		}
	}

	if dryRun {
		fmt.Printf("\ndry run: nothing was written\n")
		return
	}

	// Entity coverage is the leading indicator that a future join will
	// fail, and an import is the earliest possible moment to see it — months
	// before anyone tries the join (F-6.3, §19).
	fmt.Printf("\n  entity key coverage %.0f%% of episodes carry at least one key\n",
		(1-r.WithoutKeysRatio)*100)
	if r.WithoutKeysRatio > 0.5 && r.Episodes > 0 {
		fmt.Printf("\n  note: most episodes carry no entity keys, so they cannot later be\n" +
			"        joined to business outcomes. Check the `entities` config against\n" +
			"        the tool names actually present in this export.\n")
	}
}

func formatHelp() string {
	s := "Available formats:\n"
	for _, i := range importers.All() {
		s += fmt.Sprintf("  %-12s %s\n", i.Name(), i.Describe())
	}
	return s
}

var _ = slog.LevelInfo
