// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command cc runs and inspects the trajectory collector (F-14).
//
// Phase 1 implements run and validate. import, inspect, redact --test and
// replay are Phase 2.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/service"
	"github.com/trajectory-project/trajectory/collector/telemetry"
	"github.com/trajectory-project/trajectory/internal/version"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "run":
		os.Exit(cmdRun(os.Args[2:]))
	case "validate":
		os.Exit(cmdValidate(os.Args[2:]))
	case "import":
		os.Exit(cmdImport(os.Args[2:]))
	case "redact":
		os.Exit(cmdRedact(os.Args[2:]))
	case "inspect":
		os.Exit(cmdInspect(os.Args[2:]))
	case "replay":
		os.Exit(cmdReplay(os.Args[2:]))
	case "conform":
		os.Exit(cmdConform(os.Args[2:]))
	case "outcomes":
		os.Exit(cmdOutcomes(os.Args[2:]))
	case "join":
		os.Exit(cmdJoin(os.Args[2:]))
	case "score":
		os.Exit(cmdScore(os.Args[2:]))
	case "export":
		os.Exit(cmdExport(os.Args[2:]))
	case "version":
		fmt.Printf("cc %s (commit %s, schema %s)\n",
			version.Collector, version.Commit, version.Schema)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "cc: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `cc - the trajectory collector

Usage:
  cc run      -config <file>                 Run the collector
  cc validate -config <file>                 Check a config file, exit non-zero on error
  cc import   -config <file> -from <path> <format>
                                             Backfill a historical export
  cc redact   -config <file> --test <file>   Show what the redaction policy would do
  cc inspect  <file>                         Summarise a Parquet file, blob or manifest
  cc replay   -lake <dir> <episode-id>       Print a reconstructed episode
  cc conform  <lake-dir>                     Check a dataset against the format spec

Outcome join:
  cc outcomes -config <file> -from <csv|json>   Load business outcomes
  cc join     -lake <dir> [-as-of T] [-horizon D] -out <file>
                                             Label episodes with their outcomes
  cc score    -lake <dir> -scorer <file>     Turn outcomes into rewards
  cc export   -lake <dir> -format <f> -out <file>
                                             Write a training dataset
  cc version                                 Print version and schema version

%s`, formatHelp())
}

// cmdValidate implements F-14.2. It exits non-zero on error so it is usable as
// a deployment gate.
func cmdValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	path := fs.String("config", "", "path to the config file")
	sample := fs.String("sample", "", "dry-run: a native episode file to push through the pipeline (F-11.3)")
	_ = fs.Parse(args)

	if *path == "" {
		fmt.Fprintln(os.Stderr, "cc validate: -config is required")
		return 2
	}

	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Printf("%s is valid\n", *path)
	fmt.Printf("  tenant         %s\n", cfg.Tenant)
	fmt.Printf("  schema         %s\n", cfg.SchemaVersion)
	fmt.Printf("  sources        %d\n", len(cfg.Sources))
	for _, s := range cfg.Sources {
		fmt.Printf("                 %s (%s) on %s\n", s.Name, s.Type, s.HTTP.Listen)
	}
	fmt.Printf("  assembly       window %s, settle %s, max in-flight %d\n",
		cfg.Assembly.Window, cfg.Assembly.SettleAfterTerminal, cfg.Assembly.MaxInFlight)
	fmt.Printf("  redaction      default=%s on_error=%s, %d rule(s), %d allow path(s)\n",
		cfg.Redaction.Default, cfg.Redaction.OnError,
		len(cfg.Redaction.Rules), len(cfg.Redaction.Allow))
	fmt.Printf("  entities       %d tool(s)\n", len(cfg.Entities))
	for _, s := range cfg.Sinks {
		fmt.Printf("  sink           %s (%s) -> %s\n", s.Name, s.Type, s.Dir)
		fmt.Printf("                 partition by %v, blob threshold %d bytes\n",
			s.PartitionBy, s.BlobThresholdBytes)
	}

	if *sample != "" {
		return dryRun(cfg, *sample)
	}

	// Deny-by-default with an empty allow-list is valid but almost
	// certainly a mistake: it discards every payload. Warning rather than
	// failing, because metadata-only capture is a legitimate mode (F-5.6).
	if cfg.Redaction.Default == "deny" && len(cfg.Redaction.Allow) == 0 {
		fmt.Printf("\nnote: redaction.default is \"deny\" and redaction.allow is empty,\n" +
			"      so every payload field will be dropped. This is metadata-only\n" +
			"      capture. If that is not what you meant, add allow paths.\n")
	}
	return 0
}

// cmdRun implements F-14.1.
func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("config", "", "path to the config file")
	_ = fs.Parse(args)

	if *path == "" {
		fmt.Fprintln(os.Stderr, "cc run: -config is required")
		return 2
	}

	cfg, err := config.Load(*path)
	if err != nil {
		// Refuse to start on invalid config, naming file, key and
		// reason (F-11.6).
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	log := newLogger(cfg.Telemetry.LogLevel)

	svc, err := service.New(cfg, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc run: %v\n", err)
		return 1
	}

	// The collector's own traces (F-11.2). Off unless an endpoint is set,
	// so no outbound connection exists that an operator did not configure
	// (F-12.6).
	shutdownTracing, err := telemetry.SetupTracing(context.Background(), telemetry.TraceConfig{
		Endpoint:   cfg.Telemetry.Traces.Endpoint,
		Protocol:   cfg.Telemetry.Traces.Protocol,
		Insecure:   cfg.Telemetry.Traces.Insecure,
		SampleRate: cfg.Telemetry.Traces.SampleRate,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc run: %v\n", err)
		return 1
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()
	if cfg.Telemetry.Traces.Endpoint != "" {
		log.Info("exporting collector traces", "endpoint", cfg.Telemetry.Traces.Endpoint)
	}

	if cfg.Telemetry.Pprof {
		svc.Metrics().EnablePprof()
		log.Warn("pprof is enabled on the telemetry listener; " +
			"it exposes command-line arguments and memory contents")
	}

	if listen := cfg.Telemetry.Metrics.Listen; listen != "" {
		if _, err := svc.Metrics().Serve(listen); err != nil {
			fmt.Fprintf(os.Stderr, "cc run: telemetry listener: %v\n", err)
			return 1
		}
		log.Info("telemetry listening", "addr", listen)
	}

	// SIGTERM is what an orchestrator sends. Graceful shutdown stops
	// accepting, drains assembly and flushes within a deadline (F-11.5);
	// a second signal aborts, so an operator is never stuck waiting.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("shutdown signal received, draining")
	}()

	// SIGHUP reloads policy without dropping in-flight data (F-11.4). An
	// invalid file is refused and the running config stays in force.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			next, err := config.Load(*path)
			if err != nil {
				svc.Metrics().ConfigReloads.WithLabelValues("error").Inc()
				log.Error("reload rejected, previous config still in force", "error", err)
				continue
			}
			pending, err := svc.Reload(next)
			if err != nil {
				log.Error("reload failed", "error", err)
				continue
			}
			if len(pending) > 0 {
				log.Warn("some changes need a restart to take effect", "sections", pending)
			}
		}
	}()

	runErr := svc.Run(ctx)

	shutCtx, cancel := context.WithTimeout(context.Background(), svc.ShutdownTimeout())
	defer cancel()
	if err := svc.Shutdown(shutCtx); err != nil {
		log.Error("shutdown", "error", err)
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "cc run: %v\n", runErr)
		return 1
	}
	return 0
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
