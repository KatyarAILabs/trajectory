// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/KatyarAILabs/trajectory/collector/sink/lake"
	"github.com/KatyarAILabs/trajectory/collector/sink/objstore"
	"github.com/KatyarAILabs/trajectory/collector/wire"
	"github.com/KatyarAILabs/trajectory/pkg/export"
	"github.com/KatyarAILabs/trajectory/pkg/join"
	"github.com/KatyarAILabs/trajectory/pkg/lakeread"
	"github.com/KatyarAILabs/trajectory/pkg/record"
	"github.com/KatyarAILabs/trajectory/pkg/scorer"
	"github.com/KatyarAILabs/trajectory/pkg/scorer/rules"
)

// joinFlags are shared by join, score and export, so all three answer the same
// question about the same point in time.
type joinFlags struct {
	lake, asOf, horizon, attribution *string
}

func addJoinFlags(fs *flag.FlagSet) joinFlags {
	return joinFlags{
		lake:        fs.String("lake", "", "path to the lake directory"),
		asOf:        fs.String("as-of", "", "evaluate as of this time (RFC 3339); default now. Fix it to rebuild a dataset exactly"),
		horizon:     fs.String("horizon", "30d", "how long after an episode an outcome may be attributed to it"),
		attribution: fs.String("attribution", "last_touch", "last_touch or all"),
	}
}

func (f joinFlags) load() (*lakeread.Lake, join.Options, error) {
	if *f.lake == "" {
		return nil, join.Options{}, fmt.Errorf("-lake is required")
	}
	var o join.Options
	if *f.asOf != "" {
		us, err := wire.ParseTimestamp(*f.asOf)
		if err != nil {
			return nil, o, fmt.Errorf("-as-of: %w", err)
		}
		o.AsOf = time.UnixMicro(us).UTC()
	} else {
		o.AsOf = time.Now().UTC()
	}
	h, err := parseDays(*f.horizon)
	if err != nil {
		return nil, o, fmt.Errorf("-horizon: %w", err)
	}
	o.Horizon = h
	switch *f.attribution {
	case "last_touch", "all":
		o.Attribution = *f.attribution
	default:
		return nil, o, fmt.Errorf("-attribution must be last_touch or all")
	}

	l, err := lakeread.Load(*f.lake)
	if err != nil {
		return nil, o, err
	}
	return l, o, nil
}

// parseDays accepts Go durations plus a "d" suffix, because horizons are
// thought about in days and time.ParseDuration has no day unit.
func parseDays(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		var n float64
		if _, err := fmt.Sscanf(strings.TrimSuffix(s, "d"), "%g", &n); err != nil {
			return 0, fmt.Errorf("invalid %q", s)
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(s)
}

func printJoinStats(o join.Options, st join.Stats, unmanifested int) {
	fmt.Printf("as of %s, horizon %s\n", o.AsOf.Format(time.RFC3339), o.Horizon)
	fmt.Printf("  episodes          %d\n", st.Episodes)
	fmt.Printf("    final           %d\n", st.Final)
	fmt.Printf("    provisional     %d  (horizon still open; more outcomes may arrive)\n", st.Provisional)
	fmt.Printf("    unjoinable      %d  (no entity keys)\n", st.Unjoinable)
	fmt.Printf("    with outcomes   %d\n", st.WithOutcomes)
	fmt.Printf("  outcomes          %d attributed, %d unattributed", st.Attributed, st.Unattributed)
	if st.FutureObserved > 0 {
		fmt.Printf(", %d observed after as-of (excluded)", st.FutureObserved)
	}
	if st.Duplicates > 0 {
		fmt.Printf(", %d duplicates", st.Duplicates)
	}
	fmt.Println()
	if st.Outcomes > 0 && st.Attributed == 0 {
		fmt.Println("\n  note: no outcome matched any episode. Check that entity_name matches the\n" +
			"        `entities` key names, and that the collector that loaded the outcomes uses\n" +
			"        the same redaction rules and HMAC key as the one that captured the episodes.")
	}
	if unmanifested > 0 {
		fmt.Printf("\n  note: %d unmanifested file(s) were skipped — an interrupted write, not data loss\n", unmanifested)
	}
}

// cmdJoin writes episodes labelled with their outcomes.
func cmdJoin(args []string) int {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	jf := addJoinFlags(fs)
	out := fs.String("out", "", "output file: .jsonl or .parquet")
	_ = fs.Parse(args)

	l, o, err := jf.load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc join: %v\n", err)
		return 2
	}
	labels, st := join.Run(l, o)
	printJoinStats(o, st, l.Unmanifested)

	if *out == "" {
		return 0
	}
	if err := writeLabels(*out, labels); err != nil {
		fmt.Fprintf(os.Stderr, "cc join: %v\n", err)
		return 1
	}
	fmt.Printf("\nwrote %d labelled episodes to %s\n", len(labels), *out)
	return 0
}

// labelRow is the Parquet shape of a label, for readers that want a table.
type labelRow struct {
	EpisodeID       string            `parquet:"episode_id"`
	TaskType        string            `parquet:"task_type"`
	GroupID         string            `parquet:"group_id"`
	EpisodeStatus   string            `parquet:"episode_status"`
	LabelStatus     string            `parquet:"label_status"`
	StartedAt       int64             `parquet:"started_at,timestamp(microsecond)"`
	HorizonClosesAt int64             `parquet:"horizon_closes_at,timestamp(microsecond)"`
	AsOf            int64             `parquet:"as_of,timestamp(microsecond)"`
	OutcomeCount    int32             `parquet:"outcome_count"`
	Latest          map[string]string `parquet:"latest"`
	Outcomes        string            `parquet:"outcomes_json"`
}

func writeLabels(path string, labels []join.Label) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if strings.HasSuffix(path, ".parquet") {
		rows := make([]labelRow, 0, len(labels))
		for _, lb := range labels {
			oj, _ := json.Marshal(lb.Outcomes)
			rows = append(rows, labelRow{
				EpisodeID: lb.EpisodeID, TaskType: lb.TaskType, GroupID: lb.GroupID,
				EpisodeStatus: lb.Status, LabelStatus: lb.LabelStatus,
				StartedAt: lb.StartedAt, HorizonClosesAt: lb.HorizonClosesAt, AsOf: lb.AsOf,
				OutcomeCount: int32(len(lb.Outcomes)), Latest: lb.Latest, Outcomes: string(oj),
			})
		}
		w := parquet.NewGenericWriter[labelRow](f)
		if _, err := w.Write(rows); err != nil {
			return err
		}
		return w.Close()
	}

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, lb := range labels {
		if err := enc.Encode(lb); err != nil {
			return err
		}
	}
	return nil
}

// cmdScore runs a scorer over labelled episodes and writes rewards into the
// lake, as a manifested table like any other.
//
// It is idempotent per verifier version: an episode already scored by this
// verifier_id and version is skipped. With require_final on (the default) a
// final label never changes, so re-running never produces a different answer
// for the same episode — it only scores episodes whose horizon has since closed.
func cmdScore(args []string) int {
	fs := flag.NewFlagSet("score", flag.ExitOnError)
	jf := addJoinFlags(fs)
	scorerPath := fs.String("scorer", "", "a rules scorer YAML file")
	dryRun := fs.Bool("dry-run", false, "print the reward distribution, write nothing")
	_ = fs.Parse(args)

	if *scorerPath == "" {
		fmt.Fprintln(os.Stderr, "cc score: -scorer is required")
		return 2
	}
	sc, err := rules.Load(*scorerPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 1
	}
	l, o, err := jf.load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 2
	}
	labels, st := join.Run(l, o)
	printJoinStats(o, st, l.Unmanifested)

	already := map[string]bool{}
	for _, r := range l.Rewards {
		if r.VerifierID == sc.ID() && r.VerifierVersion == sc.Version() {
			already[r.EpisodeID] = true
		}
	}

	var out []record.Reward
	byClause := map[string]int{}
	skipped, notApplicable, failed := 0, 0, 0
	for _, lb := range labels {
		if already[lb.EpisodeID] {
			skipped++
			continue
		}
		ep, _ := l.Episode(lb.EpisodeID)
		r, ok, err := sc.Score(context.Background(), scorer.Episode{
			Episode: ep.Episode, Steps: ep.Steps,
			Latest: lb.Latest, LabelStatus: lb.LabelStatus, OutcomeCount: len(lb.Outcomes),
		})
		switch {
		case err != nil:
			failed++
			fmt.Fprintf(os.Stderr, "  episode %s: %v\n", lb.EpisodeID, err)
		case !ok:
			notApplicable++
		default:
			r.At = o.AsOf.UnixMicro()
			out = append(out, r)
			for c := range r.Clauses {
				byClause[c]++
			}
		}
	}

	fmt.Printf("\nscorer %s@%s\n", sc.ID(), sc.Version())
	fmt.Printf("  scored            %d\n", len(out))
	for _, c := range sortedCountKeys(byClause) {
		fmt.Printf("    %-24s %d\n", c, byClause[c])
	}
	fmt.Printf("  not applicable    %d  (provisional, or no rule matched)\n", notApplicable)
	if skipped > 0 {
		fmt.Printf("  already scored    %d  (this verifier version; skipped)\n", skipped)
	}
	if failed > 0 {
		fmt.Printf("  failed            %d\n", failed)
	}

	if *dryRun || len(out) == 0 {
		return exitIf(failed > 0)
	}

	store, err := objstore.NewFS(*jf.lake)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 1
	}
	sink, err := lake.New(lake.Options{Name: "score", Store: store})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 1
	}
	if err := sink.WriteRewards(context.Background(), out); err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 1
	}
	if err := sink.Flush(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "cc score: %v\n", err)
		return 1
	}
	fmt.Printf("\nwrote %d rewards to %s/rewards\n", len(out), *jf.lake)
	return exitIf(failed > 0)
}

// cmdExport writes a training dataset.
func cmdExport(args []string) int {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	jf := addJoinFlags(fs)
	format := fs.String("format", "trajectories", "trajectories, chat or preference")
	out := fs.String("out", "", "output JSONL file")
	verifier := fs.String("verifier", "", "whose rewards to use (required if the lake has several)")
	minReward := fs.String("min-reward", "", "drop episodes scoring below this")
	requireFinal := fs.Bool("require-final", true, "drop provisional and unjoinable labels")
	requireReward := fs.Bool("require-reward", false, "drop episodes the verifier did not score")
	fidelity := fs.String("require-fidelity", "", "comma list: params,token_spans,tool_versions")
	_ = fs.Parse(args)

	if *out == "" {
		fmt.Fprintln(os.Stderr, "cc export: -out is required")
		return 2
	}
	l, o, err := jf.load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc export: %v\n", err)
		return 2
	}
	labels, _ := join.Run(l, o)

	opts := export.Options{
		Format: *format, Verifier: *verifier,
		RequireFinal: *requireFinal, RequireReward: *requireReward,
	}
	if *minReward != "" {
		var v float64
		if _, err := fmt.Sscanf(*minReward, "%g", &v); err != nil {
			fmt.Fprintf(os.Stderr, "cc export: -min-reward: %v\n", err)
			return 2
		}
		opts.MinReward = &v
	}
	if *fidelity != "" {
		opts.RequireFidelity = strings.Split(*fidelity, ",")
	}

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc export: %v\n", err)
		return 1
	}
	defer f.Close()

	st, err := export.Run(f, l, labels, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc export: %v\n", err)
		return 1
	}

	fmt.Printf("exported %d %s record(s) from %d episodes to %s\n", st.Written, *format, st.Considered, *out)
	if st.Verifier != "" {
		fmt.Printf("  rewards from %s\n", st.Verifier)
	}
	if len(st.Dropped) > 0 {
		fmt.Println("  dropped:")
		for _, k := range sortedCountKeys(st.Dropped) {
			fmt.Printf("    %-26s %d\n", k, st.Dropped[k])
		}
	}
	return 0
}

func exitIf(b bool) int {
	if b {
		return 1
	}
	return 0
}
