// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// cmdReplay implements F-14.6: print a reconstructed episode as readable text.
//
// "Replay" here means reconstruct and render, not re-execute. The collector has
// no opinion about correctness and runs nothing (N-3); this is the tool for
// looking at what was captured and deciding whether it is enough to replay
// elsewhere.
func cmdReplay(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	lake := fs.String("lake", "", "path to the sink root directory")
	full := fs.Bool("full", false, "print whole payloads instead of truncating")
	_ = fs.Parse(args)

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "cc replay: an episode id is required")
		return 2
	}
	if *lake == "" {
		fmt.Fprintln(os.Stderr, "cc replay: -lake is required")
		return 2
	}
	id := rest[0]

	eps, steps, err := findEpisode(*lake, id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc replay: %v\n", err)
		return 1
	}
	if len(eps) == 0 {
		fmt.Fprintf(os.Stderr, "cc replay: no episode %s under %s\n", id, *lake)
		return 1
	}

	// An episode may be spread across an original record and any number of
	// append-only patches (F-3.5). Rendering only the original would
	// silently hide late-arriving steps.
	var original record.Episode
	patches := 0
	for _, e := range eps {
		if e.Status == record.StatusPatched {
			patches++
			continue
		}
		original = e
	}
	if original.EpisodeID == "" {
		original = eps[0]
	}

	printEpisodeHeader(original, patches)

	// Order by time, which is stable across the original and its patches;
	// step_idx is not, because patch indices are allocated separately.
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].StartedAt != steps[j].StartedAt {
			return steps[i].StartedAt < steps[j].StartedAt
		}
		return steps[i].StepIdx < steps[j].StepIdx
	})

	for _, s := range steps {
		printStep(*lake, s, *full)
	}

	printFidelityNote(original, steps)
	return 0
}

func printEpisodeHeader(e record.Episode, patches int) {
	fmt.Printf("episode  %s\n", e.EpisodeID)
	fmt.Printf("tenant   %s   source %s   status %s\n", e.Tenant, e.Source, e.Status)
	if e.TaskType != nil {
		fmt.Printf("task     %s\n", *e.TaskType)
	}
	if e.GroupID != nil {
		fmt.Printf("group    %s\n", *e.GroupID)
	}
	if e.Instrumentation != nil {
		fmt.Printf("from     %s %s\n", *e.Instrumentation, derefStr(e.InstrumentationVersion))
	}
	fmt.Printf("started  %s\n", tsOf(e.StartedAt))
	if e.Error != nil {
		fmt.Printf("error    %s: %s\n", e.Error.Type, e.Error.Message)
	}
	if len(e.EntityKeys) > 0 {
		var ks []string
		for _, k := range e.EntityKeys {
			ks = append(ks, k.Name+"="+k.Value)
		}
		fmt.Printf("keys     %s\n", strings.Join(ks, "  "))
	}
	if patches > 0 {
		fmt.Printf("patches  %d late-arriving record(s) merged into this view\n", patches)
	}
	fmt.Println(strings.Repeat("-", 72))
}

func printStep(lake string, s record.Step, full bool) {
	label := s.Kind
	switch {
	case s.ToolName != nil:
		label = "tool " + *s.ToolName
		if s.ToolVersion != nil {
			label += "@" + *s.ToolVersion
		}
	case s.Model != nil:
		label = "llm  " + *s.Model
	}

	indent := ""
	if s.ParentIdx != nil {
		indent = "  "
	}

	fmt.Printf("\n%s[%d] %s", indent, s.StepIdx, label)
	if s.ParentIdx != nil {
		fmt.Printf("   (from step %d)", *s.ParentIdx)
	}
	if s.Attempt > 0 {
		fmt.Printf("   attempt %d", s.Attempt)
	}
	if s.LatencyMs != nil {
		fmt.Printf("   %dms", *s.LatencyMs)
	}
	fmt.Println()

	if p := s.Params; p != nil {
		var parts []string
		if p.Temperature != nil {
			parts = append(parts, fmt.Sprintf("temperature=%v", *p.Temperature))
		}
		if p.TopP != nil {
			parts = append(parts, fmt.Sprintf("top_p=%v", *p.TopP))
		}
		if p.MaxTokens != nil {
			parts = append(parts, fmt.Sprintf("max_tokens=%d", *p.MaxTokens))
		}
		if p.Seed != nil {
			parts = append(parts, fmt.Sprintf("seed=%d", *p.Seed))
		}
		if len(parts) > 0 {
			fmt.Printf("%s     %s\n", indent, strings.Join(parts, "  "))
		}
	}
	if t := s.TokenCounts; t != nil {
		var parts []string
		if t.Input != nil {
			parts = append(parts, fmt.Sprintf("in=%d", *t.Input))
		}
		if t.Output != nil {
			parts = append(parts, fmt.Sprintf("out=%d", *t.Output))
		}
		if len(parts) > 0 {
			fmt.Printf("%s     tokens %s\n", indent, strings.Join(parts, " "))
		}
	}
	if s.Error != nil {
		fmt.Printf("%s     error: %s %s\n", indent, s.Error.Type, s.Error.Message)
	}

	body := resolvePayload(lake, s)
	if body == "" {
		return
	}
	if !full {
		body = truncate(body, 600)
	}
	if s.Truncated {
		body += "\n     [payload was truncated at capture time: exceeded max_payload_bytes]"
	}
	for _, line := range strings.Split(prettyJSON(body), "\n") {
		fmt.Printf("%s     %s\n", indent, line)
	}
}

// resolvePayload follows a content_ref into the blob store, so replay shows the
// actual payload rather than a hash.
func resolvePayload(lake string, s record.Step) string {
	if s.ContentInline != nil {
		return *s.ContentInline
	}
	if s.ContentRef == nil {
		return ""
	}

	h := *s.ContentRef
	if len(h) < 4 {
		return ""
	}
	path := filepath.Join(lake, record.TableBlobs, "sha256", h[0:2], h[2:4], h)
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("[blob %s could not be read: %v]", h[:12], err)
	}
	return string(b)
}

func prettyJSON(s string) string {
	t := strings.TrimSpace(s)
	if t == "" || (t[0] != '{' && t[0] != '[') {
		return s
	}
	var v any
	if err := json.Unmarshal([]byte(t), &v); err != nil {
		return s
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return s
	}
	return string(b)
}

// printFidelityNote tells the reader what this episode cannot support, which is
// the question F-4.6 exists to answer.
func printFidelityNote(e record.Episode, steps []record.Step) {
	fmt.Println()
	fmt.Println(strings.Repeat("-", 72))
	if e.Fidelity == nil {
		fmt.Println("fidelity not recorded for this episode")
		return
	}
	f := *e.Fidelity
	fmt.Printf("fidelity  params=%v token_spans=%v tool_versions=%v\n",
		f.HasParams, f.HasTokenSpans, f.HasToolVersions)
	if !f.HasParams {
		fmt.Println("          without generation parameters this episode cannot be replayed exactly")
	}
	if !f.HasTokenSpans {
		fmt.Println("          without token spans a trainer cannot tell model output from tool output")
	}

	// has_params means the source reported generation parameters, not that
	// they are sufficient to reproduce the call. A seed is what makes a
	// sampled completion reproducible, and most tracing exports drop it, so
	// it is called out separately rather than folded into the boolean.
	if f.HasParams && !anyLLMStepHasSeed(steps) {
		fmt.Println("          params were captured but no seed was: a sampled completion")
		fmt.Println("          from this episode will not reproduce exactly")
	}
}

func anyLLMStepHasSeed(steps []record.Step) bool {
	for _, s := range steps {
		if s.Kind == record.KindLLM && s.Params != nil && s.Params.Seed != nil {
			return true
		}
	}
	return false
}

// findEpisode scans the lake for one episode and its steps, including patches.
func findEpisode(lake, id string) ([]record.Episode, []record.Step, error) {
	var eps []record.Episode
	var steps []record.Step

	err := filepath.Walk(lake, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return err
		}
		p := filepath.ToSlash(path)

		switch {
		case strings.Contains(p, "/"+record.TableEpisodes+"/"):
			rows, err := parquet.ReadFile[record.Episode](path)
			if err != nil {
				return nil // a partially written file is not fatal to a lookup
			}
			for _, e := range rows {
				if e.EpisodeID == id {
					eps = append(eps, e)
				}
			}
		case strings.Contains(p, "/"+record.TableSteps+"/"):
			rows, err := parquet.ReadFile[record.Step](path)
			if err != nil {
				return nil
			}
			for _, s := range rows {
				if s.EpisodeID == id {
					steps = append(steps, s)
				}
			}
		}
		return nil
	})
	return eps, steps, err
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
