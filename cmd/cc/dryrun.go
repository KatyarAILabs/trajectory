// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/service"
	"github.com/trajectory-project/trajectory/collector/wire"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// dryRun implements `cc validate -sample` (F-11.3): what would this config do
// to this record, end to end, with nothing written.
func dryRun(cfg *config.Config, path string) int {
	body, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc validate: %v\n", err)
		return 1
	}
	wes, err := wire.DecodeEpisodes(body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc validate: %s is not a native episode file: %v\n", path, err)
		return 1
	}

	var eps []*pipeline.Assembled
	for _, we := range wes {
		envs, err := we.ToEnvelopes("dry-run", cfg.Tenant)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cc validate: episode %s: %v\n", we.EpisodeID, err)
			return 1
		}
		ep := &pipeline.Assembled{Episode: record.Episode{
			EpisodeID: we.EpisodeID,
			Tenant:    cfg.Tenant,
			Source:    firstSource(cfg),
			Status:    record.StatusComplete,
			StartedAt: int64(we.StartedAt),
			StepCount: int32(len(envs)),
			Raw:       we.Raw,
		}}
		if we.TaskType != "" {
			tt := we.TaskType
			ep.Episode.TaskType = &tt
		}
		for _, env := range envs {
			ep.Steps = append(ep.Steps, env.Step)
		}
		eps = append(eps, ep)
	}

	results, err := service.DryRun(cfg, eps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc validate: %v\n", err)
		return 1
	}

	fmt.Printf("\ndry run: %d episode(s) from %s, nothing written\n\n", len(results), path)
	quarantined := 0
	for _, r := range results {
		fmt.Println(r)
		if r.Quarantined != "" {
			quarantined++
		}
	}
	if quarantined > 0 {
		fmt.Printf("%d episode(s) would be quarantined.\n", quarantined)
		return 1
	}
	return 0
}

// firstSource attributes the sample to the first configured source, so a
// per-source redaction override is exercised the way it would be live.
func firstSource(cfg *config.Config) string {
	if len(cfg.Sources) > 0 {
		return cfg.Sources[0].Name
	}
	return "dry-run"
}
