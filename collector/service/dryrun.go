// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/collector/redact"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// DryRunResult is what a config would do to one sample episode (F-11.3).
//
// It names decisions and counts, never payload values — the same rule as the
// redaction manifest — so a dry-run report can be pasted into a ticket or a
// review without becoming a leak itself.
type DryRunResult struct {
	EpisodeID string
	Source    string

	// Quarantined is set when a processor would refuse the episode; the
	// reason says which one and why. Nothing else runs after that, exactly
	// as in production.
	Quarantined string

	Redactions []redact.ManifestEntry
	EntityKeys []record.EntityKey

	HeadSampled bool
	Kept        bool
	SampledBy   string

	// Placement is where each step's payload would land: inline, a blob,
	// or truncated.
	Placement []string
	Partition string
}

// DryRun runs sample episodes through every decision the live pipeline makes —
// quota aside — without writing anything.
//
// It builds the policy with the same code the running service uses, so a
// config that dry-runs cleanly means the same thing in production. A separate
// "preview" implementation would be one more place for the two to disagree.
func DryRun(cfg *config.Config, eps []*pipeline.Assembled) ([]DryRunResult, error) {
	var manifests []redact.Manifest
	pol, err := buildPolicy(cfg, func(m redact.Manifest) { manifests = append(manifests, m) })
	if err != nil {
		return nil, err
	}

	var sinkCfg config.Sink
	if len(cfg.Sinks) > 0 {
		sinkCfg = cfg.Sinks[0]
	}

	out := make([]DryRunResult, 0, len(eps))
	for _, ep := range eps {
		res := DryRunResult{EpisodeID: ep.Episode.EpisodeID, Source: ep.Episode.Source}
		res.HeadSampled = pol.sampler.HeadKeepFrom(ep.Episode.Source, ep.Episode.EpisodeID)

		manifests = manifests[:0]
		failed := false
		for _, p := range []pipeline.Processor{pol.redactorFor(ep.Episode.Source), pol.extractor} {
			if err := p.Process(context.Background(), ep); err != nil {
				res.Quarantined = fmt.Sprintf("%s: %v", p.Name(), err)
				failed = true
				break
			}
		}
		for _, m := range manifests {
			res.Redactions = append(res.Redactions, m.Entries...)
		}
		if failed {
			out = append(out, res)
			continue
		}

		res.EntityKeys = ep.Episode.EntityKeys
		res.Kept = res.HeadSampled && pol.sampler.TailKeep(ep)
		if ep.Episode.SampledBy != nil {
			res.SampledBy = *ep.Episode.SampledBy
		}
		res.Placement = placement(ep, sinkCfg)
		res.Partition = partition(ep.Episode, sinkCfg.PartitionBy)

		out = append(out, res)
	}
	return out, nil
}

func placement(ep *pipeline.Assembled, sink config.Sink) []string {
	var out []string
	for i, s := range ep.Steps {
		if s.ContentInline == nil {
			out = append(out, fmt.Sprintf("step %d: no payload", i))
			continue
		}
		n := len(*s.ContentInline)
		switch {
		case sink.MaxPayloadBytes > 0 && n > sink.MaxPayloadBytes:
			out = append(out, fmt.Sprintf("step %d: %d bytes, TRUNCATED to %d", i, n, sink.MaxPayloadBytes))
		case n > sink.BlobThresholdBytes:
			out = append(out, fmt.Sprintf("step %d: %d bytes, externalised to a blob", i, n))
		default:
			out = append(out, fmt.Sprintf("step %d: %d bytes, inline", i, n))
		}
	}
	return out
}

func partition(e record.Episode, keys []string) string {
	var parts []string
	for _, k := range keys {
		switch k {
		case "dt":
			ts := e.StartedAt
			if ts == 0 {
				ts = time.Now().UnixMicro()
			}
			parts = append(parts, "dt="+time.UnixMicro(ts).UTC().Format("2006-01-02"))
		case "tenant":
			parts = append(parts, "tenant="+e.Tenant)
		case "task_type":
			v := "unknown"
			if e.TaskType != nil && *e.TaskType != "" {
				v = *e.TaskType
			}
			parts = append(parts, "task_type="+v)
		}
	}
	return path.Join(append([]string{record.TableEpisodes}, parts...)...)
}

// String renders a result for the terminal.
func (r DryRunResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "episode %s (source %s)\n", r.EpisodeID, r.Source)

	if r.Quarantined != "" {
		fmt.Fprintf(&b, "  WOULD BE QUARANTINED and never written\n    %s\n", r.Quarantined)
		return b.String()
	}

	if len(r.Redactions) == 0 {
		b.WriteString("  redaction   no changes\n")
	} else {
		b.WriteString("  redaction\n")
		for _, e := range r.Redactions {
			fmt.Fprintf(&b, "    %-30s %-18s %s x%d\n", e.Path, e.RuleID, e.Action, e.Matches)
		}
	}

	if len(r.EntityKeys) == 0 {
		b.WriteString("  entity keys none (this episode could never be joined to an outcome)\n")
	} else {
		var ks []string
		for _, k := range r.EntityKeys {
			ks = append(ks, k.Name+"="+k.Value)
		}
		fmt.Fprintf(&b, "  entity keys %s\n", strings.Join(ks, " "))
	}

	switch {
	case !r.HeadSampled:
		b.WriteString("  sampling    DROPPED by head sampling\n")
	case !r.Kept:
		b.WriteString("  sampling    DROPPED by tail sampling\n")
	default:
		fmt.Fprintf(&b, "  sampling    kept (%s)\n", r.SampledBy)
	}

	if r.Kept {
		fmt.Fprintf(&b, "  written to  %s/\n", r.Partition)
		for _, p := range r.Placement {
			fmt.Fprintf(&b, "    %s\n", p)
		}
	}
	return b.String()
}
