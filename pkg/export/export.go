// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package export turns a labelled, scored lake into training data.
//
// Three shapes, because three kinds of training want different things:
//
//   - trajectories: one line per episode with every step, its outcomes and its
//     reward. The input for RL and for anyone who wants to decide for
//     themselves.
//   - chat: one supervised fine-tuning example per model call, drawn only from
//     episodes that cleared the reward bar. The common case.
//   - preference: best-versus-worst pairs within a group_id — N rollouts of one
//     task, which is what group_id means (§7.1). The input for DPO-style
//     preference training.
//
// Filters are applied before anything is written, and every dropped episode
// is counted by reason, so a dataset that comes out small says why.
package export

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/KatyarAILabs/trajectory/pkg/join"
	"github.com/KatyarAILabs/trajectory/pkg/lakeread"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Formats.
const (
	FormatTrajectories = "trajectories"
	FormatChat         = "chat"
	FormatPreference   = "preference"
)

// Options select what is exported.
type Options struct {
	Format string
	// Verifier picks whose rewards to use. Empty means: the only verifier in
	// the lake, and an error if there is more than one — silently choosing
	// between two reward definitions is how a dataset ends up trained on the
	// wrong objective.
	Verifier string
	// MinReward drops episodes scoring below it. Nil disables the filter.
	MinReward *float64
	// RequireReward drops episodes the verifier did not score.
	RequireReward bool
	// RequireFinal drops provisional and unjoinable labels.
	RequireFinal bool
	// RequireFidelity lists fidelity flags that must be true: params,
	// token_spans, tool_versions.
	RequireFidelity []string
}

// Stats say what was written and why the rest was not.
type Stats struct {
	Considered int
	Written    int
	// Dropped counts episodes excluded, by reason.
	Dropped  map[string]int
	Verifier string
}

type rewardKey struct{ episode string }

// Run writes the export to w.
func Run(w io.Writer, l *lakeread.Lake, labels []join.Label, o Options) (Stats, error) {
	st := Stats{Dropped: map[string]int{}}

	verifier, err := chooseVerifier(l.Rewards, o.Verifier)
	if err != nil {
		return st, err
	}
	st.Verifier = verifier
	rewards := latestRewards(l.Rewards, verifier)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	type kept struct {
		label  join.Label
		ep     *lakeread.Episode
		reward *record.Reward
	}
	var selected []kept

	for _, lb := range labels {
		st.Considered++
		ep, ok := l.Episode(lb.EpisodeID)
		if !ok {
			st.Dropped["missing_episode"]++
			continue
		}
		rw, scored := rewards[lb.EpisodeID]

		switch {
		case o.RequireFinal && lb.LabelStatus != join.StatusFinal:
			st.Dropped["label_"+lb.LabelStatus]++
			continue
		case o.RequireReward && !scored:
			st.Dropped["not_scored"]++
			continue
		case o.MinReward != nil && (!scored || rw.Reward < *o.MinReward):
			st.Dropped["below_min_reward"]++
			continue
		}
		if missing := missingFidelity(ep.Fidelity, o.RequireFidelity); missing != "" {
			st.Dropped["fidelity_"+missing]++
			continue
		}

		var rp *record.Reward
		if scored {
			r := rw
			rp = &r
		}
		selected = append(selected, kept{lb, ep, rp})
	}

	switch o.Format {
	case FormatTrajectories, "":
		for _, k := range selected {
			line, err := trajectoryLine(l, k.label, k.ep, k.reward)
			if err != nil {
				return st, err
			}
			if err := enc.Encode(line); err != nil {
				return st, err
			}
			st.Written++
		}

	case FormatChat:
		for _, k := range selected {
			n, unusable, err := writeChat(enc, l, k.ep, k.reward)
			if err != nil {
				return st, err
			}
			st.Written += n
			if unusable > 0 {
				// A model step with no readable prompt or reply has
				// nothing to train on. Counted, so an empty dataset
				// says why rather than just being empty.
				st.Dropped["llm_step_without_prompt_or_reply"] += unusable
			}
		}

	case FormatPreference:
		groups := map[string][]kept{}
		for _, k := range selected {
			if k.label.GroupID == "" {
				st.Dropped["no_group_id"]++
				continue
			}
			if k.reward == nil {
				st.Dropped["not_scored"]++
				continue
			}
			groups[k.label.GroupID] = append(groups[k.label.GroupID], k)
		}
		for _, g := range sortedKeys(groups) {
			ks := groups[g]
			sort.Slice(ks, func(i, j int) bool {
				if ks[i].reward.Reward != ks[j].reward.Reward {
					return ks[i].reward.Reward > ks[j].reward.Reward
				}
				return ks[i].label.EpisodeID < ks[j].label.EpisodeID
			})
			best, worst := ks[0], ks[len(ks)-1]
			if len(ks) < 2 || best.reward.Reward == worst.reward.Reward {
				// No preference to learn from a group whose
				// rollouts all scored the same.
				st.Dropped["group_without_contrast"] += len(ks)
				continue
			}
			chosen, err := trajectoryLine(l, best.label, best.ep, best.reward)
			if err != nil {
				return st, err
			}
			rejected, err := trajectoryLine(l, worst.label, worst.ep, worst.reward)
			if err != nil {
				return st, err
			}
			if err := enc.Encode(map[string]any{
				"group_id": g,
				"prompt":   firstPrompt(l, best.ep),
				"chosen":   chosen,
				"rejected": rejected,
			}); err != nil {
				return st, err
			}
			st.Written++
		}

	default:
		return st, fmt.Errorf("unknown format %q; use trajectories, chat or preference", o.Format)
	}
	return st, nil
}

func chooseVerifier(rewards []record.Reward, want string) (string, error) {
	ids := map[string]bool{}
	for _, r := range rewards {
		ids[r.VerifierID] = true
	}
	if want != "" {
		if len(rewards) > 0 && !ids[want] {
			return "", fmt.Errorf("no rewards from verifier %q in this lake; present: %s",
				want, strings.Join(keys(ids), ", "))
		}
		return want, nil
	}
	switch len(ids) {
	case 0:
		return "", nil
	case 1:
		for id := range ids {
			return id, nil
		}
	}
	return "", fmt.Errorf("this lake holds rewards from %d verifiers (%s); choose one with -verifier",
		len(ids), strings.Join(keys(ids), ", "))
}

// latestRewards keeps, per episode, the reward from the highest version of the
// chosen verifier, and the latest scoring of that version.
func latestRewards(all []record.Reward, verifier string) map[string]record.Reward {
	out := map[string]record.Reward{}
	for _, r := range all {
		if r.VerifierID != verifier {
			continue
		}
		prev, ok := out[r.EpisodeID]
		if !ok || r.VerifierVersion > prev.VerifierVersion ||
			(r.VerifierVersion == prev.VerifierVersion && r.At > prev.At) {
			out[r.EpisodeID] = r
		}
	}
	return out
}

func missingFidelity(f *record.Fidelity, want []string) string {
	for _, w := range want {
		ok := false
		if f != nil {
			switch w {
			case "params":
				ok = f.HasParams
			case "token_spans":
				ok = f.HasTokenSpans
			case "tool_versions":
				ok = f.HasToolVersions
			}
		}
		if !ok {
			return w
		}
	}
	return ""
}

func keys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys[T any](m map[string]T) []string { return keys(m) }
