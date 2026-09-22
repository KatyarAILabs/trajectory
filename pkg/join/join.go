// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package join attaches business outcomes to the agent trajectories that
// produced them.
//
// This is the step the v0.1 spec deferred (N-2) because it "requires
// watermarks and as-of semantics". Both are here, and both are the difference
// between a dataset you can train on and one that quietly lies:
//
//   - As-of. A join is evaluated as of a point in time. An outcome observed
//     after that point is invisible, so the same as-of on the same lake always
//     yields the same labels — a training set can be rebuilt exactly.
//   - Watermark. Each episode has an observation horizon: how long after it
//     ends an outcome can still be attributed to it. Until the horizon has
//     passed (as of the join), its label is provisional. Without this, every
//     recent episode looks like a success simply because the complaint about it
//     has not arrived yet — a bias toward rewarding whatever the agent did last
//     week.
//
// Attribution is last-touch: when several episodes touched the same key, an
// outcome goes to the most recent episode that started before it happened.
// Crediting every episode that ever touched a ticket would reward runs for
// outcomes other runs caused.
package join

import (
	"sort"
	"time"

	"github.com/KatyarAILabs/trajectory/pkg/lakeread"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Label statuses.
const (
	// Final: the observation horizon closed before as_of. The outcome set
	// is complete and the label will not change on a later join.
	StatusFinal = "final"
	// Provisional: the horizon is still open. More outcomes may arrive.
	StatusProvisional = "provisional"
	// Unjoinable: the episode carries no entity keys, so no outcome can
	// ever reach it. The share of these is what cc_episodes_without_entity_
	// keys_ratio warns about.
	StatusUnjoinable = "unjoinable"
)

// Options configure a join.
type Options struct {
	// AsOf is the point in time the join is evaluated at. Zero means now.
	AsOf time.Time
	// Horizon is how long after an episode ends an outcome may still be
	// attributed to it. Zero means 30 days.
	Horizon time.Duration
	// Attribution is "last_touch" (default) or "all".
	Attribution string
}

// Outcome is one outcome attributed to an episode.
type Outcome struct {
	OutcomeID  string `json:"outcome_id,omitempty"`
	EntityName string `json:"entity_name"`
	EntityKey  string `json:"entity_key"`
	Kind       string `json:"kind"`
	Value      string `json:"value"`
	OccurredAt int64  `json:"occurred_at"`
	ObservedAt int64  `json:"observed_at"`
	Source     string `json:"source,omitempty"`
}

// Label is an episode with its outcomes.
type Label struct {
	EpisodeID   string `json:"episode_id"`
	Tenant      string `json:"tenant"`
	TaskType    string `json:"task_type,omitempty"`
	GroupID     string `json:"group_id,omitempty"`
	Status      string `json:"episode_status"`
	StartedAt   int64  `json:"started_at"`
	EndedAt     int64  `json:"ended_at"`
	StepCount   int32  `json:"step_count"`
	HasError    bool   `json:"has_error"`
	LabelStatus string `json:"label_status"`
	// HorizonClosesAt is when this episode's label becomes final.
	HorizonClosesAt int64 `json:"horizon_closes_at"`
	AsOf            int64 `json:"as_of"`

	EntityKeys []record.EntityKey `json:"entity_keys"`
	Fidelity   *record.Fidelity   `json:"fidelity,omitempty"`

	// Outcomes attributed to this episode, oldest first.
	Outcomes []Outcome `json:"outcomes"`
	// Latest is the most recent value of each outcome kind — what a scorer
	// usually wants ("what is the ticket's status now").
	Latest map[string]string `json:"latest"`
}

// Stats summarise a join.
type Stats struct {
	Episodes     int
	Final        int
	Provisional  int
	Unjoinable   int
	WithOutcomes int
	Outcomes     int
	Attributed   int
	// Unattributed outcomes matched no episode: wrong keys, keys from a
	// source whose episodes are not in this lake, or outcomes that happened
	// before any episode touched the entity. A high count is the first
	// thing to check when labels look empty.
	Unattributed int
	// FutureObserved outcomes were excluded because they were observed
	// after as_of.
	FutureObserved int
	Duplicates     int
}

type keyID struct{ name, value string }

// Run joins a lake's outcomes to its episodes.
func Run(l *lakeread.Lake, o Options) ([]Label, Stats) {
	asOf := o.AsOf
	if asOf.IsZero() {
		asOf = time.Now()
	}
	horizon := o.Horizon
	if horizon <= 0 {
		horizon = 30 * 24 * time.Hour
	}
	asOfUS := asOf.UnixMicro()
	horizonUS := horizon.Microseconds()

	var st Stats
	labels := make([]Label, 0, len(l.Episodes))
	index := map[*lakeread.Episode]int{}
	byKey := map[keyID][]*lakeread.Episode{}

	for _, ep := range l.Episodes {
		end := ep.StartedAt
		if ep.EndedAt != nil && *ep.EndedAt > end {
			end = *ep.EndedAt
		}
		lb := Label{
			EpisodeID:       ep.EpisodeID,
			Tenant:          ep.Tenant,
			Status:          ep.Status,
			StartedAt:       ep.StartedAt,
			EndedAt:         end,
			StepCount:       int32(len(ep.Steps)),
			HasError:        ep.Error != nil,
			HorizonClosesAt: end + horizonUS,
			AsOf:            asOfUS,
			EntityKeys:      ep.EntityKeys,
			Fidelity:        ep.Fidelity,
			Outcomes:        []Outcome{},
			Latest:          map[string]string{},
		}
		if ep.TaskType != nil {
			lb.TaskType = *ep.TaskType
		}
		if ep.GroupID != nil {
			lb.GroupID = *ep.GroupID
		}

		switch {
		case len(ep.EntityKeys) == 0:
			lb.LabelStatus = StatusUnjoinable
		case asOfUS >= lb.HorizonClosesAt:
			lb.LabelStatus = StatusFinal
		default:
			lb.LabelStatus = StatusProvisional
		}

		index[ep] = len(labels)
		labels = append(labels, lb)
		for _, k := range ep.EntityKeys {
			id := keyID{k.Name, k.Value}
			byKey[id] = append(byKey[id], ep)
		}
	}

	// Episodes per key, newest first, so last-touch is the first eligible.
	for k := range byKey {
		eps := byKey[k]
		sort.Slice(eps, func(i, j int) bool { return eps[i].StartedAt > eps[j].StartedAt })
	}

	for _, oc := range dedupe(l.Outcomes, &st) {
		if oc.ObservedAt > asOfUS {
			st.FutureObserved++
			continue
		}
		st.Outcomes++

		name := ""
		if oc.EntityName != nil {
			name = *oc.EntityName
		}
		matched := false
		for _, ep := range byKey[keyID{name, oc.EntityKey}] {
			lb := &labels[index[ep]]
			// Only outcomes that happened after the episode started,
			// and within its horizon, can have been caused by it.
			if oc.OccurredAt < ep.StartedAt || oc.OccurredAt > lb.HorizonClosesAt {
				continue
			}
			lb.Outcomes = append(lb.Outcomes, toOutcome(oc, name))
			matched = true
			if o.Attribution != "all" {
				break // last touch: newest eligible episode only
			}
		}
		if matched {
			st.Attributed++
		} else {
			st.Unattributed++
		}
	}

	for i := range labels {
		lb := &labels[i]
		sort.Slice(lb.Outcomes, func(a, b int) bool {
			if lb.Outcomes[a].OccurredAt != lb.Outcomes[b].OccurredAt {
				return lb.Outcomes[a].OccurredAt < lb.Outcomes[b].OccurredAt
			}
			return lb.Outcomes[a].ObservedAt < lb.Outcomes[b].ObservedAt
		})
		for _, oc := range lb.Outcomes {
			lb.Latest[oc.Kind] = oc.Value // sorted ascending: last wins
		}

		st.Episodes++
		switch lb.LabelStatus {
		case StatusFinal:
			st.Final++
		case StatusProvisional:
			st.Provisional++
		case StatusUnjoinable:
			st.Unjoinable++
		}
		if len(lb.Outcomes) > 0 {
			st.WithOutcomes++
		}
	}
	return labels, st
}

// dedupe removes re-posted outcomes: by outcome_id when present, else by the
// full identity of the fact. The earliest observation is kept, because that is
// when the collector first knew.
func dedupe(in []record.Outcome, st *Stats) []record.Outcome {
	sorted := append([]record.Outcome(nil), in...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ObservedAt < sorted[j].ObservedAt })

	seen := map[string]bool{}
	out := sorted[:0]
	for _, o := range sorted {
		var key string
		if o.OutcomeID != nil && *o.OutcomeID != "" {
			key = "id:" + *o.OutcomeID
		} else {
			name := ""
			if o.EntityName != nil {
				name = *o.EntityName
			}
			key = name + "\x00" + o.EntityKey + "\x00" + o.Kind + "\x00" + o.Value + "\x00" +
				time.UnixMicro(o.OccurredAt).UTC().Format(time.RFC3339Nano)
		}
		if seen[key] {
			st.Duplicates++
			continue
		}
		seen[key] = true
		out = append(out, o)
	}
	return out
}

func toOutcome(o record.Outcome, name string) Outcome {
	out := Outcome{
		EntityName: name, EntityKey: o.EntityKey, Kind: o.Kind, Value: o.Value,
		OccurredAt: o.OccurredAt, ObservedAt: o.ObservedAt, Source: o.Source,
	}
	if o.OutcomeID != nil {
		out.OutcomeID = *o.OutcomeID
	}
	return out
}
