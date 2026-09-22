// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package join

import (
	"testing"
	"time"

	"github.com/KatyarAILabs/trajectory/pkg/lakeread"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

var t0 = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) int64 { return t0.Add(d).UnixMicro() }
func sp(s string) *string      { return &s }

func episode(id string, start time.Duration, keys ...record.EntityKey) *lakeread.Episode {
	end := at(start + time.Minute)
	return &lakeread.Episode{Episode: record.Episode{
		EpisodeID: id, Tenant: "acme", Status: record.StatusComplete,
		StartedAt: at(start), EndedAt: &end, EntityKeys: keys,
	}}
}

func ticket(v string) record.EntityKey { return record.EntityKey{Name: "ticket_id", Value: v} }

func outcome(key, kind, value string, occurred, observed time.Duration) record.Outcome {
	return record.Outcome{
		EntityName: sp("ticket_id"), EntityKey: key, Kind: kind, Value: value,
		OccurredAt: at(occurred), ObservedAt: at(observed),
	}
}

func byID(labels []Label) map[string]Label {
	m := map[string]Label{}
	for _, l := range labels {
		m[l.EpisodeID] = l
	}
	return m
}

// The basic join: an outcome after the episode, on its key, is attached.
func TestAttachesOutcome(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 0, ticket("T-1"))},
		Outcomes: []record.Outcome{outcome("T-1", "ticket_status", "solved", time.Hour, time.Hour)},
	}
	labels, st := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour), Horizon: 30 * 24 * time.Hour})

	lb := byID(labels)["e1"]
	if lb.Latest["ticket_status"] != "solved" {
		t.Errorf("latest = %v", lb.Latest)
	}
	if lb.LabelStatus != StatusFinal {
		t.Errorf("label status = %q, want final: the horizon closed before as_of", lb.LabelStatus)
	}
	if st.Attributed != 1 || st.Unattributed != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// As-of: an outcome observed after as_of is invisible, so a dataset rebuilt at
// the same as_of is identical no matter what arrived since.
func TestAsOfExcludesLaterObservations(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 0, ticket("T-1"))},
		Outcomes: []record.Outcome{
			outcome("T-1", "ticket_status", "solved", time.Hour, time.Hour),
			// Happened on day 2 but only reported on day 10.
			outcome("T-1", "ticket_status", "reopened", 48*time.Hour, 240*time.Hour),
		},
	}

	asOf := t0.Add(5 * 24 * time.Hour)
	before, st := Run(l, Options{AsOf: asOf})
	if got := byID(before)["e1"].Latest["ticket_status"]; got != "solved" {
		t.Errorf("as of day 5 the ticket is %q; the reopen was not yet known", got)
	}
	if st.FutureObserved != 1 {
		t.Errorf("FutureObserved = %d, want 1", st.FutureObserved)
	}

	// Reproducibility: the same as_of gives the same answer.
	again, _ := Run(l, Options{AsOf: asOf})
	if byID(again)["e1"].Latest["ticket_status"] != "solved" {
		t.Error("the same as_of produced a different label")
	}

	after, _ := Run(l, Options{AsOf: t0.Add(11 * 24 * time.Hour)})
	if got := byID(after)["e1"].Latest["ticket_status"]; got != "reopened" {
		t.Errorf("as of day 11 the ticket is %q, want reopened", got)
	}
}

// Watermark: until the horizon closes, a label is provisional. Treating a
// recent episode's silence as success is the bias this prevents.
func TestProvisionalUntilHorizonCloses(t *testing.T) {
	l := &lakeread.Lake{Episodes: []*lakeread.Episode{episode("e1", 0, ticket("T-1"))}}
	h := 7 * 24 * time.Hour

	early, _ := Run(l, Options{AsOf: t0.Add(3 * 24 * time.Hour), Horizon: h})
	if s := byID(early)["e1"].LabelStatus; s != StatusProvisional {
		t.Errorf("day 3 of a 7-day horizon: %q, want provisional", s)
	}
	late, _ := Run(l, Options{AsOf: t0.Add(8 * 24 * time.Hour), Horizon: h})
	if s := byID(late)["e1"].LabelStatus; s != StatusFinal {
		t.Errorf("day 8 of a 7-day horizon: %q, want final", s)
	}
}

// An outcome that happened before the episode, or after its horizon, cannot
// have been caused by it.
func TestOutcomesOutsideTheWindowAreNotAttributed(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 24*time.Hour, ticket("T-1"))},
		Outcomes: []record.Outcome{
			outcome("T-1", "ticket_status", "opened", 0, 0),                             // before
			outcome("T-1", "ticket_status", "closed", 60*24*time.Hour, 60*24*time.Hour), // after horizon
		},
	}
	labels, st := Run(l, Options{AsOf: t0.Add(90 * 24 * time.Hour), Horizon: 7 * 24 * time.Hour})
	if n := len(byID(labels)["e1"].Outcomes); n != 0 {
		t.Errorf("attached %d outcomes from outside the window", n)
	}
	if st.Unattributed != 2 {
		t.Errorf("Unattributed = %d, want 2", st.Unattributed)
	}
}

// Last touch: when two runs touched the same ticket, the outcome goes to the
// most recent run before it — not to both.
func TestLastTouchAttribution(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{
			episode("first-run", 0, ticket("T-1")),
			episode("second-run", 2*time.Hour, ticket("T-1")),
		},
		Outcomes: []record.Outcome{outcome("T-1", "refund_status", "completed", 3*time.Hour, 3*time.Hour)},
	}
	labels, _ := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour)})
	m := byID(labels)
	if len(m["second-run"].Outcomes) != 1 || len(m["first-run"].Outcomes) != 0 {
		t.Errorf("last touch: first=%d second=%d, want 0 and 1",
			len(m["first-run"].Outcomes), len(m["second-run"].Outcomes))
	}

	all, _ := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour), Attribution: "all"})
	if n := len(byID(all)["first-run"].Outcomes); n != 1 {
		t.Errorf("attribution=all: first run got %d outcomes, want 1", n)
	}
}

// A ticket and an order that share an id string must not join to each other.
func TestEntityNameDisambiguates(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 0, ticket("77"))},
		Outcomes: []record.Outcome{{
			EntityName: sp("order_id"), EntityKey: "77", Kind: "refund_status",
			Value: "completed", OccurredAt: at(time.Hour), ObservedAt: at(time.Hour),
		}},
	}
	labels, _ := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour)})
	if n := len(byID(labels)["e1"].Outcomes); n != 0 {
		t.Error("an order outcome joined to a ticket with the same id string")
	}
}

// Re-posting an outcome counts once; the first observation wins.
func TestDuplicateOutcomesCountOnce(t *testing.T) {
	a := outcome("T-1", "ticket_status", "solved", time.Hour, time.Hour)
	b := outcome("T-1", "ticket_status", "solved", time.Hour, 2*time.Hour)
	a.OutcomeID, b.OutcomeID = sp("evt-1"), sp("evt-1")
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 0, ticket("T-1"))},
		Outcomes: []record.Outcome{b, a},
	}
	labels, st := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour)})
	lb := byID(labels)["e1"]
	if len(lb.Outcomes) != 1 || st.Duplicates != 1 {
		t.Fatalf("outcomes=%d duplicates=%d, want 1 and 1", len(lb.Outcomes), st.Duplicates)
	}
	if lb.Outcomes[0].ObservedAt != at(time.Hour) {
		t.Error("the later observation was kept; the first is when the fact was known")
	}
}

// Episodes with no keys can never be labelled, and say so.
func TestUnjoinable(t *testing.T) {
	l := &lakeread.Lake{Episodes: []*lakeread.Episode{episode("e1", 0)}}
	labels, st := Run(l, Options{})
	if byID(labels)["e1"].LabelStatus != StatusUnjoinable || st.Unjoinable != 1 {
		t.Errorf("status = %q", byID(labels)["e1"].LabelStatus)
	}
}

// The latest value of a kind is the most recent by occurrence, not by the
// order outcomes were loaded in.
func TestLatestIsByOccurrence(t *testing.T) {
	l := &lakeread.Lake{
		Episodes: []*lakeread.Episode{episode("e1", 0, ticket("T-1"))},
		Outcomes: []record.Outcome{
			outcome("T-1", "ticket_status", "reopened", 5*time.Hour, 5*time.Hour),
			outcome("T-1", "ticket_status", "solved", time.Hour, 6*time.Hour), // reported late
		},
	}
	labels, _ := Run(l, Options{AsOf: t0.Add(40 * 24 * time.Hour)})
	if got := byID(labels)["e1"].Latest["ticket_status"]; got != "reopened" {
		t.Errorf("latest = %q; the reopen happened later even though it was reported first", got)
	}
}
