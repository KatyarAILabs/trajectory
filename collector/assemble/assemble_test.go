// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package assemble

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

func ptr[T any](v T) *T { return &v }

// fixedClock lets window behaviour be tested without sleeping.
type fixedClock struct{ t time.Time }

func (c *fixedClock) now() time.Time          { return c.t }
func (c *fixedClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// seqIDs makes episode ids deterministic so two runs are comparable.
func seqIDs() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("ep-%04d", n)
	}
}

func newTestAssembler(t *testing.T, clk *fixedClock, opts ...func(*Options)) (*Assembler, *[]*pipeline.Assembled) {
	t.Helper()
	var got []*pipeline.Assembled

	o := Options{
		Tenant:      "acme",
		Window:      5 * time.Minute,
		MaxInFlight: 1000,
		// Long enough that nothing closes while a test feeds spans at a
		// frozen clock; tests drain with Flush or by advancing time.
		SettleAfterTerminal: 30 * time.Second,
		Now:                 clk.now,
		NewID:               seqIDs(),
	}
	for _, f := range opts {
		f(&o)
	}

	a := New(o, func(ep *pipeline.Assembled) { got = append(got, ep) })
	return a, &got
}

// span builds one envelope in a session.
func span(session, spanID, parent string, startUS int64, kind string, content string) pipeline.Envelope {
	return pipeline.Envelope{
		SessionKey:   session,
		Source:       "otlp",
		SpanID:       spanID,
		ParentSpanID: parent,
		Meta:         pipeline.EpisodeMeta{Instrumentation: "openinference"},
		Step: record.Step{
			Kind:          kind,
			StartedAt:     startUS,
			ContentInline: ptr(content),
			Trainable:     record.TrainableUnknown,
			LatencyMs:     ptr(int32(10)),
		},
	}
}

func terminal(e pipeline.Envelope) pipeline.Envelope {
	e.Terminal = true
	return e
}

// episodeStream is a fixed trajectory: a root chain with a retry branch, so
// the ordering and tree logic both get exercised.
func episodeStream() []pipeline.Envelope {
	const s = "sess-1"
	return []pipeline.Envelope{
		span(s, "a", "", 1_000_000, record.KindLLM, "plan the refund"),
		span(s, "b", "a", 2_000_000, record.KindTool, "lookup order"),
		span(s, "c", "a", 3_000_000, record.KindTool, "retry lookup order"),
		span(s, "d", "c", 4_000_000, record.KindLLM, "summarise"),
		terminal(span(s, "e", "", 5_000_000, record.KindLLM, "done")),
	}
}

// F-3 acceptance: a recorded span stream replayed shuffled, with duplicates,
// must produce episodes byte-identical to those from the ordered stream.
//
// This is the test that forbids any arrival-order state in the assembler.
func TestShuffledAndDuplicatedStreamIsIdentical(t *testing.T) {
	ordered := func() *pipeline.Assembled {
		clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
		a, got := newTestAssembler(t, clk)
		for _, e := range episodeStream() {
			a.Add(e)
		}
		a.Flush(record.StatusTimedOut)
		if len(*got) != 1 {
			t.Fatalf("ordered stream produced %d episodes, want 1", len(*got))
		}
		return (*got)[0]
	}()

	for seed := int64(0); seed < 50; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			stream := episodeStream()
			// 10% duplicate rate, per the acceptance criterion.
			var dup []pipeline.Envelope
			for _, e := range stream {
				dup = append(dup, e)
				if rng.Float64() < 0.10 {
					dup = append(dup, e)
				}
			}
			rng.Shuffle(len(dup), func(i, j int) { dup[i], dup[j] = dup[j], dup[i] })

			clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
			a, got := newTestAssembler(t, clk)
			for _, e := range dup {
				a.Add(e)
			}
			// The terminal span may not have arrived last, so drain.
			// The fallback is timed_out on purpose: if settle were
			// not working, an episode split in two and the tail
			// piece would surface here as timed_out rather than
			// being masked as complete.
			a.Flush(record.StatusTimedOut)

			if len(*got) != 1 {
				t.Fatalf("got %d episodes, want 1", len(*got))
			}
			if !reflect.DeepEqual((*got)[0], ordered) {
				t.Errorf("shuffled stream differs from ordered stream\nordered: %+v\nshuffled: %+v",
					ordered.Steps, (*got)[0].Steps)
			}
		})
	}
}

// F-3.4: retries and branches survive as a tree alongside the linear ordering.
func TestTreeStructurePreserved(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk)
	for _, e := range episodeStream() {
		a.Add(e)
	}
	a.Flush(record.StatusTimedOut)

	steps := (*got)[0].Steps
	if len(steps) != 5 {
		t.Fatalf("got %d steps, want 5", len(steps))
	}

	// Linear order follows timestamps: a,b,c,d,e at indices 0..4.
	for i, s := range steps {
		if s.StepIdx != int32(i) {
			t.Errorf("step %d has StepIdx %d", i, s.StepIdx)
		}
	}
	if steps[0].ParentIdx != nil {
		t.Errorf("root step has parent %v, want nil", steps[0].ParentIdx)
	}
	// b and c both branch from a (index 0) — that is the retry.
	if steps[1].ParentIdx == nil || *steps[1].ParentIdx != 0 {
		t.Errorf("step b parent = %v, want 0", steps[1].ParentIdx)
	}
	if steps[2].ParentIdx == nil || *steps[2].ParentIdx != 0 {
		t.Errorf("step c parent = %v, want 0", steps[2].ParentIdx)
	}
	if steps[3].ParentIdx == nil || *steps[3].ParentIdx != 2 {
		t.Errorf("step d parent = %v, want 2", steps[3].ParentIdx)
	}
}

// F-3.7: at capacity the oldest episode is emitted early marked evicted,
// rather than the buffer growing. An OOM here takes down a production pod.
func TestBoundedBufferEvictsOldest(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk, func(o *Options) { o.MaxInFlight = 3 })

	for i := 0; i < 5; i++ {
		a.Add(span(fmt.Sprintf("sess-%d", i), fmt.Sprintf("s%d", i), "",
			int64(i)*1_000_000, record.KindLLM, "x"))
	}

	if a.InFlight() > 3 {
		t.Errorf("in-flight %d exceeds MaxInFlight 3", a.InFlight())
	}
	if len(*got) != 2 {
		t.Fatalf("got %d evictions, want 2", len(*got))
	}
	for _, ep := range *got {
		if ep.Episode.Status != record.StatusEvicted {
			t.Errorf("status = %q, want %q", ep.Episode.Status, record.StatusEvicted)
		}
	}
	if s := a.Stats(); s.Evicted != 2 {
		t.Errorf("Stats().Evicted = %d, want 2", s.Evicted)
	}
}

// F-3.2/F-3.3: an episode with no terminal event is emitted at window expiry,
// marked timed_out rather than silently held or dropped.
func TestWindowExpiryEmitsTimedOut(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk, func(o *Options) { o.Window = time.Minute })

	a.Add(span("sess-1", "a", "", 1_000_000, record.KindLLM, "no end marker"))
	if len(*got) != 0 {
		t.Fatalf("emitted early: %d", len(*got))
	}

	clk.advance(90 * time.Second)
	a.Expire()

	if len(*got) != 1 {
		t.Fatalf("got %d episodes after expiry, want 1", len(*got))
	}
	if s := (*got)[0].Episode.Status; s != record.StatusTimedOut {
		t.Errorf("status = %q, want %q", s, record.StatusTimedOut)
	}
}

// F-4.6: the fidelity flag must not claim a gap for a step kind the episode
// never contained, or the filter it exists to serve becomes useless.
func TestFidelityFlags(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk)

	withParams := span("s", "a", "", 1_000_000, record.KindLLM, "hi")
	withParams.Step.Params = &record.Params{Temperature: ptr(0.7)}
	a.Add(withParams)
	a.Add(terminal(span("s", "b", "", 2_000_000, record.KindLLM, "bye")))
	a.Flush(record.StatusTimedOut)

	fid := (*got)[0].Episode.Fidelity
	if fid == nil {
		t.Fatal("fidelity is nil")
	}
	// One of the two LLM steps lacks params, so the episode does not have
	// params. A partially instrumented episode is not trainable.
	if fid.HasParams {
		t.Error("HasParams = true, but one LLM step reported no params")
	}
	// No tool steps at all, so tool versions are not a gap this episode has.
	if fid.HasToolVersions {
		t.Error("HasToolVersions = true for an episode with no tool steps")
	}
}

// Every record names the source that produced it (F-1.6).
func TestSourceAttributed(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk)
	a.Add(terminal(span("s", "a", "", 1_000_000, record.KindLLM, "x")))
	a.Flush(record.StatusTimedOut)

	if src := (*got)[0].Episode.Source; src != "otlp" {
		t.Errorf("source = %q, want %q", src, "otlp")
	}
}

// A terminal marker closes the episode as complete once the settle period
// elapses — not immediately, and not only at window expiry.
//
// This is the behaviour that reconciles F-3.2 with F-3.3. The regression it
// guards against is a span arriving microseconds after the terminal marker and
// being split into a second episode.
func TestTerminalClosesAfterSettleAndAbsorbsLateSpan(t *testing.T) {
	clk := &fixedClock{t: time.Unix(1_757_000_000, 0).UTC()}
	a, got := newTestAssembler(t, clk, func(o *Options) {
		o.SettleAfterTerminal = 10 * time.Second
		o.Window = 5 * time.Minute
	})

	a.Add(terminal(span("s", "a", "", 1_000_000, record.KindLLM, "done")))
	if len(*got) != 0 {
		t.Fatalf("emitted immediately on terminal; settle period was not honoured")
	}

	// A span that was in flight when the terminal marker landed.
	clk.advance(2 * time.Second)
	a.Add(span("s", "b", "a", 500_000, record.KindTool, "still arriving"))
	if len(*got) != 0 {
		t.Fatalf("emitted during settle window")
	}

	clk.advance(15 * time.Second)
	a.Expire()

	if len(*got) != 1 {
		t.Fatalf("got %d episodes, want 1 — the late span should have joined, not split", len(*got))
	}
	ep := (*got)[0]
	if ep.Episode.Status != record.StatusComplete {
		t.Errorf("status = %q, want %q", ep.Episode.Status, record.StatusComplete)
	}
	if len(ep.Steps) != 2 {
		t.Fatalf("got %d steps, want 2 (the late span must be absorbed)", len(ep.Steps))
	}
	// Ordering is by timestamp, so the late-arriving earlier span sorts first.
	if *ep.Steps[0].ContentInline != "still arriving" {
		t.Errorf("step order is arrival-dependent: got %q first", *ep.Steps[0].ContentInline)
	}
}
