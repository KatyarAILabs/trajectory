// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package assemble groups normalised observations into episodes (F-3).
//
// The hard requirements are memory and determinism. The buffer is bounded: at
// capacity the oldest episode is emitted early marked "evicted" rather than
// growing without limit (F-3.7), because an OOM that takes down a production
// pod is the thing that gets a telemetry agent removed. And output must not
// depend on arrival order: the same spans shuffled and duplicated must produce
// byte-identical episodes (F-3 acceptance), which is why ordering is derived
// from span timestamps and the tree, never from the order spans arrived.
package assemble

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"sync"
	"time"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Emit receives a completed episode. It is called without the assembler lock
// held, so a slow sink does not block ingest.
type Emit func(*pipeline.Assembled)

// Options configure an Assembler.
type Options struct {
	Tenant string
	// Window is how long to tolerate out-of-order arrival (F-3.2).
	Window time.Duration
	// MaxInFlight bounds the number of episodes held open (F-3.7).
	MaxInFlight int
	// SettleAfterTerminal is how long to keep an episode open after its
	// terminal marker arrives.
	//
	// This knob is not in the spec, and it exists because F-3.2 and F-3.3
	// conflict without it: emitting the instant a terminal event arrives
	// gives an episode zero tolerance for out-of-order arrival, so any
	// span still in flight lands after emit and starts a bogus second
	// episode. Waiting the full window instead would honour F-3.2 but
	// blow the p95 < 60s ingest-to-durable target (§13) for every
	// well-behaved producer.
	//
	// A short settle satisfies both: out-of-order spans are absorbed, and
	// latency stays bounded well under the window.
	SettleAfterTerminal time.Duration
	// PatchMemory is how many recently emitted session keys to remember so
	// late spans can be attached to the right episode (F-3.5). Bounded,
	// because unbounded memory here would defeat bounding the assembly
	// buffer at all.
	PatchMemory int
	// Now is injectable so window behaviour is testable without sleeping.
	Now func() time.Time
	// NewID generates episode ids; injectable for deterministic tests.
	NewID func() string
}

// Assembler groups envelopes into episodes.
type Assembler struct {
	opts Options
	emit Emit

	mu sync.Mutex
	// open holds in-flight episodes keyed by session key, with an LRU-by-
	// creation list so eviction is O(1) and always picks the oldest.
	open  map[string]*list.Element
	order *list.List

	// emitted remembers recently closed sessions, so a span arriving after
	// emit can be attached to the episode it belongs to as a patch rather
	// than inventing a new episode from a fragment (F-3.5).
	//
	// It is bounded and evicted in insertion order: unbounded memory here
	// would defeat the whole point of bounding the assembly buffer.
	emitted     map[string]string
	emittedFIFO []string
	patchIdx    map[string]int32
	// emittedSpans remembers which span ids each emitted session already
	// carried, so a redelivery is recognised as a duplicate. Bounded with
	// emitted, by PatchMemory.
	emittedSpans map[string]map[string]bool

	stats Stats
}

// Stats are the counters this stage contributes to §11.
type Stats struct {
	Evicted   int64
	TimedOut  int64
	Completed int64
	// LateSpans counts spans that arrived for an episode already emitted
	// and were carried in a patch record (F-3.5).
	LateSpans int64
	// Patched counts patch episodes emitted (F-3.5).
	Patched    int64
	Duplicates int64
}

type inFlight struct {
	key       string
	episodeID string
	createdAt time.Time
	lastSeen  time.Time

	meta   pipeline.EpisodeMeta
	source string
	// terminalAt is when the terminal marker arrived; zero means it has
	// not. The episode closes SettleAfterTerminal later.
	terminalAt time.Time
	err        *record.Error

	// spans is keyed by span id so a duplicate delivery replaces rather
	// than appends. This is what makes a 10% duplicate rate invisible in
	// the output (F-3 acceptance).
	//
	// Deliberately no arrival counter: any state derived from the order
	// spans showed up in would leak into the output and break the shuffle
	// acceptance test.
	spans map[string]pipeline.Envelope

	startedAt int64
	endedAt   int64
}

// New creates an Assembler. emit is called for every completed episode.
func New(opts Options, emit Emit) *Assembler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Window <= 0 {
		opts.Window = 5 * time.Minute
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = 50000
	}
	if opts.SettleAfterTerminal < 0 {
		opts.SettleAfterTerminal = 0
	}
	if opts.PatchMemory <= 0 {
		opts.PatchMemory = 10000
	}
	return &Assembler{
		opts:    opts,
		emit:    emit,
		open:    make(map[string]*list.Element),
		order:   list.New(),
		emitted: make(map[string]string, opts.PatchMemory),
	}
}

// Add ingests one envelope. Episodes it completes are emitted before Add
// returns, with the lock released first.
func (a *Assembler) Add(env pipeline.Envelope) {
	var ready []*pipeline.Assembled

	a.mu.Lock()
	if env.SessionKey == "" {
		// Nothing to group by. Treat it as its own single-step episode
		// rather than discarding evidence.
		env.SessionKey = a.opts.NewID()
	}

	el, ok := a.open[env.SessionKey]
	if !ok {
		// The episode for this session already closed. Emit the late
		// span as an append-only patch rather than mutating the emitted
		// episode (F-3.5) or starting a bogus new one from a fragment.
		if epID, closed := a.emitted[env.SessionKey]; closed {
			// A redelivery of a span that was already emitted is a
			// duplicate, not a late arrival. Patching it in again would
			// duplicate the step, which is what makes a producer's retry
			// after a lost acknowledgement unsafe (§9.1, F-8.4).
			if a.emittedSpans[env.SessionKey][spanKey(env)] {
				a.stats.Duplicates++
				a.mu.Unlock()
				return
			}
			a.markEmittedSpan(env.SessionKey, spanKey(env))
			patch := a.patchFor(epID, env)
			a.stats.LateSpans++
			a.stats.Patched++
			a.mu.Unlock()
			a.emit(patch)
			return
		}

		// At capacity: make room by emitting the oldest episode early,
		// marked so a consumer knows it may be incomplete (F-3.7).
		if a.order.Len() >= a.opts.MaxInFlight {
			if oldest := a.order.Front(); oldest != nil {
				ep := oldest.Value.(*inFlight)
				ready = append(ready, a.closeLocked(ep, record.StatusEvicted))
				a.stats.Evicted++
			}
		}
		id := env.EpisodeID
		if id == "" {
			id = a.opts.NewID()
		}
		ep := &inFlight{
			key:       env.SessionKey,
			episodeID: id,
			createdAt: a.opts.Now(),
			spans:     map[string]pipeline.Envelope{},
			source:    env.Source,
		}
		el = a.order.PushBack(ep)
		a.open[env.SessionKey] = el
	}

	ep := el.Value.(*inFlight)
	ep.absorb(env, a.opts.Now(), &a.stats)

	// Closing is never immediate, even on a terminal marker: see
	// Options.SettleAfterTerminal. expireLocked decides what is due.
	ready = append(ready, a.expireLocked()...)
	a.mu.Unlock()

	for _, r := range ready {
		a.emit(r)
	}
}

// absorb folds one envelope into an in-flight episode.
func (f *inFlight) absorb(env pipeline.Envelope, now time.Time, stats *Stats) {
	f.lastSeen = now

	id := env.SpanID
	if id == "" {
		// A span with no id still has to be identified somehow. Hashing
		// its content rather than counting arrivals keeps both
		// properties that matter: a redelivery of the same span
		// collapses onto the same key, and the id does not depend on
		// when it showed up.
		id = syntheticSpanID(env)
	}
	// On a duplicate, keep the earliest sighting.
	//
	// For a genuine redelivery the copies are identical and the choice is
	// moot. It matters when the same observation is reported more than
	// once with different timestamps — a gateway rebuilds a tool step from
	// history on every later call, and only the first sighting is close to
	// when the tool actually ran. Keeping the earliest is also independent
	// of arrival order, which keeping the latest is not, so the
	// shuffle-determinism guarantee (F-3 acceptance) still holds.
	//
	// Only the stored span is kept; the rest of absorb still runs, because
	// a later copy can carry something the first did not — a terminal
	// marker most importantly.
	if prev, dup := f.spans[id]; dup {
		stats.Duplicates++
		if env.Step.StartedAt < prev.Step.StartedAt {
			f.spans[id] = env
		}
	} else {
		f.spans[id] = env
	}

	// Episode-level metadata: first non-empty value wins, so a producer
	// that sets task_type on only one span still gets it recorded.
	if f.meta.TaskType == "" {
		f.meta.TaskType = env.Meta.TaskType
	}
	if f.meta.GroupID == "" {
		f.meta.GroupID = env.Meta.GroupID
	}
	if f.meta.Instrumentation == "" {
		f.meta.Instrumentation = env.Meta.Instrumentation
	}
	if f.meta.InstrumentationVersion == "" {
		f.meta.InstrumentationVersion = env.Meta.InstrumentationVersion
	}
	for k, v := range env.Meta.Raw {
		if f.meta.Raw == nil {
			f.meta.Raw = map[string]string{}
		}
		f.meta.Raw[k] = v
	}

	if env.Error != nil && f.err == nil {
		f.err = env.Error
	}
	if env.Terminal && f.terminalAt.IsZero() {
		f.terminalAt = now
	}

	if s := env.Step.StartedAt; s > 0 && (f.startedAt == 0 || s < f.startedAt) {
		f.startedAt = s
	}
	end := env.Step.StartedAt
	if env.Step.LatencyMs != nil {
		end += int64(*env.Step.LatencyMs) * 1000
	}
	if end > f.endedAt {
		f.endedAt = end
	}
}

// Flush emits every in-flight episode. Called on graceful shutdown (F-11.5).
//
// An episode that already received its terminal marker is emitted as complete
// regardless of the fallback status: it genuinely finished, and it would be
// wrong to label it timed_out just because the process is stopping.
func (a *Assembler) Flush(fallback string) {
	a.mu.Lock()
	var ready []*pipeline.Assembled
	for a.order.Len() > 0 {
		ep := a.order.Front().Value.(*inFlight)
		status := fallback
		if !ep.terminalAt.IsZero() {
			status = record.StatusComplete
		}
		ready = append(ready, a.closeLocked(ep, status))
	}
	a.mu.Unlock()

	for _, r := range ready {
		a.emit(r)
	}
}

// Expire emits episodes whose window has passed (F-3.3). The service calls it
// on a ticker; Add also calls it so a purely event-driven test needs no timer.
func (a *Assembler) Expire() {
	a.mu.Lock()
	ready := a.expireLocked()
	a.mu.Unlock()

	for _, r := range ready {
		a.emit(r)
	}
}

// expireLocked closes every episode whose deadline has passed.
//
// Two deadlines coexist, so this is a full scan rather than a walk from the
// front of the creation-ordered list: a terminated episode created early may
// be due long before an idle one created later. The list stays ordered by
// creation because that is what eviction needs (F-3.7).
//
// The scan is O(open) per call. At the default max_in_flight of 50k on a
// one-second ticker that is cheap, but it is the first thing to revisit if
// that bound is raised.
func (a *Assembler) expireLocked() []*pipeline.Assembled {
	var ready []*pipeline.Assembled
	now := a.opts.Now()
	idleCutoff := now.Add(-a.opts.Window)

	for el := a.order.Front(); el != nil; {
		next := el.Next()
		ep := el.Value.(*inFlight)

		switch {
		case !ep.terminalAt.IsZero() && !now.Before(ep.terminalAt.Add(a.opts.SettleAfterTerminal)):
			ready = append(ready, a.closeLocked(ep, record.StatusComplete))
			a.stats.Completed++
		case ep.lastSeen.Before(idleCutoff) || ep.lastSeen.Equal(idleCutoff):
			ready = append(ready, a.closeLocked(ep, record.StatusTimedOut))
			a.stats.TimedOut++
		}
		el = next
	}
	return ready
}

// Stats returns a snapshot of the stage counters.
func (a *Assembler) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

// InFlight reports how many episodes are currently open (§11 gauge).
func (a *Assembler) InFlight() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.order.Len()
}

// syntheticSpanID derives a stable identity for a span whose producer sent no
// span id, from the fields that distinguish one observation from another.
func syntheticSpanID(env pipeline.Envelope) string {
	h := sha256.New()
	h.Write([]byte(strconv.FormatInt(env.Step.StartedAt, 10)))
	h.Write([]byte{0})
	h.Write([]byte(env.Step.Kind))
	h.Write([]byte{0})
	if env.Step.ContentInline != nil {
		h.Write([]byte(*env.Step.ContentInline))
	}
	h.Write([]byte{0})
	if env.Step.ToolName != nil {
		h.Write([]byte(*env.Step.ToolName))
	}
	return "syn-" + hex.EncodeToString(h.Sum(nil)[:12])
}

// spanKey is the identity used for deduplication: the producer's span id, or
// a content hash when it sent none.
func spanKey(env pipeline.Envelope) string {
	if env.SpanID != "" {
		return env.SpanID
	}
	return syntheticSpanID(env)
}

func (a *Assembler) markEmittedSpan(session, span string) {
	if a.emittedSpans == nil {
		a.emittedSpans = map[string]map[string]bool{}
	}
	set := a.emittedSpans[session]
	if set == nil {
		set = map[string]bool{}
		a.emittedSpans[session] = set
	}
	set[span] = true
}
