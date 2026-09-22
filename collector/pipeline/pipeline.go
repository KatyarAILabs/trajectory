// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pipeline defines the stage boundaries every component plugs into
// (F-13.1): adding a source, processor or sink is a self-contained package
// that implements one of these interfaces and knows nothing about the others.
//
// Data changes shape exactly once, at assembly. Before it, an Envelope is one
// normalised observation. After it, an Assembled is one episode with its steps.
// The assembler is deliberately not a Processor because it is the only stage
// that changes cardinality.
package pipeline

import (
	"context"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// EpisodeMeta is episode-level context carried on an individual observation.
// Producers repeat it on every span; the assembler takes the first non-empty
// value it sees for each field.
type EpisodeMeta struct {
	TaskType               string
	GroupID                string
	Instrumentation        string
	InstrumentationVersion string
	// Raw holds unrecognised episode-level attributes. Normalisation is
	// never lossy (F-2.2).
	Raw map[string]string
}

// Envelope is one normalised observation before assembly.
//
// Payloads travel in Step.ContentInline through the whole pipeline regardless
// of size. Blob externalisation is a sink concern (F-9.3), which is what
// guarantees redaction sees the complete payload before anything is written
// (F-5.3).
type Envelope struct {
	// SessionKey groups observations into an episode (F-3.1). Empty means
	// the producer supplied nothing usable and the record is unassemblable.
	SessionKey string
	// EpisodeID, when a producer supplies one, becomes the stored
	// episode_id. It is what makes the native API idempotent on episode_id
	// (§9.1) and lets a producer look up its own episode. Empty means the
	// collector assigns one, as it must for OTLP, where no convention names
	// an episode.
	EpisodeID string
	// Source is the configured source that produced this (F-1.6).
	Source string
	// SpanID and ParentSpanID preserve the tree so retries and abandoned
	// branches survive assembly (F-3.4).
	SpanID       string
	ParentSpanID string
	// Terminal marks an explicit end-of-episode event (F-3.3).
	Terminal bool
	// Error is an episode-level terminal error, when the span carries one.
	Error *record.Error

	Meta EpisodeMeta
	Step record.Step
}

// Assembled is one episode with its steps, as it flows through the processors
// and into a sink.
//
// Steps are in canonical linear order and their StepIdx and ParentIdx are
// already resolved.
type Assembled struct {
	Episode record.Episode
	Steps   []record.Step
}

// Next is how a source hands an observation to the rest of the pipeline.
// Returning an error means the observation was not accepted; a source should
// surface that to its producer (for example as a 503) rather than dropping it.
type Next func(context.Context, Envelope) error

// A Source accepts telemetry from producers and emits normalised Envelopes.
//
// Every source is independently named, and that name is attributed onto every
// record it produces (F-1.6).
type Source interface {
	Name() string
	// Start blocks until ctx is cancelled or the source fails.
	Start(ctx context.Context, next Next) error
	Shutdown(ctx context.Context) error
}

// A Processor transforms an assembled episode in place.
//
// Returning an error quarantines the episode rather than passing it on. This
// is what fail-closed means in practice (F-5.5): a policy that cannot be
// evaluated must not result in an unredacted record reaching a sink.
//
// Processor order is significant and set by the service, not by config:
// redaction runs before extraction so that extracted keys are tokenized
// (F-6.4), and both run before any sink write (F-5.3).
type Processor interface {
	Name() string
	Process(ctx context.Context, ep *Assembled) error
}

// A Sink durably lands assembled episodes.
//
// Write may buffer. Nothing is guaranteed visible to a reader until Flush
// returns, and a reader must never observe a partially written batch (F-9.4).
type Sink interface {
	Name() string
	Write(ctx context.Context, eps []*Assembled) error
	Flush(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
