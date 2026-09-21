// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package record defines the Parquet row types for the trajectory schema.
//
// These are the storage contract. The wire contract lives in spec/proto and is
// generated into gen/go. The two are deliberately separate types: generated
// protobuf structs carry internal state fields and pointer semantics that make
// poor Parquet rows. proto_parity_test.go walks the proto descriptors and fails
// if the two ever drift, so the separation cannot become a divergence.
//
// Field order here is the physical column order in the file and is asserted by
// the golden schema test. Within a major version, fields may only be appended
// (F-10.1).
package record

// Error is a terminal error on an episode or a step.
type Error struct {
	Type    string `parquet:"type"`
	Message string `parquet:"message"`
}

// EntityKey is a business key extracted from tool arguments or results (F-6).
// The value is tokenized: extraction runs after redaction (F-6.4).
type EntityKey struct {
	Name  string `parquet:"name"`
	Value string `parquet:"value"`
}

// Fidelity records which replay-critical fields the producer could supply
// (F-4.6), so a consumer can filter to trainable episodes in one predicate.
type Fidelity struct {
	HasParams       bool `parquet:"has_params"`
	HasTokenSpans   bool `parquet:"has_token_spans"`
	HasToolVersions bool `parquet:"has_tool_versions"`
}

// Params are the generation parameters needed to re-execute an LLM step
// (F-4.2). A nil field means the source did not report it, not that a default
// applied — the distinction matters to a replayer.
type Params struct {
	Temperature *float64 `parquet:"temperature,optional"`
	TopP        *float64 `parquet:"top_p,optional"`
	MaxTokens   *int32   `parquet:"max_tokens,optional"`
	Seed        *int64   `parquet:"seed,optional"`
	Stop        []string `parquet:"stop,list"`
}

// TokenCounts are reported by the source when available (F-4.3).
type TokenCounts struct {
	Input     *int64 `parquet:"input,optional"`
	Output    *int64 `parquet:"output,optional"`
	Cached    *int64 `parquet:"cached,optional"`
	Reasoning *int64 `parquet:"reasoning,optional"`
}

// TokenSpan marks a payload range as model-generated versus tool/context
// output (F-4.4).
type TokenSpan struct {
	Start     int64  `parquet:"start"`
	End       int64  `parquet:"end"`
	Trainable string `parquet:"trainable,enum"`
}

// Episode is one complete agent trajectory and the unit of record.
//
// Timestamps are microseconds since the Unix epoch. Producer clocks
// (StartedAt, EndedAt) and the collector clock (ReceivedAt) are both recorded
// so skew is observable rather than corrected (§20).
type Episode struct {
	EpisodeID string `parquet:"episode_id"`
	Tenant    string `parquet:"tenant"`
	// N rollouts of one task instance. Producer-supplied only (F-3.6).
	GroupID *string `parquet:"group_id,optional"`
	// Partition key. Free-form, producer-supplied.
	TaskType *string `parquet:"task_type,optional"`
	// The configured source that produced this record (F-1.6).
	Source string `parquet:"source"`
	// Instrumentation identity, so fidelity gaps stay attributable (F-2.3).
	Instrumentation        *string `parquet:"instrumentation,optional"`
	InstrumentationVersion *string `parquet:"instrumentation_version,optional"`

	EntityKeys []EntityKey `parquet:"entity_keys,list"`
	Status     string      `parquet:"status,enum"`
	Fidelity   *Fidelity   `parquet:"fidelity,optional"`
	// The sampling rule that kept this episode, so a consumer can reason
	// about bias (F-7.4).
	SampledBy *string `parquet:"sampled_by,optional"`

	StartedAt  int64  `parquet:"started_at,timestamp(microsecond)"`
	EndedAt    *int64 `parquet:"ended_at,optional,timestamp(microsecond)"`
	ReceivedAt int64  `parquet:"received_at,timestamp(microsecond)"`

	StepCount     int32  `parquet:"step_count"`
	Error         *Error `parquet:"error,optional"`
	SchemaVersion string `parquet:"schema_version"`
	// Unrecognised episode-level attributes. Normalisation is never lossy
	// (F-2.2).
	Raw map[string]string `parquet:"raw,optional"`
}

// Step is one action inside an episode.
//
// Exactly one of ContentRef and ContentInline carries the payload: content
// above the sink's blob threshold is externalised to a content-addressed blob
// (F-9.3), otherwise it is stored inline.
type Step struct {
	EpisodeID string `parquet:"episode_id"`
	// Canonical linear order.
	StepIdx int32 `parquet:"step_idx"`
	// Tree structure; nil for a root step. Retries and abandoned branches
	// survive as a tree alongside the linear ordering (F-3.4).
	ParentIdx *int32 `parquet:"parent_idx,optional"`
	// Retry counter, 0-based.
	Attempt int32   `parquet:"attempt"`
	Kind    string  `parquet:"kind,enum"`
	Role    *string `parquet:"role,optional"`

	// sha256 of the blob holding this payload, when externalised.
	ContentRef *string `parquet:"content_ref,optional"`
	// The payload itself, when below the blob threshold.
	ContentInline *string `parquet:"content_inline,optional"`
	// Set when the payload exceeded the hard maximum and was cut (F-9.7).
	// The record is kept rather than failed.
	Truncated bool `parquet:"truncated"`

	ToolName    *string `parquet:"tool_name,optional"`
	ToolVersion *string `parquet:"tool_version,optional"`
	// Stable hash of normalised arguments.
	ArgsHash *string `parquet:"args_hash,optional"`
	Model    *string `parquet:"model,optional"`
	Provider *string `parquet:"provider,optional"`

	Params      *Params      `parquet:"params,optional"`
	TokenCounts *TokenCounts `parquet:"token_counts,optional"`
	TokenSpans  []TokenSpan  `parquet:"token_spans,list"`
	Trainable   string       `parquet:"trainable,enum"`
	LogprobsRef *string      `parquet:"logprobs_ref,optional"`

	FinishReason *string           `parquet:"finish_reason,optional"`
	StartedAt    int64             `parquet:"started_at,timestamp(microsecond)"`
	LatencyMs    *int32            `parquet:"latency_ms,optional"`
	Error        *Error            `parquet:"error,optional"`
	Raw          map[string]string `parquet:"raw,optional"`
	// CostUSD is reported cost when the source provides it (F-4.3). It is
	// the last column on purpose: fields are only ever appended (F-10.1).
	CostUSD *float64 `parquet:"cost_usd,optional"`
}

// Blob is a payload externalised above the inline threshold, addressed by the
// sha256 of its content.
//
// Deduplication is the point: a system prompt repeated across millions of
// episodes is stored once (§7.3).
type Blob struct {
	// Primary key, and the object key under blobs/sha256/<aa>/<bb>/.
	Sha256      string `parquet:"sha256"`
	Bytes       int64  `parquet:"bytes"`
	ContentType string `parquet:"content_type"`
	Encoding    string `parquet:"encoding,enum"`
	FirstSeenAt int64  `parquet:"first_seen_at,timestamp(microsecond)"`
}
