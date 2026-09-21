# The trajectory schema

Schema version: see [`VERSION`](VERSION). It is mirrored by
`internal/version.Schema`, and a test fails if the two drift.

## What is the source of truth

```
spec/proto/trajectory/v1/*.proto      ← edit this
        │
        ├── protoc ──▶ gen/go/            the wire contract
        ├── gen-jsonschema ──▶ jsonschema/   the published JSON Schema (F-10.3)
        └── (by hand, parity-tested) ──▶ pkg/record/   the storage contract
```

The proto is the only file a human edits. `gen/go/` and `spec/jsonschema/` are
generated and CI fails if they are stale.

**`pkg/record` is the one exception, and it is deliberate.** Generated protobuf
structs carry internal state fields and pointer semantics that make poor
Parquet rows, so the Parquet row types are written by hand. That separation is
only safe because it cannot silently become a divergence:
`pkg/record/proto_parity_test.go` walks every proto descriptor and fails if the
two disagree on field names, order or cardinality.

## Compatibility promise

Semver, **additive only within a major version** (F-10.1). A reader written
against 0.1 reads 0.2 without modification.

`schema_version` is stamped in two places (F-10.2): on every row, and in the
Parquet file's key-value metadata — so a reader can check compatibility
without decoding any data.

Enforcement is mechanical, not conventional:

| Guard | Catches |
|---|---|
| `pkg/record/testdata/schema/*.txt` | Any change to physical column order, type, repetition or logical annotation |
| `proto_parity_test.go` | A field added to the wire contract but not the storage contract, or vice versa |
| `make verify-generated` | A `.proto` edit committed without its regenerated artifacts |

## Reserved surface

`outcomes`, `labels` and `rewards` are defined in
[`reserved.proto`](proto/trajectory/v1/reserved.proto) and their tables are
created empty. **The collector never writes them in v1.** Joining to outcomes
and computing rewards are declared non-goals (spec §2.2 N-2, N-3).

They are defined now because it costs nothing, and it means the join later
arrives as an additive change rather than a major version bump.
`TestReservedTablesAreNotWritten` fails if one is added to the written set.

## Vocabularies

Enums have two spellings and this is worth understanding before you write a
producer.

- **In Parquet**, enums are stored as the lowercase spec §7 values —
  `complete`, `timed_out`, `llm`, `retrieval` — so a file is readable in DuckDB
  with no mapping table.
- **On the JSON wire**, protojson's own names (`STATUS_COMPLETE`) are what a
  standard protobuf JSON encoder emits.

The published JSON Schema lists **both**, so either is accepted. The conversion
between them lives in exactly one place, `pkg/record/enums.go`.

Both spellings are accepted on the native JSON API, in `collector/wire`, which
is tested against the published schema's enum lists.

## Timestamps

Microseconds since the Unix epoch, UTC-adjusted.

Producer clocks (`started_at`, `ended_at`) and the collector clock
(`received_at`) are both recorded. Skew is *observed, never corrected* (§20) —
`cc_clock_skew_seconds` is the metric that makes it visible.

64-bit integers appear in the JSON Schema as `["string", "integer"]` because
protojson encodes them as strings to survive JavaScript's 2^53 limit. For
microsecond timestamps this stops being academic in the year 2255, but the
encoding is what it is today.

## Payload placement

A step's payload is in exactly one of two places:

- `content_inline` when it is below the sink's blob threshold
- `content_ref` — the sha256 of a content-addressed blob — when it is above

Deduplication is the point: a system prompt repeated across millions of
episodes is stored once (§7.3).

## Payload shape

`content_inline` and the blob a `content_ref` points at hold a **JSON
envelope**, not a bare string:

```json
{"input": …, "output": …}     // llm, retrieval, human, other
{"args":  …, "result": …}     // tool
```

A value that is itself JSON is embedded as JSON so a path reaches into it;
anything else is embedded as the exact string the producer sent. Nothing is
truncated or reformatted (F-4.1).

The structure is not decoration. A replayer needs the prompt and a scorer needs
the completion, and they have to stay separable; and entity extraction
addresses these with JSONPath from config (`$.args.id`), which requires args and
result to remain addressable rather than flattened together.

## What `fidelity.has_params` does and does not mean

It means the source reported generation parameters. It does **not** mean they
are sufficient to reproduce the call.

A seed is what makes a sampled completion reproducible, and most tracing
exports drop it — so an episode can carry `has_params: true` and still not
replay exactly. `cc replay` says so explicitly when it sees params without a
seed. A consumer filtering for exactly-reproducible episodes should check for a
seed on the LLM steps, not only the flag.
