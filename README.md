# Trajectory

Capture agent trajectories at a fidelity sufficient for offline replay and
scoring, redact what must never leave the perimeter, and land it in cheap,
durable, queryable storage — with no opinion about what reads it afterwards.

> **Status: pre-alpha.** Every MUST in the capture path is implemented and
> tested, but there is still **no disk buffer, no S3 sink and no retry**:
> nothing survives a sink outage or a process kill mid-batch. That is Phase 3.
> See [the build plan](docs/PLAN.md).

## What works today

| | |
|---|---|
| **Ingest** | OTLP over HTTP and gRPC; a native JSON/protobuf episode API; four import formats |
| **Conventions** | OpenInference and OTel GenAI, selected per span by attribute presence, defined in [versioned data files](collector/normalize/conventions) rather than code |
| **Redaction** | Deny-by-default allow-lists, explicit deny, regex and JSONPath rules, deterministic HMAC tokenization, metadata-only mode, fail-closed quarantine |
| **Assembly** | Windowed, bounded, order-independent; retries stay branches; late spans become append-only patches |
| **Sampling** | Head by session, tail by CEL; never splits an episode |
| **Storage** | Parquet plus content-addressed deduplicated blobs, partitioned, manifest written last |
| **CLI** | `run`, `validate`, `import`, `redact --test`, `inspect`, `replay` |

Not yet: disk buffer, S3, retry/DLQ, TLS, Helm, OTel Collector components.

## Try it

```sh
make demo
```

That runs the collector against a local filesystem sink, sends it a recorded
OpenInference trajectory, and reconstructs that trajectory from the files on
disk with DuckDB — no cloud account, no credentials, no Kubernetes (UC-5).

What it shows: a four-step trajectory including a tool-call retry that stays a
branch rather than being flattened; a 12 KB prompt externalised to a
content-addressed blob; a seeded email present only as its HMAC token, with the
same token in both steps that touched it, so it is still joinable; and the
ticket id extracted into `entity_keys`.

## Why

Teams running production agents already emit telemetry, but what they keep is
shaped for humans reading a trace UI: prompts truncated to fit a display,
retries flattened into a linear list, seeds and tool versions dropped, payloads
on a 7–30 day retention clock. That telemetry can explain an incident. It
cannot be scored by a verifier written three months later, and it cannot be
replayed.

## What it is not

Deliberately, and contractually ([spec §2.2](docs/REQUIREMENTS.md)):

- **not** change data capture from systems of record
- **not** a joiner of trajectories to business outcomes
- **not** a computer of rewards, and it ships no verifier
- **not** a training-set exporter — readers consume Parquet directly
- **not** a UI, a search engine, or an alerting system
- **not** a general-purpose observability agent

The schema *defines* `outcomes`, `labels` and `rewards` and creates those
tables empty. It never writes them in v1. That costs nothing now and means the
join arrives later as an additive change rather than a major version bump that
breaks every reader.

## Layout

| Path | Contents |
|---|---|
| `spec/` | Schema: protobuf source of truth, generated JSON Schema, conformance tests, sample dataset |
| `pkg/record/` | Parquet row types — the storage contract |
| `gen/go/` | Generated Go types — the wire contract. Do not edit by hand. |
| `collector/` | Sources, processors, sinks, buffer |
| `cmd/cc/` | The CLI |
| `tools/` | Code generation, the licence gate, the traffic generator |
| `otelcol/` | OpenTelemetry Collector components and builder manifest |
| `importers/` | langfuse, phoenix, langsmith, jsonl, parquet |
| `mappings/` | Declarative source mappings (litellm, portkey, helicone) |
| `sdk/` | Python and TypeScript emit + read |
| `deploy/helm/` | Chart, network policy, RBAC |

## Evaluate it against your own data

No deployment, no instrumentation change, no cloud account (UC-4):

```sh
cc import -config examples/local.yaml -from export.json langfuse
cc replay -lake ./var/lake <episode-id>
```

Formats: `langfuse`, `langsmith`, `phoenix`, `jsonl`, `native`. The import path
runs the same normalise → assemble → redact → extract → sink chain as live
traffic, so what you see is what the live path will produce.

It also reports **entity key coverage** — the share of episodes carrying a
business key. That is the leading indicator that a future join will fail, and
an import is the earliest possible moment to find out, rather than months later
when someone tries the join.

## Check a redaction policy before it runs

```sh
cc redact -config examples/local.yaml --test sample.json
```

Prints rule ids, field paths and match counts. Never a payload value — for the
same reason the redaction manifest does not (F-5.4).

## Developing

```sh
make tools      # install protoc-gen-go and go-licenses into ./bin
make            # generate, build, test, lint
```

Two guards are load-bearing and will fail your PR:

- **`pkg/record/testdata/schema/*.txt`** pins the physical Parquet layout. Any
  change to column order, type or repetition fails the golden test. Within a
  major version the schema is additive only; regenerate deliberately with
  `make golden` and expect the diff to be reviewed.
- **`make licences`** rejects a non-permissive transitive dependency. It runs
  from the first commit so one can never become established.
- **`make acceptance`** is DoD-1, the end-to-end test that defines the walking
  skeleton as working. It asserts against files on disk rather than the
  pipeline'"'"'s own accounting — including that a seeded secret appears in no
  file anywhere under the sink root.

## Licence

Apache 2.0. Contributions are under [DCO](CONTRIBUTING.md) sign-off, not a CLA.
