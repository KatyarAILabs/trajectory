# Trajectory

Capture agent trajectories at a fidelity sufficient for offline replay and
scoring, redact what must never leave the perimeter, and land it in cheap,
durable, queryable storage — with no opinion about what reads it afterwards.

> **Status: pre-alpha.** The schema is drafted and the contract is enforced in
> CI, but no collector exists yet. See [the build plan](docs/PLAN.md).

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
| `collector/` | Sources, processors, sinks, buffer, CLI |
| `otelcol/` | OpenTelemetry Collector components and builder manifest |
| `importers/` | langfuse, phoenix, langsmith, jsonl, parquet |
| `mappings/` | Declarative source mappings (litellm, portkey, helicone) |
| `sdk/` | Python and TypeScript emit + read |
| `deploy/helm/` | Chart, network policy, RBAC |

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

## Licence

Apache 2.0. Contributions are under [DCO](CONTRIBUTING.md) sign-off, not a CLA.
