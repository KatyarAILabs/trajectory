# Trajectory

Capture agent trajectories at a fidelity sufficient for offline replay and
scoring, redact what must never leave the perimeter, and land it in cheap,
durable, queryable storage — with no opinion about what reads it afterwards.

> **Status: MVP complete.** Every MUST and every SHOULD in the spec is
> implemented and tested, and every deployment artifact has been exercised for
> real: the container image run read-only, the Helm chart installed on a
> Kubernetes cluster and restarted, the OTel Collector distribution built and
> run. What remains is listed under *Not done* below. It has not yet run
> unattended in a design partner's cluster for a week, which is the bar §16 sets
> for M3.

## What works today

| | |
|---|---|
| **Ingest** | OTLP over HTTP and gRPC; native episode and span APIs in JSON and protobuf; gateway webhooks (LiteLLM, Portkey, Helicone); JSONL and container-stdout tailing; five import formats |
| **SDKs** | Python and TypeScript, zero dependencies — the only path that captures token spans, trainable masks, seeds and tool versions |
| **Conventions** | OpenInference and OTel GenAI, selected per span by attribute presence, defined in [versioned data files](collector/normalize/conventions) rather than code |
| **Redaction** | Deny-by-default allow-lists, explicit deny, regex and JSONPath rules, deterministic HMAC tokenization, per-source policies, an external entity detector hook, metadata-only mode, fail-closed quarantine |
| **Assembly** | Windowed, bounded, order-independent; retries stay branches; late spans become append-only patches |
| **Sampling** | Head by session, tail by CEL, per-source rate limits answering 429; never splits an episode |
| **Durability** | Disk buffer surviving sink outages and unclean restarts, optionally encrypted at rest; exponential backoff with jitter; dead-lettering; backpressure; episodes lost to a crash are counted |
| **Operations** | Hot reload of policy on SIGHUP; full-pipeline dry run (`cc validate -sample`); the collector's own traces over OTLP |
| **Storage** | Parquet plus content-addressed deduplicated blobs, on local FS or any S3-compatible store, partitioned, manifest written last |
| **Security** | TLS and mTLS, per-source tokens, customer-managed encryption keys, distroless non-root image, a CI lint that forbids payloads in logs |
| **Deployment** | Helm chart with NetworkPolicy and per-replica buffer volumes; OTel Collector exporter; signed releases with SBOM |
| **Outcome join** | Business outcomes via `POST /v1/outcomes` or file; as-of join with watermarks and last-touch attribution; rules-based rewards; training export as trajectories, chat or preference pairs — see [docs/OUTCOMES.md](docs/OUTCOMES.md) |
| **CLI** | `run`, `validate [-sample]`, `import`, `redact --test`, `inspect`, `replay`, `conform`, `outcomes`, `join`, `score`, `export` |

Measured, not claimed — reproduce with `make loadtest`:

| §13 target | Measured |
|---|---|
| 5,000 spans/s | **6,836 spans/s** (with delivery keeping pace) |
| < 100 MiB idle RSS | **26 MiB** idle, 66 MiB under sustained load |
| Bounded memory | goroutines flat at 15 across a 25s soak |

### Not done

| | Why |
|---|---|
| Kafka source (F-1.5) | MAY in the spec. The source interface makes it a self-contained package. |
| Iceberg registration (F-9.8) | Deferred to v1.1 by Q-3. The manifest already carries what a catalog needs. |
| Durable assembly state (F-3.8) | Best-effort in v1 by Q-8. Loss on an unclean stop is bounded and counted. |
| 24-hour soak at 2× load (§15) | Needs CI hardware. Only a 25-second run has been done. |
| A week unattended at a design partner (M3) | Needs a design partner. |
| Published packages | Nothing is on PyPI, npm or a container registry yet. |

## Try it

```sh
make demo            # capture a trajectory and reconstruct it with DuckDB
make demo-outcomes   # then label it with business outcomes, score it, export training data
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
- **not** a CDC system — outcomes are posted to it, never pulled
- **not** a scorer on the capture path — rewards are computed offline, over a
  lake, by `cc score`
- **not** a UI, a search engine, or an alerting system
- **not** a general-purpose observability agent

The v0.1 spec reserved `outcomes`, `labels` and `rewards` so the outcome join
could arrive later as an additive change. In v0.2 it did: `outcomes` and
`rewards` are written, `labels` stays reserved, and nothing a v0.1 reader
understood changed.

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

## Operating it

Two metrics belong on a dashboard from the first day:

**`cc_buffer_bytes`** — the undelivered backlog. Rising steadily means the sink
is not keeping up; flat and high means it is unreachable. It reports the
backlog, not disk usage, so it falls as delivery progresses even before
segments are reclaimed.

**`cc_episodes_without_entity_keys_ratio`** — the share of episodes carrying no
business key. Those episodes can never be joined to an outcome. Finding this
out months later, when someone finally tries the join, is the failure the
metric exists to prevent.

### Things that will bite you

**One buffer directory, one collector.** Two processes sharing it interleave
partial records and corrupt the log. The collector refuses to start rather than
allow it, which is why the Helm chart is a StatefulSet with per-replica
volumes.

**Rotating the HMAC key breaks joinability** of new records against old ones. A
`key_id` is recorded so a rotation is detectable rather than silent, but the
break is real. Treat the key as long-lived.

**`buffer.max_bytes` is how long an outage you can absorb.** Roughly your
bytes/sec times your tolerable outage. Past it, the oldest data is dropped and
counted — never silently.

**Head and tail sampling compose multiplicatively.** `head: 0.1` with
`otherwise_rate: 0.1` keeps 1%, not 10%. `cc validate` refuses that combination
rather than letting you find out from the bill.
