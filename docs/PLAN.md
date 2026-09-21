# CC Collector — Build Plan

| | |
|---|---|
| **Companion to** | `cc-collector-requirements.md` v0.1-draft |
| **Status** | Proposed. Sequencing and library choices open for revision; scope inherits the spec. |
| **Date** | 2026-09-20 |

## 0. Decisions taken

These resolve §21 open questions enough to start. Each is revisitable; none is load-bearing on the others except Q-1.

| Q | Decision | Consequence for this plan |
|---|---|---|
| Q-1 | **Go** | OTLP receiver, Parquet writer and S3 client are all off-the-shelf and Apache-2.0. Static binary, distroless, dual-arch come free. PII entity detection (F-5.8) stays an external service. |
| Q-2 | **Both** — standalone binary first, OTel Collector components in Phase 4 | Pipeline stages must be written as interfaces from day one (F-13.1) so the same processors compile into both. |
| Q-3 | **Parquet + manifest in v1**; Iceberg in v1.1 | Sink writes a manifest last, atomically (F-9.4/9.6). No catalog dependency. |
| Q-5 | **Single-tenant per deployment**; `tenant` is a schema field | No per-request tenant resolution, no cross-tenant isolation code. Quotas (F-7.3) are per-source. |
| Q-8 | **Best-effort assembly state in v1** + a metric for episodes lost to restart | In-flight episodes live in memory, bounded per F-3.7. Durable assembly deferred. |
| Q-9 | **Retention out of scope** — bucket lifecycle rules, documented policy | No compaction or expiry code. |
| Q-4 | **Regex + allow-list in core**; entity detection external | No Python dependency, no model in the binary. |
| Q-7 | Neutral OSS name, Apache 2.0, DCO | **Standalone repo**, separate from this one. Name still to be chosen — blocks the repo's creation, nothing else. |

**Build order chosen:** thin end-to-end vertical slice before breadth in any stage. This departs from the spec's M0→M1→M2→M3 ordering. Rationale and the risk it carries are in §1.

## 1. Strategy

The spec's milestones widen one stage at a time (spec, then import, then live ingest). We instead drive one narrow path through **every** stage first, then widen.

**Why:** the integration seams in this system are where the cost is — redaction-before-buffer (F-5.3), atomic visibility (F-9.4), blob externalisation interacting with content-addressed dedup (F-9.3), and the immutability contract between assembly and sink (F-3.5). Widening a stage in isolation defers all of those to one big-bang integration.

**The risk this creates:** a walking skeleton silently freezes the schema by implementation rather than by contract, which is exactly what F-10 exists to prevent. **Mitigation:** Phase 0 lands the schema as a versioned artifact (protobuf → generated Go) *before* any pipeline code, so the skeleton is written against a contract even though the conformance suite arrives later.

**Second departure worth naming:** the spec makes M1 (import path) the first thing a design partner touches, because it proves value with zero deployment. The slice ordering below preserves that — `cc import` is Phase 2's first item, roughly a day's work once the skeleton exists, since it is a file reader in front of the same normalise→redact→sink chain.

## 2. Phase 0 — Repo and contract

No pipeline code in this phase.

1. **Repo** — new standalone repo, Apache 2.0, DCO bot, layout per spec §17. `CODEOWNERS`, issue templates, `SECURITY.md`. Blocked only on the name (Q-7).
2. **Schema as one source of truth** — write `spec/proto/*.proto` for `episodes`, `steps`, `blobs`, plus the three reserved tables (§7.4) defined and never written. Generate Go types from it. Derive JSON Schema from the same proto rather than hand-maintaining a second copy — two hand-written schemas will drift.
3. **Parquet mapping** — the Go structs carry `parquet:` tags; assert the physical Parquet schema in a golden test so a struct edit that changes the file layout fails CI. This is the practical enforcement of F-10.1's additive-only rule.
4. **`schema_version` plumbing** — constant stamped on every record and in Parquet file key-value metadata (F-10.2).
5. **CI skeleton** — build, test, `go vet`, licence gate (`go-licenses check` against an allow-list), SBOM generation. The licence gate exists from commit one so a non-permissive dependency can never get established.

**Exit:** `spec/` publishes a versioned schema; a generated Go type round-trips to Parquet and back; CI is green and blocks a GPL transitive dep.

## 3. Phase 1 — Walking skeleton

The narrowest path that touches every stage. **One** option at each stage.

```
OTLP/HTTP ─▶ normalise ─▶ assemble ─▶ redact ─▶ extract ─▶ Parquet+blobs ─▶ local FS
(OpenInference)  (subset)   (session id)  (1 rule)  (1 JSONPath)              (+ manifest)
```

| Stage | In the skeleton | Deliberately not yet |
|---|---|---|
| Ingest | OTLP over **HTTP/protobuf** only, one listener | gRPC, native `/v1/episodes`, webhooks, file tail, Kafka |
| Normalise | OpenInference attribute subset → canonical step; everything unrecognised into `raw` (F-2.2) | OTel GenAI conventions, declarative mapping files |
| Assemble | Group by `session.id`, fixed window, emit on terminal event or expiry, bounded in-flight with eviction (F-3.1/3.2/3.3/3.7) | Patch records (F-3.5), fallback key chains, group_id |
| Redact | Deny-by-default allow-list + one HMAC tokenize rule; runs before any write (F-5.3) | CEL/JSONPath policies, per-source override, metadata-only mode |
| Extract | One per-tool JSONPath → `entity_keys[]`, after redaction (F-6.4) | CEL expressions, coverage metric |
| Sink | Parquet to local filesystem, blobs content-addressed with two-level fan-out, manifest written **last** (F-9.4/9.6) | S3, partitioning beyond `dt`, file rolling, compression tuning |
| Buffer | None — synchronous write | Disk buffer, backpressure, retry, DLQ |
| CLI | `cc run`, `cc validate` (strict YAML, unknown key = error, F-11.6) | import, inspect, redact --test, replay |
| Telemetry | `/healthz`, `/metrics`, and the handful of counters the slice can move | Self-traces, full metric set |

**Acceptance (DoD-1) — the single test that defines this phase done:** a recorded OpenInference span stream, posted to the OTLP/HTTP endpoint, produces `episodes/` and `steps/` Parquet plus externalised blobs on local disk; a DuckDB query joins them and reconstructs the episode in correct order with payloads verbatim and a seeded email present only as its HMAC token. Committed as a golden test.

**Interfaces to get right here, because everything later plugs into them** (F-13.1): `Source`, `Processor`, `Sink`, and the record batch that flows between them. Worth a design review before the phase, not after.

## 4. Phase 2 — Widen to MUST coverage

Order within the phase is roughly by partner value.

1. **`cc import`** (F-14.3, UC-4) — one export format first, reusing normalise→redact→sink behind a file reader. This is the first artifact a design partner can evaluate with zero deployment. Then the remaining importers.
2. **Ingest breadth** — OTLP/gRPC, native `POST /v1/episodes` and `/v1/spans`, oversize rejection with a metric (F-1.7), per-source naming and attribution (F-1.6).
3. **Normalisation breadth** — OTel GenAI conventions; move mapping tables out of code into versioned data files (F-2.4); `kind=other` retention (F-2.5).
4. **Replay fidelity** (F-4) — params, token counts, token spans, tool versions, and the per-episode `fidelity` struct (F-4.6). The fidelity flag is what lets a consumer filter to trainable episodes without re-deriving it per field.
5. **Redaction engine** (F-5) — JSONPath/CEL policies, fail-closed quarantine, redaction manifest with counts but no values, `cc redact --test`. Acceptance is the seeded-PII corpus: zero leaks, manifest counts match the seed exactly.
6. **Assembly hardening** — fallback session key chain, patch records for late spans (F-3.5), `group_id`. Acceptance is the shuffle/duplicate harness: shuffled stream with 10% duplicates produces byte-identical episodes.
7. **Sampling** (F-7) — head by rate, tail rules in CEL, `sampled_by` recorded. The invariant to test explicitly: sampling never splits an episode.

## 5. Phase 3 — Durability, storage, security

This is where the NFRs in §13 get met rather than asserted.

1. **Disk buffer** (F-8) — WAL-backed, bounded by bytes *and* age, backpressure at threshold, exponential backoff with jitter, DLQ after N attempts, stable idempotency key. Redaction already ran, so the buffer holds nothing unredacted.
2. **S3 sink** (F-9) — S3-compatible (S3/GCS/Azure/R2/MinIO), configurable partitioning, file rolling toward 128–512 MB, zstd, SSE with customer-managed keys, blob dedup counters.
3. **Security** (F-12) — TLS/mTLS per ingest path, per-source API keys, distroless non-root read-only rootfs, and **a CI lint rule on log call sites** enforcing that no payload field is ever logged (F-12.2). That lint is the only durable enforcement; review alone will not hold.
4. **Test harnesses** — chaos (SIGKILL mid-batch, sink 500s, disk full, clock jumps), 24h soak at 2× rated load with memory and file-size assertions, and the load harness that publishes throughput per release.

**Exit:** the §13 table is measured, not estimated, and the numbers are in the release notes.

## 6. Phase 4 — Deployable

Helm chart with network policy and RBAC; OTel Collector components plus builder manifest (F-13.3, Q-2); gateway mapping files for LiteLLM/Portkey/Helicone; `cc inspect` and `cc replay`; conformance suite published with a sample dataset (F-10.3/10.5); signed artifacts and SBOM (F-12.4); docs.

**Exit (spec M3):** a design partner runs it in their cluster unattended for a week.

## 7. Library choices

All Apache-2.0, MIT or BSD. Each is a load-bearing decision worth confirming before Phase 1.

| Need | Choice | Why |
|---|---|---|
| OTLP receive | `go.opentelemetry.io/collector/pdata` (`ptraceotlp`) | Gives the OTLP service definitions without pulling the whole Collector runtime into the standalone binary. |
| Parquet | `parquet-go/parquet-go` | Struct-tag schema mapping, actively maintained, Apache 2.0. |
| Object store | `aws-sdk-go-v2` | S3-compatible covers MinIO/R2; GCS and Azure via their own SDKs if native is needed. |
| Config | `gopkg.in/yaml.v3` with `KnownFields(true)` | Strict decoding *is* F-11.6. Viper's leniency on unknown keys is the opposite of what the spec requires. |
| Expressions | `google/cel-go` | Sampling tail rules and entity extraction share one evaluator. |
| JSONPath | `ohler55/ojg` | For the JSONPath half of F-5.1/F-6.1. |
| Metrics | `prometheus/client_golang` | §11. |
| Disk buffer | `tidwall/wal` | Segment WAL; avoids writing a buffer state machine in Phase 3. Re-evaluate if encryption-at-rest (F-8.6) doesn't fit cleanly. |
| IDs | `oklog/ulid` | Sortable, matches the file-naming scheme in §8. |
| Licence gate | `go-licenses` in CI | F-12.4 and the §13 dependency row. |

## 8. Sizing

Rough, one engineer, calendar weeks. Phases 2 and 3 parallelise across two or three people; Phases 0 and 1 do not.

| Phase | Estimate | Note |
|---|---|---|
| 0 — Repo and contract | ~1 week | Blocked on the project name only for repo creation. |
| 1 — Walking skeleton | ~3 weeks | The interface design review is inside this. |
| 2 — MUST coverage | ~6 weeks | Redaction engine and assembly hardening are the two heavy items. |
| 3 — Durability and security | ~5 weeks | Soak and chaos harnesses are a meaningful share of this. |
| 4 — Deployable | ~4 weeks | Helm, OTel distro, conformance suite, docs, release engineering. |

## 9. What this plan does not cover

Inherited from spec §2.2 and unchanged: CDC connectors, outcome joins, reward computation, verifiers, training-set export, and any UI. The reserved tables (§7.4) are created empty and never written; `POST /v1/outcomes` (§9.4) is the pressure valve so a partner can prove a join by hand without the collector growing a CDC subsystem.

## 10. Open items before Phase 1 starts

| Item | Needed by | Note |
|---|---|---|
| Project name (Q-7) | Phase 0, repo creation | Blocks nothing else. |
| Which export format `cc import` supports first | Phase 2 item 1 | Should be whichever a named design partner actually uses. |
| A recorded OpenInference span stream to use as the golden fixture | Phase 1 acceptance | Can be synthesised, but a real one from a partner is worth more. |
| Confirmation of the library table in §7 | Phase 1 | Swapping the Parquet or buffer library later is expensive. |
| Target instance size for the §13 throughput number | Phase 3 | Spec says 5k spans/s at 2 vCPU / 2 GB; confirm that is the real deployment shape. |

---

## Appendix A — Phase 1 build notes

Written during the phase, not before it. These are the things the plan did not
anticipate.

### A.1 F-3.2 and F-3.3 conflict, and the spec does not say so

F-3.3 says emit on an explicit terminal event or window expiry. F-3.2 says
tolerate out-of-order arrival within the window. Taken literally together they
are contradictory: emitting the instant a terminal marker arrives gives an
episode **zero** tolerance for out-of-order arrival, so any span still in
flight lands after emit and starts a bogus second episode.

This was found by the shuffle/duplicate harness, which failed on roughly 30 of
50 seeds before the fix. It would not have been found by an ordered-stream
test, which is exactly why F-3's acceptance criterion is written the way it is.

**Resolution:** a terminal marker makes an episode *eligible* to close, after a
short settle period (`assembly.settle_after_terminal`, default 5s), rather than
closing it immediately. Waiting the full window would honour F-3.2 but blow the
p95 < 60s ingest-to-durable target in §13 for every well-behaved producer.

**Spec change needed:** F-3.3 should be reworded, and the knob documented. It
is currently a collector behaviour that the requirements do not describe.

### A.2 Ordering must not derive from arrival order

The corollary of A.1. Any state derived from the order spans arrived in leaks
into the output and breaks the shuffle criterion. Two places had to be fixed:
the step ordering (now timestamp, then span id as a total-order tiebreak) and
the identity of spans that arrive with no span id (now a content hash, so a
redelivery collapses onto the same key instead of duplicating).

### A.3 The licence gate had to be rewritten

`go-licenses` breaks against the Go 1.26 standard library — it reports
`Package bytes does not have module info` and exits non-zero regardless of the
actual licences. A supply-chain gate that fails spuriously is worse than no
gate, because the first thing anyone does with a noisy gate is switch it off.

Replaced with `tools/licensecheck`, ~150 lines, no dependencies, classifying by
distinctive licence phrases and **failing closed** on anything it cannot
positively identify. It has its own unit tests, including that copyleft wins
over permissive boilerplate in a mixed file.

### A.4 Payloads are JSON envelopes, not concatenated strings

Entity extraction addresses payloads with JSONPath from config (`$.args.id`,
`$.result.metadata.order_id` per §10). That only works if args and result stay
separately addressable, so normalisation emits `{"args":…,"result":…}` for tool
steps and `{"input":…,"output":…}` otherwise. A value that is itself JSON is
embedded as JSON so paths reach into it; anything else is embedded as the exact
string the producer sent.

**Spec gap:** §7.2 describes `content_inline` only as "payload", and does not
say it is structured. Readers need to know this.

### A.5 Deferred, with the reason

| Deferred | Why it is safe for now | When it bites |
|---|---|---|
| `pdata` forces Go 1.26 | Released and stable | A contributor on an older toolchain |
| Assembly expiry is an O(open) scan per tick | Cheap at the default 50k in-flight on a 1s ticker | First thing to revisit if that bound is raised |
| Late spans after emit are counted, not patched | The counter makes the loss visible rather than silent | F-3.5 patch records, Phase 2 |
| The JSON Schema advertises two enum spellings; no decoder honours both yet | Nothing decodes JSON episodes yet | The first `POST /v1/episodes` handler, Phase 2 |
| No unit tests for the sink, OTLP receiver, service or telemetry | All four are exercised end-to-end by DoD-1 | A refactor that DoD-1 happens not to cover |

---

## Appendix B — Phase 2 build notes

### B.1 FNV-1a silently under-sampled by a third

The first head sampler hashed the session key with FNV-1a and took the top
bits. Configured for 10%, it kept **6.8%**. FNV-1a's avalanche in the high bits
is weak for near-identical inputs, which is exactly what session ids are.

An operator would have collected two thirds of the data they asked for and had
no way to notice: the rate is not reported anywhere, and the corpus would just
be smaller than expected. Fixed with a splitmix64 finalizer, which costs a
couple of nanoseconds. The test asserts measured rates within 2 points of the
configured value across four rates, because this class of bug is invisible
without one.

### B.2 The spec's own example sampling rule would have disabled sampling

Spec §10 shows this tail rule:

```yaml
keep_if:
  - "episode.raw['human_edited'] == 'true'"
```

Under CEL's default map semantics, indexing an absent key is an **error**, not
a false. Most episodes do not carry `human_edited`. And since a rule that fails
to evaluate keeps the episode — deliberately, so a sampling bug never silently
deletes data — this policy would have degraded into "keep everything" on every
ordinary episode, invisible except as an unexpectedly large storage bill.

Fixed with a map wrapper where a missing key reads as the empty string.
`Contains` is left honest, so `'k' in raw` still distinguishes present from
absent.

**Spec note:** worth stating in §10 that indexing is lenient and `in` is the
presence test.

### B.3 Opposite failure directions for redaction and sampling

These two look symmetrical and are not, which is worth stating explicitly
because it is the kind of thing a later contributor will "fix" in the wrong
direction:

| Stage | On rule failure | Why |
|---|---|---|
| Redaction | **Quarantine** the episode | Passing an unredacted record on is a breach |
| Sampling | **Keep** the episode | Dropping on a bug is silent data loss |

Both are failing safe. They just disagree about which direction safe is.

### B.4 Enum spellings: the schema's promise is now kept

Phase 1 left the published JSON Schema advertising both `STATUS_COMPLETE` and
`complete` with no decoder honouring either-or. `collector/wire` now accepts
both on the native API, and the test asserts against the same vocabulary the
schema publishes.

An invalid enum is an error rather than being coerced to `other`. F-2.5's
retention rule is about span kinds a *tracing convention* did not define; a
native producer sending an invalid value against a published schema is a bug in
that producer, and saying so is more useful than silently reclassifying data.

### B.5 `has_params: true` does not mean replayable

Langfuse exports carry temperature and max_tokens but no seed. The fidelity
flag says `has_params: true`, which is accurate — parameters *were* reported —
but a consumer reading it as "this can be replayed exactly" would be wrong,
because a sampled completion without a seed will not reproduce.

Rather than overload the boolean, `cc replay` now says so explicitly, and
`spec/README.md` documents the distinction. **A consumer filtering for
exactly-reproducible episodes should check for a seed on the LLM steps, not
only the flag.**

### B.6 Payload shape is a contract the spec does not describe

§7.2 calls `content_inline` a payload and says nothing about its structure. It
is a JSON envelope — `{"args":…,"result":…}` for tool steps, `{"input":…,"output":…}`
otherwise — because entity extraction addresses it with JSONPath from config
and a flattened string would make `$.args.id` unwritable.

Now documented in `spec/README.md`. **The spec should say this in §7.2**; a
reader writing against the tables alone would not expect it.

### B.7 Deferred, with the reason

| Deferred | Why it is safe for now | When it bites |
|---|---|---|
| Gateway mappings (F-1.3), file tail (F-1.4), Kafka (F-1.5) | SHOULD/MAY, and the declarative mapping format they need is already proven by the convention tables | UC-1, a gateway-only deployment |
| Per-source redaction override (F-5.7) | Single-tenant deployments share one policy | A collector fronting two teams with different rules |
| Per-tenant quota and shed (F-7.3) | Single-tenant per deployment (Q-5) | Multi-tenancy, which is a v1.1 question anyway |
| External PII detection (F-5.8) | Regex plus allow-list covers the structured cases; the hook is an interface away | A partner with free-text PII that no regex catches |
| Logprobs (F-4.5) | MAY, and no convention carries them | A partner doing token-level analysis |
| `cc inspect` cannot read a partially written file | The manifest is written last, so an unmanifested file is not yet real to a reader | Debugging a crashed write, where a partial file is exactly what you want to look at |
