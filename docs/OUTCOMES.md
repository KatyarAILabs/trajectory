# The outcome join

Captured trajectories tell you what an agent did. The outcome join tells you
whether it worked — in the business, not in an eval — by attaching what
happened afterwards (the refund went through, the ticket reopened) to the run
that caused it. That turns a trajectory into a labelled, rewardable training
example.

```
capture ──▶ outcomes ──▶ join ──▶ score ──▶ export
 cc run      /v1/outcomes  cc join  cc score  cc export
             cc outcomes
```

Run it end to end on sample data: `make demo-outcomes`.

## 1. Capture with keys

The join can only reach episodes that carry business keys. Configure
`entities` so the collector extracts them from tool calls:

```yaml
entities:
  - tool: zendesk.update_ticket
    keys:
      ticket_id: "$.args.id"
      requester: "$.args.requester"
```

Watch `cc_episodes_without_entity_keys_ratio`. An episode with no key can never
be labelled, and that cannot be fixed retroactively.

## 2. Send outcomes

Your own job reports what happened. The collector never pulls — there are no
connectors to ticketing or order systems.

```sh
curl -X POST https://collector:4319/v1/outcomes -H 'Authorization: Bearer …' -d '[
  {"outcome_id":"evt-1001","entity_name":"ticket_id","entity_key":"TKT-9001",
   "kind":"refund_status","value":"completed","occurred_at":"2026-09-20T12:00:00Z"}
]'
```

or from a file (CSV with a header row, or JSON/JSONL):

```sh
cc outcomes -config collector.yaml -from outcomes.csv
```

or from the SDKs: `client.outcome(...)`.

| Field | Required | Meaning |
|---|---|---|
| `entity_name` | yes | Which key: must match an `entities` key name |
| `entity_key` | yes | The **raw** identifier. See below. |
| `kind` | yes | What was measured: `refund_status`, `ticket_status` |
| `value` | | The value: `completed`, `reopened` |
| `occurred_at` | yes | When it happened. RFC 3339 or epoch s/ms/µs |
| `observed_at` | | When it was known. Defaults to receipt time |
| `outcome_id` | | Stable id; a re-post is counted once |

**Send raw identifiers.** If a redaction rule tokenized a key when the episode
was captured, the lake holds a token, not the ID. The collector puts outcome
keys through the same rules with the same HMAC key, so they become the same
token and meet. This also means a raw email you send is never stored.

It does mean the collector that loads outcomes must use the **same redaction
rules and HMAC key** as the one that captured the episodes. Different rules, or
a rotated key, and outcomes silently match nothing — `cc join` warns when
nothing was attributed.

## 3. Join

```sh
cc join -lake ./lake -as-of 2026-10-01T00:00:00Z -horizon 30d -out labels.jsonl
```

Three rules decide what attaches to what:

- **Window.** An outcome attaches only if it happened after the episode started
  and before its horizon closed (episode end + `-horizon`). An outcome from
  before the run cannot have been caused by it.
- **Last touch.** When several runs touched the same key, the outcome goes to
  the most recent run before it. `-attribution all` credits every run.
- **As of.** Outcomes observed after `-as-of` are invisible. Fix `-as-of` and
  the same lake always gives the same labels, so a training set can be rebuilt
  exactly.

Each episode gets a label status:

| Status | Meaning |
|---|---|
| `final` | The horizon closed before as-of. The label will not change. |
| `provisional` | The horizon is still open; more outcomes may arrive. |
| `unjoinable` | No entity keys. No outcome can ever reach it. |

**Do not train on provisional labels.** A recent episode with no complaint yet
is not a success; its complaint may simply not have arrived.

## 4. Score

A reward is a short, ordered list of rules. Write it as YAML
(`examples/scorer.yaml`):

```yaml
verifier_id: refund.resolved
version: "1"
rules:
  - {clause: reopened, when: "latest['ticket_status'] == 'reopened'", reward: 0.0}
  - {clause: refund_completed, when: "latest['refund_status'] == 'completed'", reward: 1.0}
```

```sh
cc score -lake ./lake -as-of 2026-10-01T00:00:00Z -scorer scorer.yaml
```

- The first matching rule wins; its clause is recorded on the reward.
- `latest[kind]` is the most recent value of that outcome kind.
- Only `final` labels are scored unless `require_final: false`.
- Rewards are written into the lake's `rewards` table, keyed by verifier and
  version. Re-running is idempotent; change a rule's meaning, bump `version`.

Rules not enough? Implement `pkg/scorer.Scorer` in Go.

## 5. Export

```sh
cc export -lake ./lake -as-of … -format chat -min-reward 1 -out sft.jsonl
```

| Format | One line per | For |
|---|---|---|
| `trajectories` | episode: steps, outcomes, reward | RL, analysis |
| `chat` | model call, in chat-message format | supervised fine-tuning |
| `preference` | pair of best and worst rollouts in a `group_id` | DPO-style preference training |

Filters: `-min-reward`, `-require-final` (on by default), `-require-reward`,
`-require-fidelity params,token_spans,tool_versions`. Every excluded episode is
counted by reason, so a small dataset says why.

## What the join cannot fix

- **Missing keys at capture time.** Configure `entities` before capturing.
- **Different redaction or HMAC key** between capture and outcome loading.
- **Outcomes that describe many runs at once** ("customer churned") are
  attributed by last touch, which may not be what caused them. Consider
  `-attribution all` and a scorer that discounts accordingly.
