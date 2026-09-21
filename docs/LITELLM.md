# Connecting LiteLLM

Verified against LiteLLM 1.102.0 running as a proxy, with real payloads
captured into `spec/testdata/litellm-standard-logging-payload.json` and tested
in CI.

## Use the `generic_api` callback, not `otel`

LiteLLM can send to the collector two ways. Both were run end to end; they are
not equivalent.

| | `generic_api` → webhook source | `otel` → OTLP source |
|---|---|---|
| Calls of one agent run grouped into one episode | Yes, via `litellm_session_id` | **No** — one trace per request |
| Seed | Yes | **No** — not emitted |
| Temperature, max tokens, tokens, cost | Yes | Yes |
| Step kind | `llm` | `other` — no `gen_ai.operation.name` |
| Tool calls rebuilt as tool steps | Yes, from message history | No |
| Failures recorded | Yes | Partly |
| Noise | None | ~6 internal LiteLLM spans per call |

## Configuration

LiteLLM:

```yaml
litellm_settings:
  callbacks: ["generic_api"]
```

```sh
GENERIC_LOGGER_ENDPOINT=https://<collector>:<port>/v1/hooks/litellm
GENERIC_LOGGER_HEADERS="Authorization=Bearer <token>"
```

Collector:

```yaml
sources:
  - name: litellm
    type: webhook
    http:
      listen: "0.0.0.0:4320"
      tls: {cert_file: /certs/tls.crt, key_file: /certs/tls.key}
    mapping: mappings/litellm.yaml
    auth: {type: bearer, token_env: CC_LITELLM_TOKEN}
```

`cc validate` refuses a bearer token on a plaintext listener that is not bound
to localhost — the token would cross the network in the clear.

## What your agent code should send

LiteLLM passes these through; without them the data is much weaker.

| Send | Why |
|---|---|
| `litellm_session_id` — one per agent run | Groups the run's calls into one episode. Without it every call is its own one-step episode. |
| `seed` | The only way a sampled completion can be reproduced |
| `metadata.task_type` | Partition key |
| `metadata.group_id` | Ties N rollouts of one task together |
| `metadata.episode_end: true` on the last call | A gateway cannot know a run finished. Without it the episode closes when the assembly window expires, marked `timed_out` rather than `complete`. |

## What a gateway cannot capture

This is structural, not something a mapping can fix:

- **Tool versions and tool timing.** The tool runs in your application, out of
  the gateway's sight. Tool steps are rebuilt from the conversation — the
  arguments the model asked for and the result fed back — so entity extraction
  works, but `has_tool_versions` is always false.
- **Tool calls that never reach the model again.** A tool whose result is never
  sent back in a later call is never seen.
- **Token spans / trainable masks.** No gateway knows which tokens to train on.

If you need those, use the SDKs (`sdk/python`, `sdk/typescript`) for the agent
loop — they are the only path that reaches all three fidelity flags true. The
two can run together: gateway for organisation-wide coverage, SDK for the agents
you train on.

## Privacy

LiteLLM's payload includes the caller's IP address, user agent and API-key
hash. The mapping excludes the top-level IP address and user agent from `raw`;
the rest sits in nested `metadata`, which is not copied. The prompt and
completion are the payload and go through your redaction policy like any other.
