# Security policy

## Reporting a vulnerability

Report privately through GitHub's "Report a vulnerability" flow on this
repository. Do not open a public issue.

Please include the version or commit, what an attacker gains, and the minimum
steps to reproduce. We will acknowledge and give you an assessment timeline.

## What this project handles

The collector processes agent trajectories that may contain prompts, tool
arguments and tool results — that is, potentially sensitive customer data. The
security properties below are requirements, not aspirations, and a report that
one of them does not hold is a vulnerability:

| Property | Requirement |
|---|---|
| Redaction precedes persistence | Redaction runs before any write to disk buffer, sink or log (F-5.3). Even the on-disk buffer contains no unredacted payloads. |
| Fail closed | A policy that cannot be evaluated quarantines the record rather than passing it through (F-5.5). |
| No payloads in logs | Payload content is never logged at any level, including debug (F-12.2). |
| No payloads in metric labels | No metric label carries user data — no tenant free text, no tool arguments (§11). |
| No unconfigured egress | The only outbound connections are to configured sinks and telemetry destinations. No update checks, no analytics (F-12.6). |
| Least privilege at runtime | Non-root, distroless, read-only root filesystem apart from the buffer volume (F-12.5). |

## Key management

The HMAC tokenization key never leaves the deployment and is supplied by
environment or secret manager, never inline in config.

**Rotating it breaks joinability of new records against old ones.** A `key_id`
is recorded on each record so a rotation is detectable rather than silent.
