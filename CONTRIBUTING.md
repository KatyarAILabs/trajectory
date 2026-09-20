# Contributing

Apache 2.0, and **DCO sign-off rather than a CLA**: you keep your copyright,
and you certify you have the right to contribute the patch.

Sign every commit:

```sh
git commit -s -m "your message"
```

That appends a `Signed-off-by:` line, which is your agreement to the
[Developer Certificate of Origin](https://developercertificate.org/). CI checks
every commit in a PR.

## Before you open a PR

```sh
make            # generate, build, test, lint
```

## Things that will fail CI, and why

| Gate | Why it exists |
|---|---|
| `make verify-generated` | The protos in `spec/proto` are the source of truth. Editing one without committing the regenerated Go and JSON Schema publishes a contract that does not match the code. |
| Parquet golden test | The physical file layout is a published contract. Readers are built against it. Reordering or retyping a column breaks every one of them. |
| `make licences` | Every transitive dependency must be permissive. A security reviewer rejects the project over one unclear licence. |
| DCO | §18 of the spec. |

## Changing the schema

The schema is semver'd and **additive only within a major version** (F-10.1):
a reader written against 0.1 must read 0.2 unmodified.

1. Edit the `.proto` in `spec/proto/trajectory/v1/`. Append fields; never
   renumber, retype or remove one.
2. Add the matching field to the Parquet row type in `pkg/record/`, in the
   same position. The proto parity test enforces that the two agree.
3. `make generate` then `make golden`, and commit both.
4. Include in the PR description what a reader built against the previous
   version sees when it reads a file written by the new one.

Adding a *writer* for `outcomes`, `labels` or `rewards` is not a schema change.
Those are reserved surface and writing them reopens the non-goals in spec §2.2
— raise an issue first.

## Payloads never reach logs

The collector must never write payload content to its own logs at any level,
including debug (F-12.2). This is enforced by a lint rule on log call sites,
but review it yourself: a leak here is the failure mode that loses a security
review, and it is invisible until it matters.
