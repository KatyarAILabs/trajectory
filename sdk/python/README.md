# trajectory-sdk (Python)

Emit agent trajectories to a trajectory collector. Zero dependencies.

```python
import trajectory

client = trajectory.Client("http://localhost:4319", token=os.environ.get("CC_SDK_TOKEN"))

with client.episode(task_type="refund", group_id="rollout-3") as ep:
    plan = ep.llm(prompt, completion, model="claude-opus-5",
                  params={"temperature": 0.2, "seed": 7},
                  trainable=True,
                  token_spans=[trajectory.TokenSpan(0, 120, False),
                               trajectory.TokenSpan(120, 180, True)])
    ep.tool("zendesk.update_ticket", args={"id": "TKT-1"}, error="rate limited",
            parent=plan, version="2.3.1")
    ep.tool("zendesk.update_ticket", args={"id": "TKT-1"}, result={"ok": True},
            parent=plan, attempt=1, version="2.3.1")
```

**Why use this rather than OpenTelemetry instrumentation:** it is the only path
that carries token spans, trainable masks, seeds and tool versions — the fields
that decide whether a trajectory can be replayed or trained on. Episodes sent
this way are the ones that reach `fidelity` all-true.

**Behaviour worth knowing**

- Steps stream as they happen. A crash loses one step, not the trajectory.
- An exception inside `with client.episode()` is recorded as the episode's
  error and re-raised. Failed trajectories are exactly what a verifier wants.
- A retry is a branch: give it the same `parent` and a higher `attempt`.
- `trainable=None` means unknown. It is never coerced to `False`.
- It never raises into your agent loop by default. Delivery failures are logged
  and counted (`client.failed`). Use `raise_errors=True` in tests.

Run the tests: `python -m unittest discover -s tests`
