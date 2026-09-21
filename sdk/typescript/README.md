# @trajectory/sdk (TypeScript)

Emit agent trajectories to a trajectory collector. Zero runtime dependencies;
uses the platform `fetch` (Node 18+, browsers, Deno, Bun).

```ts
import { Client } from "@trajectory/sdk";

const client = new Client({ endpoint: "http://localhost:4319", token: process.env.CC_SDK_TOKEN });

await client.episode({ taskType: "refund", groupId: "rollout-3" }, async (ep) => {
  const plan = await ep.llm(prompt, completion, {
    model: "claude-opus-5", params: { seed: 7 }, trainable: true,
    tokenSpans: [{ start: 0, end: 120, trainable: false }, { start: 120, end: 180, trainable: true }],
  });
  await ep.tool("zendesk.update_ticket", { args: { id: "TKT-1" }, result: { ok: true },
    version: "2.3.1", parent: plan });
});
```

Same design and guarantees as the Python SDK: streaming steps, the last real
step carries the terminal marker (no synthetic steps), thrown errors are
recorded and re-thrown, `trainable: null` means unknown, and it never throws
into the agent loop unless `throwErrors: true`.

`npm install && npm test`
