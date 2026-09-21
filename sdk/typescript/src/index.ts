// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

/**
 * Emit agent trajectories to a trajectory collector (UC-3).
 *
 * The same design as the Python SDK, for the same reasons:
 *
 * - Steps stream to `/v1/spans` as they happen, so a crashed agent loses one
 *   step rather than the whole trajectory.
 * - Each step is held until the next arrives, so the last real step carries the
 *   terminal marker. No synthetic step is ever added to a trajectory.
 * - It never throws into the agent loop by default. Telemetry that can take
 *   down the thing it observes gets removed.
 * - Zero runtime dependencies; it uses the platform `fetch`.
 *
 * ```ts
 * const client = new Client({ endpoint: "http://localhost:4319" });
 * await client.episode({ taskType: "refund" }, async (ep) => {
 *   const plan = await ep.llm("plan", "look up the order", { model: "claude-opus-5" });
 *   await ep.tool("zendesk.update_ticket", { args: { id: "TKT-1" }, result: { ok: true },
 *     version: "2.3.1", parent: plan });
 * });
 * ```
 */

export const VERSION = "0.1.0";

export type Trainable = boolean | null;

/** A byte range of a step's content, marked trainable or not (F-4.4). */
export interface TokenSpan {
  start: number;
  end: number;
  /** `null` means unknown. Never coerce unknown to false: a trainer acts on it. */
  trainable: Trainable;
}

export interface ClientOptions {
  endpoint?: string;
  /** Bearer token. Read it from the environment; never hard-code it. */
  token?: string;
  timeoutMs?: number;
  retries?: number;
  /** Throw instead of logging. Off by default. */
  throwErrors?: boolean;
  /** Where delivery failures are reported when not throwing. */
  onError?: (message: string) => void;
}

export interface EpisodeOptions {
  taskType?: string;
  groupId?: string;
  sessionId?: string;
  raw?: Record<string, string>;
}

interface StepCommon {
  parent?: string;
  attempt?: number;
  latencyMs?: number;
}

export interface LLMOptions extends StepCommon {
  model?: string;
  provider?: string;
  params?: { temperature?: number; top_p?: number; max_tokens?: number; seed?: number; stop?: string[] };
  tokens?: { input?: number; output?: number; cached?: number; reasoning?: number };
  tokenSpans?: TokenSpan[];
  trainable?: Trainable;
  finishReason?: string;
  /** Cost of the call in US dollars (F-4.3). */
  costUsd?: number;
}

export interface ToolOptions extends StepCommon {
  args?: unknown;
  result?: unknown;
  version?: string;
  error?: string;
}

const trainableWire = (t: Trainable | undefined): string =>
  t === true ? "true" : t === false ? "false" : "unknown";

const nowUs = (): number => Math.floor(performance.timeOrigin * 1000 + performance.now() * 1000);

function uuid(): string {
  return globalThis.crypto?.randomUUID?.() ??
    "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
}

export class Client {
  readonly endpoint: string;
  private readonly opts: Required<Omit<ClientOptions, "token" | "endpoint">> & { token?: string };
  sent = 0;
  failed = 0;

  constructor(options: ClientOptions = {}) {
    this.endpoint = (options.endpoint ?? "http://localhost:4319").replace(/\/+$/, "");
    this.opts = {
      token: options.token,
      timeoutMs: options.timeoutMs ?? 5000,
      retries: Math.max(1, options.retries ?? 3),
      throwErrors: options.throwErrors ?? false,
      onError: options.onError ?? ((m) => console.warn(m)),
    };
  }

  /**
   * Run `fn` inside an episode. A thrown error is recorded as the episode's
   * error and re-thrown; the episode is closed either way, because failed
   * trajectories are exactly the ones a verifier wants.
   */
  async episode<T>(options: EpisodeOptions, fn: (ep: Episode) => Promise<T>): Promise<T> {
    const ep = new Episode(this, options);
    try {
      const out = await fn(ep);
      await ep.end();
      return out;
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      await ep.end({ type: e.name, message: e.message.slice(0, 500) });
      throw err;
    }
  }

  /**
   * Report a business outcome for the outcome join (§9.4). Send the raw
   * identifier; the collector transforms it the same way it transformed
   * episode keys, so a tokenized key still joins. Pass a stable `outcomeId`
   * so a retry is counted once.
   */
  outcome(o: {
    entityName: string; entityKey: string; kind: string; value: string;
    occurredAt: string | number; outcomeId?: string; source?: string;
  }): Promise<boolean> {
    const body: Record<string, unknown> = {
      entity_name: o.entityName, entity_key: o.entityKey, kind: o.kind,
      value: o.value, occurred_at: o.occurredAt,
    };
    if (o.outcomeId) body.outcome_id = o.outcomeId;
    if (o.source) body.source = o.source;
    return this.post("/v1/outcomes", body);
  }

  /** @internal */
  async post(path: string, body: unknown): Promise<boolean> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.opts.token) headers["Authorization"] = `Bearer ${this.opts.token}`;

    let delay = 200;
    let last = "unknown error";
    for (let attempt = 0; attempt < this.opts.retries; attempt++) {
      try {
        const res = await fetch(this.endpoint + path, {
          method: "POST",
          headers,
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.opts.timeoutMs),
        });
        await res.arrayBuffer();
        if (res.ok) {
          this.sent++;
          return true;
        }
        last = `HTTP ${res.status}`;
        // 400 and 401 will not succeed on retry; 429 and 503 might.
        if (res.status !== 429 && res.status !== 503) break;
      } catch (err) {
        last = err instanceof Error ? err.name : "network error";
      }
      if (attempt < this.opts.retries - 1) {
        await new Promise((r) => setTimeout(r, delay));
        delay *= 2;
      }
    }

    this.failed++;
    // Names the endpoint and the failure, never the body: the body is payload.
    const msg = `trajectory: could not deliver to ${this.endpoint}${path}: ${last}`;
    if (this.opts.throwErrors) throw new Error(msg);
    this.opts.onError(msg);
    return false;
  }
}

type Pending = { spanId: string; step: Record<string, unknown>; parent: string };

export class Episode {
  readonly sessionId: string;
  private idx = 0;
  private lastSpan = "";
  private closed = false;
  private pending: Pending | null = null;

  constructor(private readonly client: Client, private readonly options: EpisodeOptions = {}) {
    this.sessionId = options.sessionId ?? uuid();
  }

  /** Record a model call. Returns the span id, for `parent`. */
  llm(prompt: unknown, completion: unknown, o: LLMOptions = {}): Promise<string> {
    const step: Record<string, unknown> = {
      kind: "llm",
      content: JSON.stringify({ input: prompt, output: completion }),
      trainable: trainableWire(o.trainable),
    };
    if (o.model) step.model = o.model;
    if (o.provider) step.provider = o.provider;
    if (o.params) step.params = o.params;
    if (o.tokens) step.token_counts = o.tokens;
    if (o.finishReason) step.finish_reason = o.finishReason;
    if (o.costUsd !== undefined) step.cost_usd = o.costUsd;
    if (o.tokenSpans?.length) {
      for (const s of o.tokenSpans) {
        if (s.end < s.start) throw new RangeError("token span end is before start");
      }
      step.token_spans = o.tokenSpans.map((s) => ({
        start: s.start, end: s.end, trainable: trainableWire(s.trainable),
      }));
    }
    return this.step(step, o);
  }

  /**
   * Record a tool call. Give a retry the same `parent` and a higher `attempt`
   * so it is kept as a branch rather than flattened into a line (F-3.4).
   */
  tool(name: string, o: ToolOptions = {}): Promise<string> {
    const content: Record<string, unknown> = {};
    if (o.args !== undefined) content.args = o.args;
    if (o.result !== undefined) content.result = o.result;
    const step: Record<string, unknown> = {
      kind: "tool", tool_name: name, content: JSON.stringify(content), trainable: "unknown",
    };
    if (o.version) step.tool_version = o.version;
    if (o.error) step.error = { type: "tool_error", message: o.error.slice(0, 500) };
    return this.step(step, o);
  }

  retrieval(query: unknown, results: unknown, o: StepCommon = {}): Promise<string> {
    return this.step({
      kind: "retrieval", content: JSON.stringify({ input: query, output: results }), trainable: "unknown",
    }, o);
  }

  /** Record a human intervention — an edit, approval or correction. */
  human(content: unknown, o: StepCommon = {}): Promise<string> {
    return this.step({ kind: "human", content: JSON.stringify({ input: content }), trainable: "unknown" }, o);
  }

  private async step(step: Record<string, unknown>, o: StepCommon): Promise<string> {
    if (this.closed) throw new Error("episode is already closed");
    const spanId = `${this.sessionId}-${this.idx++}`;
    step.started_at = nowUs();
    step.attempt = o.attempt ?? 0;
    if (o.latencyMs !== undefined) step.latency_ms = o.latencyMs;

    await this.flush(false);
    this.pending = { spanId, step, parent: o.parent ?? this.lastSpan };
    this.lastSpan = spanId;
    return spanId;
  }

  /** Close the episode. The last step carries the terminal marker. */
  async end(error?: { type: string; message: string }): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.flush(true, error ? { episode_error: error } : undefined);
  }

  private async flush(terminal: boolean, extra?: Record<string, unknown>): Promise<void> {
    const p = this.pending;
    if (!p) return;
    this.pending = null;

    const body: Record<string, unknown> = {
      session_id: this.sessionId,
      // The stored episode_id, so `ep.sessionId` finds this trajectory later.
      episode_id: this.sessionId,
      span_id: p.spanId,
      parent_span_id: p.parent,
      terminal,
      instrumentation: "trajectory-typescript",
      instrumentation_version: VERSION,
      step: p.step,
      ...extra,
    };
    if (this.options.taskType) body.task_type = this.options.taskType;
    if (this.options.groupId) body.group_id = this.options.groupId;
    if (this.options.raw) body.episode_raw = this.options.raw;

    await this.client.post("/v1/spans", body);
  }
}
