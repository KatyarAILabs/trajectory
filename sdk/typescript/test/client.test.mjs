// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

import { test, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client } from "../dist/index.js";

let server, url, bodies, status;

before(async () => {
  server = http.createServer((req, res) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      bodies.push({ path: req.url, auth: req.headers.authorization, body: JSON.parse(data) });
      res.writeHead(status);
      res.end("{}");
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  url = `http://127.0.0.1:${server.address().port}`;
});
after(() => server.close());
beforeEach(() => { bodies = []; status = 202; });

test("steps stream and the last real step is terminal", async () => {
  const c = new Client({ endpoint: url, throwErrors: true });
  await c.episode({ taskType: "refund", groupId: "g" }, async (ep) => {
    const plan = await ep.llm("p", "c", { model: "m", params: { seed: 7 } });
    await ep.tool("zendesk.update_ticket", { args: { id: 1 }, version: "2.3.1", parent: plan });
  });
  assert.equal(bodies.length, 2, "no synthetic trailing step");
  assert.equal(bodies[0].body.terminal, false);
  assert.equal(bodies[1].body.terminal, true);
  assert.equal(bodies[1].body.parent_span_id, bodies[0].body.span_id);
  assert.equal(bodies[0].body.step.params.seed, 7);
  assert.equal(bodies[1].body.step.tool_version, "2.3.1");
});

test("an exception is recorded and re-thrown", async () => {
  const c = new Client({ endpoint: url, throwErrors: true });
  await assert.rejects(c.episode({}, async (ep) => {
    await ep.llm("p", "c");
    throw new TypeError("agent crashed");
  }), TypeError);
  const last = bodies.at(-1).body;
  assert.equal(last.terminal, true);
  assert.equal(last.episode_error.type, "TypeError");
});

test("unknown trainability is never coerced to false", async () => {
  const c = new Client({ endpoint: url, throwErrors: true });
  await c.episode({}, async (ep) => {
    await ep.llm("p", "c", { trainable: true, tokenSpans: [
      { start: 0, end: 5, trainable: false }, { start: 5, end: 9, trainable: null }] });
    await ep.tool("t");
  });
  assert.deepEqual(bodies[0].body.step.token_spans.map((s) => s.trainable), ["false", "unknown"]);
  assert.equal(bodies[1].body.step.trainable, "unknown");
});

test("never throws into the agent by default", async () => {
  const errors = [];
  const c = new Client({ endpoint: "http://127.0.0.1:1", retries: 1, timeoutMs: 200,
    onError: (m) => errors.push(m) });
  await c.episode({}, async (ep) => { await ep.llm("p", "c"); });
  assert.equal(c.failed, 1);
  assert.equal(errors.length, 1);
  assert.ok(!errors[0].includes('"p"'), "the error must not quote the payload");
});

test("a 400 is not retried", async () => {
  status = 400;
  const c = new Client({ endpoint: url, retries: 3, onError: () => {} });
  await c.episode({}, async (ep) => { await ep.llm("p", "c"); });
  assert.equal(bodies.length, 1);
});

test("bearer token is sent", async () => {
  const c = new Client({ endpoint: url, token: "secret", throwErrors: true });
  await c.episode({}, async (ep) => { await ep.llm("p", "c"); });
  assert.equal(bodies[0].auth, "Bearer secret");
});

test("outcome posts to /v1/outcomes", async () => {
  const c = new Client({ endpoint: url, throwErrors: true });
  await c.outcome({ entityName: "ticket_id", entityKey: "TKT-1", kind: "refund_status",
    value: "completed", occurredAt: "2026-09-21T10:00:00Z", outcomeId: "evt-1" });
  assert.equal(bodies[0].path, "/v1/outcomes");
  assert.equal(bodies[0].body.outcome_id, "evt-1");
});
