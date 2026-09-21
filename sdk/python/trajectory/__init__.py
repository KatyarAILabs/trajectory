# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0
"""Emit agent trajectories to a trajectory collector (UC-3).

This is the highest-fidelity path into the collector. It exists for the fields
auto-instrumentation cannot see: tool versions, the seed, which tokens the model
generated versus which came from tools, and group ids for N rollouts of one
task. No tracing convention carries those, so an agent loop that wants them
recorded has to say them explicitly.

    import trajectory

    client = trajectory.Client("http://localhost:4319")
    with client.episode(task_type="refund", group_id="rollout-3") as ep:
        ep.llm("plan the refund", "I will look up the order",
               model="claude-opus-5", params={"temperature": 0.2, "seed": 7})
        ep.tool("zendesk.update_ticket", args={"id": "TKT-1"},
                result={"ok": True}, version="2.3.1")

Steps are streamed to /v1/spans as they happen rather than held until the end.
A long-running agent that crashes should lose one step, not the whole
trajectory, and the collector's assembler already knows how to group steps by
session.

The SDK never raises into your agent loop by default. A telemetry library that
can take down the thing it observes gets removed; failures are logged and
counted instead. Pass ``raise_errors=True`` in tests.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

__all__ = ["Client", "Episode", "TokenSpan", "__version__"]
__version__ = "0.1.0"

log = logging.getLogger("trajectory")

_KINDS = {"llm", "tool", "retrieval", "human", "other"}
_TRAINABLE = {True: "true", False: "false", None: "unknown"}


def _now_us() -> int:
    return time.time_ns() // 1000


class TokenSpan:
    """A byte range of a step's content, marked as trainable or not (F-4.4).

    This is what lets a trainer mask out tool output and train only on what the
    model generated. ``trainable=None`` means unknown — never coerce an unknown
    into False, because a trainer will act on it.
    """

    __slots__ = ("start", "end", "trainable")

    def __init__(self, start: int, end: int, trainable: Optional[bool]):
        if end < start:
            raise ValueError("token span end is before start")
        self.start, self.end, self.trainable = start, end, trainable

    def to_json(self) -> Dict[str, Any]:
        return {"start": self.start, "end": self.end, "trainable": _TRAINABLE[self.trainable]}


class Client:
    """A connection to a collector's native API.

    Args:
        endpoint: Base URL of a native source, e.g. ``http://localhost:4319``.
        token: Bearer token, if the source requires one. Read it from the
            environment; never hard-code it.
        timeout: Per-request timeout in seconds.
        retries: Attempts for a retryable failure (503, 429, network error).
        raise_errors: Raise instead of logging. Off by default, so telemetry
            can never break the agent it observes.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4319",
        token: Optional[str] = None,
        timeout: float = 5.0,
        retries: int = 3,
        raise_errors: bool = False,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.retries = max(1, retries)
        self.raise_errors = raise_errors
        self._lock = threading.Lock()
        self.sent = 0
        self.failed = 0

    @contextmanager
    def episode(self, **kwargs: Any) -> Iterator["Episode"]:
        """Open an episode for the duration of a ``with`` block.

        An exception inside the block is recorded as the episode's error and
        re-raised; the episode is still closed, so a failed trajectory is
        captured rather than lost — failures are exactly the ones a verifier
        wants to see.
        """
        ep = Episode(self, **kwargs)
        try:
            yield ep
        except BaseException as exc:
            ep.end(error=(type(exc).__name__, str(exc)[:500]))
            raise
        else:
            ep.end()

    def outcome(
        self,
        entity_name: str,
        entity_key: str,
        kind: str,
        value: str,
        occurred_at: Any,
        *,
        outcome_id: Optional[str] = None,
        source: Optional[str] = None,
    ) -> bool:
        """Report a business outcome for the outcome join (§9.4).

        Send the raw identifier — the collector applies the same redaction
        rules to it as to episode keys, so a key stored as a token still joins.
        ``occurred_at`` may be an RFC 3339 string or epoch seconds. Pass a
        stable ``outcome_id`` so a retry is counted once.
        """
        body: Dict[str, Any] = {
            "entity_name": entity_name,
            "entity_key": entity_key,
            "kind": kind,
            "value": value,
            "occurred_at": occurred_at,
        }
        if outcome_id:
            body["outcome_id"] = outcome_id
        if source:
            body["source"] = source
        return self._post("/v1/outcomes", body)

    def _post(self, path: str, body: Any) -> bool:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token

        delay = 0.2
        last: Optional[BaseException] = None
        for attempt in range(self.retries):
            req = urllib.request.Request(self.endpoint + path, data=data, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    resp.read()
                with self._lock:
                    self.sent += 1
                return True
            except urllib.error.HTTPError as exc:
                last = exc
                # 400 and 401 will not succeed on retry; 429 and 503 might.
                if exc.code not in (429, 503):
                    break
            except (urllib.error.URLError, OSError) as exc:
                last = exc
            if attempt < self.retries - 1:
                time.sleep(delay)
                delay *= 2

        with self._lock:
            self.failed += 1
        # The message names the endpoint and the failure, never the body:
        # the body is the payload, and it does not belong in a log.
        msg = "trajectory: could not deliver to %s%s: %s" % (self.endpoint, path, _describe(last))
        if self.raise_errors:
            raise RuntimeError(msg) from last
        log.warning(msg)
        return False


def _describe(exc: Optional[BaseException]) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return "HTTP %d" % exc.code
    return type(exc).__name__ if exc else "unknown error"


class Episode:
    """One trajectory. Create it with :meth:`Client.episode`."""

    def __init__(
        self,
        client: Client,
        task_type: Optional[str] = None,
        group_id: Optional[str] = None,
        session_id: Optional[str] = None,
        raw: Optional[Dict[str, str]] = None,
    ):
        self._client = client
        self.session_id = session_id or str(uuid.uuid4())
        self.task_type = task_type
        self.group_id = group_id
        self.raw = {k: str(v) for k, v in (raw or {}).items()}
        self._idx = 0
        self._last_span: Optional[str] = None
        self._closed = False
        # Each step is held until the next one arrives, so the final step can
        # carry the terminal marker itself. The alternative — a synthetic
        # empty step at the end — would put a fake step into every
        # trajectory a trainer reads. The cost is that a crash loses the one
        # step being held, which is the same bound streaming promises.
        self._pending: Optional[tuple] = None

    # -- step constructors -------------------------------------------------

    def llm(
        self,
        prompt: Any,
        completion: Any,
        *,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        tokens: Optional[Dict[str, int]] = None,
        token_spans: Optional[List[TokenSpan]] = None,
        trainable: Optional[bool] = None,
        finish_reason: Optional[str] = None,
        parent: Optional[str] = None,
        attempt: int = 0,
        latency_ms: Optional[int] = None,
        cost_usd: Optional[float] = None,
    ) -> str:
        """Record a model call. Returns the step's span id, for ``parent=``."""
        return self._step(
            "llm", {"input": prompt, "output": completion},
            model=model, provider=provider, params=params, tokens=tokens,
            token_spans=token_spans, trainable=trainable,
            finish_reason=finish_reason, parent=parent, attempt=attempt,
            latency_ms=latency_ms, cost_usd=cost_usd,
        )

    def tool(
        self,
        name: str,
        *,
        args: Any = None,
        result: Any = None,
        version: Optional[str] = None,
        error: Optional[str] = None,
        parent: Optional[str] = None,
        attempt: int = 0,
        latency_ms: Optional[int] = None,
    ) -> str:
        """Record a tool call. Pass the same ``parent`` and a higher
        ``attempt`` for a retry, so it is kept as a branch rather than
        flattened into a line (F-3.4)."""
        return self._step(
            "tool", {"args": args, "result": result},
            tool_name=name, tool_version=version, error=error,
            parent=parent, attempt=attempt, latency_ms=latency_ms,
        )

    def retrieval(self, query: Any, results: Any, **kw: Any) -> str:
        return self._step("retrieval", {"input": query, "output": results}, **kw)

    def human(self, content: Any, **kw: Any) -> str:
        """Record a human intervention — an edit, an approval, a correction."""
        return self._step("human", {"input": content}, **kw)

    # -- plumbing ----------------------------------------------------------

    def _step(self, kind: str, content: Dict[str, Any], **kw: Any) -> str:
        if self._closed:
            raise RuntimeError("episode is already closed")
        if kind not in _KINDS:
            raise ValueError("unknown step kind %r" % kind)

        span_id = "%s-%d" % (self.session_id, self._idx)
        step: Dict[str, Any] = {
            "kind": kind,
            "content": json.dumps({k: v for k, v in content.items() if v is not None}),
            "started_at": _now_us(),
            "attempt": kw.get("attempt", 0),
            "trainable": _TRAINABLE[kw.get("trainable")],
        }
        for key, field in (("model", "model"), ("provider", "provider"),
                           ("tool_name", "tool_name"), ("tool_version", "tool_version"),
                           ("finish_reason", "finish_reason"), ("latency_ms", "latency_ms"),
                           ("cost_usd", "cost_usd")):
            if kw.get(key) is not None:
                step[field] = kw[key]
        if kw.get("params"):
            step["params"] = kw["params"]
        if kw.get("tokens"):
            step["token_counts"] = kw["tokens"]
        if kw.get("token_spans"):
            step["token_spans"] = [s.to_json() for s in kw["token_spans"]]
        if kw.get("error"):
            step["error"] = {"type": "tool_error", "message": str(kw["error"])[:500]}

        self._idx += 1
        self._flush_pending(terminal=False)
        self._pending = (span_id, step, kw.get("parent") or self._last_span or "")
        self._last_span = span_id
        return span_id

    def _flush_pending(self, terminal: bool, extra: Optional[Dict[str, Any]] = None) -> None:
        if self._pending is None:
            return
        span_id, step, parent = self._pending
        self._pending = None
        self._send(span_id, step, parent=parent, terminal=terminal, extra=extra)

    def end(self, error: Optional[tuple] = None) -> None:
        """Close the episode. Called for you by ``with client.episode()``.

        The last step goes out carrying the terminal marker, so the collector
        can close the episode after its settle period instead of waiting out
        the whole window. An episode with no steps sends nothing.
        """
        if self._closed:
            return
        self._closed = True
        extra: Dict[str, Any] = {}
        if error:
            extra["episode_error"] = {"type": error[0], "message": error[1]}
        self._flush_pending(terminal=True, extra=extra)

    def _send(self, span_id: str, step: Dict[str, Any], *, parent: str,
              terminal: bool, extra: Optional[Dict[str, Any]] = None) -> None:
        body: Dict[str, Any] = {
            "session_id": self.session_id,
            # The stored episode_id, so ``ep.session_id`` is what you pass to
            # ``cc replay`` to find this trajectory again.
            "episode_id": self.session_id,
            "span_id": span_id,
            "parent_span_id": parent,
            "terminal": terminal,
            "instrumentation": "trajectory-python",
            "instrumentation_version": __version__,
            "step": step,
        }
        if self.task_type:
            body["task_type"] = self.task_type
        if self.group_id:
            body["group_id"] = self.group_id
        if self.raw:
            body["episode_raw"] = self.raw
        if extra:
            body.update(extra)
        self._client._post("/v1/spans", body)
