# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import trajectory  # noqa: E402


class Recorder(BaseHTTPRequestHandler):
    bodies = []
    status = 202
    auth = []

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        Recorder.bodies.append((self.path, json.loads(self.rfile.read(n))))
        Recorder.auth.append(self.headers.get("Authorization"))
        self.send_response(Recorder.status)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), Recorder)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d" % cls.srv.server_port

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        Recorder.bodies, Recorder.auth, Recorder.status = [], [], 202

    def test_steps_stream_and_last_is_terminal(self):
        c = trajectory.Client(self.url, raise_errors=True)
        with c.episode(task_type="refund", group_id="rollout-3") as ep:
            a = ep.llm("plan", "look up order", model="m", params={"seed": 7})
            ep.tool("zendesk.update_ticket", args={"id": 1}, result={"ok": True},
                    version="2.3.1", parent=a)

        paths = [p for p, _ in Recorder.bodies]
        self.assertEqual(paths, ["/v1/spans", "/v1/spans"])
        first, last = Recorder.bodies[0][1], Recorder.bodies[1][1]

        # No synthetic trailing step: exactly the two real ones went out.
        self.assertEqual(len(Recorder.bodies), 2)
        self.assertFalse(first["terminal"])
        self.assertTrue(last["terminal"], "the final real step must carry the terminal marker")

        self.assertEqual(first["session_id"], last["session_id"])
        self.assertEqual(first["group_id"], "rollout-3")
        self.assertEqual(first["step"]["params"]["seed"], 7)
        self.assertEqual(last["step"]["tool_version"], "2.3.1")
        self.assertEqual(last["parent_span_id"], first["span_id"])
        self.assertEqual(json.loads(last["step"]["content"])["args"], {"id": 1})

    def test_retry_is_a_branch(self):
        c = trajectory.Client(self.url, raise_errors=True)
        with c.episode() as ep:
            plan = ep.llm("p", "c")
            ep.tool("t", args={}, error="rate limited", parent=plan)
            ep.tool("t", args={}, result={}, parent=plan, attempt=1)
        tools = [b for _, b in Recorder.bodies if b["step"]["kind"] == "tool"]
        self.assertEqual([t["parent_span_id"] for t in tools], [tools[0]["parent_span_id"]] * 2)
        self.assertEqual(tools[1]["step"]["attempt"], 1)

    def test_exception_is_recorded_and_reraised(self):
        c = trajectory.Client(self.url, raise_errors=True)
        with self.assertRaises(ValueError):
            with c.episode() as ep:
                ep.llm("p", "c")
                raise ValueError("agent crashed")
        last = Recorder.bodies[-1][1]
        self.assertTrue(last["terminal"])
        self.assertEqual(last["episode_error"]["type"], "ValueError")

    def test_token_spans_and_unknown_trainable(self):
        c = trajectory.Client(self.url, raise_errors=True)
        with c.episode() as ep:
            ep.llm("p", "c", trainable=True, token_spans=[
                trajectory.TokenSpan(0, 5, False), trajectory.TokenSpan(5, 9, None)])
            ep.tool("t")
        llm = Recorder.bodies[0][1]["step"]
        self.assertEqual(llm["trainable"], "true")
        self.assertEqual([s["trainable"] for s in llm["token_spans"]], ["false", "unknown"])
        # A step that said nothing about trainability is unknown, never false.
        self.assertEqual(Recorder.bodies[1][1]["step"]["trainable"], "unknown")

    def test_never_raises_into_the_agent_by_default(self):
        c = trajectory.Client("http://127.0.0.1:1", retries=1, timeout=0.2)
        with c.episode() as ep:  # must not raise even though delivery fails
            ep.llm("p", "c")
        self.assertEqual(c.failed, 1)

    def test_bearer_token(self):
        c = trajectory.Client(self.url, token="secret", raise_errors=True)
        with c.episode() as ep:
            ep.llm("p", "c")
        self.assertEqual(Recorder.auth[0], "Bearer secret")

    def test_client_error_is_not_retried(self):
        Recorder.status = 400
        c = trajectory.Client(self.url, retries=3)
        with c.episode() as ep:
            ep.llm("p", "c")
        self.assertEqual(len(Recorder.bodies), 1, "a 400 will not succeed on retry")

    def test_reversed_token_span_rejected(self):
        with self.assertRaises(ValueError):
            trajectory.TokenSpan(9, 2, True)


if __name__ == "__main__":
    unittest.main()
