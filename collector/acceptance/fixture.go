// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package acceptance holds the end-to-end test that defines Phase 1 done.
package acceptance

import (
	"encoding/json"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Base time for the fixture, so expected values are stable.
var base = time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)

// seededEmail is planted in a payload. It must never appear in any output.
const seededEmail = "alice@example.com"

// bigPrompt is over the 8 KiB blob threshold, so it must be externalised to a
// content-addressed blob rather than stored inline (F-9.3).
var bigPrompt = func() string {
	b := make([]byte, 12000)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return string(b)
}()

// OpenInferenceFixture builds a recorded OpenInference span stream for one
// refund-handling trajectory: an LLM plan, a tool call, a retry of that tool
// call, and a terminal LLM summary.
//
// It is deliberately not a happy path. It contains a retry (so the tree is
// exercised), a payload over the blob threshold (so externalisation is), a
// seeded email (so redaction is), an extractable ticket id (so extraction is),
// and an unmapped attribute (so lossless preservation is).
func OpenInferenceFixture() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "refund-agent")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("openinference-langchain")
	ss.Scope().SetVersion("0.1.14")

	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sid := func(n byte) pcommon.SpanID {
		return pcommon.SpanID([8]byte{n, 0, 0, 0, 0, 0, 0, 1})
	}

	// 1. The planning LLM call. Its prompt is large enough to be blobbed.
	s1 := ss.Spans().AppendEmpty()
	s1.SetTraceID(traceID)
	s1.SetSpanID(sid(1))
	s1.SetName("llm.plan")
	s1.SetStartTimestamp(ts(base))
	s1.SetEndTimestamp(ts(base.Add(900 * time.Millisecond)))
	a := s1.Attributes()
	a.PutStr("openinference.span.kind", "LLM")
	a.PutStr("session.id", "sess-refund-42")
	a.PutStr("input.value", bigPrompt)
	a.PutStr("output.value", "I will look up the order.")
	a.PutStr("llm.model_name", "claude-opus-5")
	a.PutStr("llm.provider", "anthropic")
	a.PutStr("llm.invocation_parameters", `{"temperature":0.2,"max_tokens":1024,"seed":7}`)
	a.PutInt("llm.token_count.prompt", 3200)
	a.PutInt("llm.token_count.completion", 12)
	a.PutStr("task.type", "refund")
	// An attribute this build does not map. It must survive verbatim in raw.
	a.PutStr("acme.internal.experiment", "arm-b")

	// 2. A tool call that fails. The seeded email rides in its result.
	s2 := ss.Spans().AppendEmpty()
	s2.SetTraceID(traceID)
	s2.SetSpanID(sid(2))
	s2.SetParentSpanID(sid(1))
	s2.SetName("tool.zendesk")
	s2.SetStartTimestamp(ts(base.Add(1 * time.Second)))
	s2.SetEndTimestamp(ts(base.Add(1200 * time.Millisecond)))
	a = s2.Attributes()
	a.PutStr("openinference.span.kind", "TOOL")
	a.PutStr("session.id", "sess-refund-42")
	a.PutStr("tool.name", "zendesk.update_ticket")
	a.PutStr("tool.version", "2.3.1")
	a.PutStr("tool.parameters", mustJSON(map[string]any{"id": "TKT-9001", "requester": seededEmail}))
	a.PutStr("output.value", `{"error":"rate limited"}`)
	s2.Status().SetCode(ptrace.StatusCodeError)
	s2.Status().SetMessage("rate limited")

	// 3. The retry of that tool call: same parent, so the branch is visible
	//    in the tree rather than flattened into a linear list.
	s3 := ss.Spans().AppendEmpty()
	s3.SetTraceID(traceID)
	s3.SetSpanID(sid(3))
	s3.SetParentSpanID(sid(1))
	s3.SetName("tool.zendesk")
	s3.SetStartTimestamp(ts(base.Add(2 * time.Second)))
	s3.SetEndTimestamp(ts(base.Add(2300 * time.Millisecond)))
	a = s3.Attributes()
	a.PutStr("openinference.span.kind", "TOOL")
	a.PutStr("session.id", "sess-refund-42")
	a.PutStr("tool.name", "zendesk.update_ticket")
	a.PutStr("tool.version", "2.3.1")
	a.PutStr("tool.parameters", mustJSON(map[string]any{"id": "TKT-9001", "requester": seededEmail}))
	a.PutStr("output.value", `{"ok":true,"ticket":"TKT-9001"}`)

	// 4. The terminal summary.
	s4 := ss.Spans().AppendEmpty()
	s4.SetTraceID(traceID)
	s4.SetSpanID(sid(4))
	s4.SetParentSpanID(sid(3))
	s4.SetName("llm.summarise")
	s4.SetStartTimestamp(ts(base.Add(3 * time.Second)))
	s4.SetEndTimestamp(ts(base.Add(3400 * time.Millisecond)))
	a = s4.Attributes()
	a.PutStr("openinference.span.kind", "LLM")
	a.PutStr("session.id", "sess-refund-42")
	a.PutStr("input.value", "summarise the outcome")
	a.PutStr("output.value", "Refund processed for ticket TKT-9001.")
	a.PutStr("llm.model_name", "claude-opus-5")
	a.PutStr("llm.invocation_parameters", `{"temperature":0.0}`)
	a.PutStr("episode.end", "true")

	return td
}

func ts(t time.Time) pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(t)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
