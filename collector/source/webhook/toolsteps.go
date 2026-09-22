// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"encoding/json"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// toolStepsFromMessages rebuilds tool steps from OpenAI-format history.
//
// A gateway sees a tool call twice, never the call itself: the model's request
// (an assistant message with tool_calls, carrying the arguments) and, on the
// next turn, the result the application fed back (a role "tool" message with
// the same tool_call_id). Pairing them gives a tool step with args and result —
// which is what entity extraction addresses — without the gateway ever seeing
// the tool run.
//
// Every later call repeats the whole history, so the same pair arrives again
// and again. The tool_call_id is used as the span id, which is stable across
// those repeats, so the assembler's existing deduplication collapses them into
// one step instead of this code having to remember what it already sent.
//
// What cannot be recovered is stated rather than invented: there is no tool
// version and no tool timing, because the tool ran in the application. Such a
// step is timestamped just before the call that consumed its result, so it
// orders correctly, and fidelity reports has_tool_versions false.
func toolStepsFromMessages(messages any, call pipeline.Envelope) []pipeline.Envelope {
	list, ok := messages.([]any)
	if !ok {
		return nil
	}

	type request struct {
		name string
		args any
	}
	requested := map[string]request{}

	var out []pipeline.Envelope
	for _, m := range list {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}

		switch msg["role"] {
		case "assistant":
			calls, _ := msg["tool_calls"].([]any)
			for _, c := range calls {
				tc, ok := c.(map[string]any)
				if !ok {
					continue
				}
				id, _ := tc["id"].(string)
				fn, _ := tc["function"].(map[string]any)
				if id == "" || fn == nil {
					continue
				}
				name, _ := fn["name"].(string)
				requested[id] = request{name: name, args: parseMaybeJSON(fn["arguments"])}
			}

		case "tool":
			id, _ := msg["tool_call_id"].(string)
			req, ok := requested[id]
			if id == "" || !ok {
				continue
			}

			payload, err := json.Marshal(map[string]any{
				"args":   req.args,
				"result": parseMaybeJSON(msg["content"]),
			})
			if err != nil {
				continue
			}
			content := string(payload)
			name := req.name

			started := call.Step.StartedAt - 1
			if started < 0 {
				started = 0
			}

			out = append(out, pipeline.Envelope{
				SessionKey: call.SessionKey,
				Source:     call.Source,
				SpanID:     "tool:" + id,
				Meta:       call.Meta,
				Step: record.Step{
					Kind:          record.KindTool,
					ToolName:      &name,
					ContentInline: &content,
					StartedAt:     started,
					Trainable:     record.TrainableUnknown,
				},
			})
		}
	}
	return out
}

// parseMaybeJSON decodes a string that holds JSON — OpenAI sends tool
// arguments as a JSON-encoded string — so paths like $.args.id reach into it.
// Anything else is returned unchanged.
func parseMaybeJSON(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	var out any
	if err := json.Unmarshal([]byte(s), &out); err == nil {
		return out
	}
	return s
}
