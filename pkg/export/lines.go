// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package export

import (
	"encoding/json"

	"github.com/KatyarAILabs/trajectory/pkg/join"
	"github.com/KatyarAILabs/trajectory/pkg/lakeread"
	"github.com/KatyarAILabs/trajectory/pkg/record"
)

type stepLine struct {
	Idx         int32              `json:"idx"`
	Parent      *int32             `json:"parent,omitempty"`
	Attempt     int32              `json:"attempt,omitempty"`
	Kind        string             `json:"kind"`
	ToolName    string             `json:"tool_name,omitempty"`
	ToolVersion string             `json:"tool_version,omitempty"`
	Model       string             `json:"model,omitempty"`
	Params      *record.Params     `json:"params,omitempty"`
	Content     json.RawMessage    `json:"content,omitempty"`
	Trainable   string             `json:"trainable"`
	TokenSpans  []record.TokenSpan `json:"token_spans,omitempty"`
	Error       *record.Error      `json:"error,omitempty"`
	Truncated   bool               `json:"truncated,omitempty"`
}

type trajectory struct {
	EpisodeID   string             `json:"episode_id"`
	TaskType    string             `json:"task_type,omitempty"`
	GroupID     string             `json:"group_id,omitempty"`
	LabelStatus string             `json:"label_status"`
	Reward      *float64           `json:"reward,omitempty"`
	Clauses     map[string]float64 `json:"clauses,omitempty"`
	Verifier    string             `json:"verifier,omitempty"`
	Outcomes    []join.Outcome     `json:"outcomes"`
	Fidelity    *record.Fidelity   `json:"fidelity,omitempty"`
	Steps       []stepLine         `json:"steps"`
}

func trajectoryLine(l *lakeread.Lake, lb join.Label, ep *lakeread.Episode, rw *record.Reward) (trajectory, error) {
	t := trajectory{
		EpisodeID: lb.EpisodeID, TaskType: lb.TaskType, GroupID: lb.GroupID,
		LabelStatus: lb.LabelStatus, Outcomes: lb.Outcomes, Fidelity: ep.Fidelity,
	}
	if rw != nil {
		r := rw.Reward
		t.Reward, t.Clauses = &r, rw.Clauses
		t.Verifier = rw.VerifierID + "@" + rw.VerifierVersion
	}
	for _, s := range ep.Steps {
		body, err := l.Payload(s)
		if err != nil {
			return t, err
		}
		sl := stepLine{
			Idx: s.StepIdx, Parent: s.ParentIdx, Attempt: s.Attempt, Kind: s.Kind,
			Params: s.Params, Trainable: s.Trainable, TokenSpans: s.TokenSpans,
			Error: s.Error, Truncated: s.Truncated,
		}
		if s.ToolName != nil {
			sl.ToolName = *s.ToolName
		}
		if s.ToolVersion != nil {
			sl.ToolVersion = *s.ToolVersion
		}
		if s.Model != nil {
			sl.Model = *s.Model
		}
		if body != "" {
			sl.Content = asJSON(body)
		}
		t.Steps = append(t.Steps, sl)
	}
	return t, nil
}

// writeChat emits one example per model call: the messages the model saw,
// then what it answered.
//
// The payload of an llm step is {"input": …, "output": …}. Input is either a
// message list (what a gateway logs) or a string (what most instrumentation
// logs); output is a string, an object with content, or an OpenAI choices
// list. All of those are normalised to the chat format a fine-tuning job reads.
// A step that cannot be read as a conversation is skipped, not guessed at.
func writeChat(enc *json.Encoder, l *lakeread.Lake, ep *lakeread.Episode, rw *record.Reward) (int, int, error) {
	n, unusable := 0, 0
	for _, s := range ep.Steps {
		if s.Kind != record.KindLLM {
			continue
		}
		body, err := l.Payload(s)
		if err != nil {
			return n, unusable, err
		}
		var p struct {
			Input  any `json:"input"`
			Output any `json:"output"`
		}
		if json.Unmarshal([]byte(body), &p) != nil {
			unusable++
			continue
		}
		msgs := toMessages(p.Input)
		reply, ok := toAssistant(p.Output)
		if len(msgs) == 0 || !ok {
			unusable++
			continue
		}
		meta := map[string]any{"episode_id": ep.EpisodeID, "step_idx": s.StepIdx}
		if rw != nil {
			meta["reward"] = rw.Reward
		}
		if err := enc.Encode(map[string]any{
			"messages": append(msgs, reply),
			"metadata": meta,
		}); err != nil {
			return n, unusable, err
		}
		n++
	}
	return n, unusable, nil
}

func toMessages(in any) []map[string]any {
	switch v := in.(type) {
	case string:
		if v == "" {
			return nil
		}
		return []map[string]any{{"role": "user", "content": v}}
	case []any:
		var out []map[string]any
		for _, m := range v {
			if mm, ok := m.(map[string]any); ok && mm["role"] != nil {
				out = append(out, mm)
			}
		}
		return out
	case map[string]any:
		if msgs, ok := v["messages"]; ok {
			return toMessages(msgs)
		}
	}
	return nil
}

func toAssistant(out any) (map[string]any, bool) {
	switch v := out.(type) {
	case string:
		return map[string]any{"role": "assistant", "content": v}, v != ""
	case []any: // OpenAI choices
		if len(v) == 0 {
			return nil, false
		}
		if c, ok := v[0].(map[string]any); ok {
			if m, ok := c["message"].(map[string]any); ok {
				msg := map[string]any{"role": "assistant", "content": m["content"]}
				if tc, ok := m["tool_calls"]; ok && tc != nil {
					msg["tool_calls"] = tc
				}
				return msg, true
			}
		}
	case map[string]any:
		if c, ok := v["content"]; ok {
			return map[string]any{"role": "assistant", "content": c}, true
		}
	}
	return nil, false
}

func firstPrompt(l *lakeread.Lake, ep *lakeread.Episode) []map[string]any {
	for _, s := range ep.Steps {
		if s.Kind != record.KindLLM {
			continue
		}
		body, err := l.Payload(s)
		if err != nil {
			return nil
		}
		var p struct {
			Input any `json:"input"`
		}
		if json.Unmarshal([]byte(body), &p) == nil {
			return toMessages(p.Input)
		}
	}
	return nil
}

func asJSON(s string) json.RawMessage {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	b, _ := json.Marshal(s)
	return b
}
