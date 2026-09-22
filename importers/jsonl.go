// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package importers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/KatyarAILabs/trajectory/collector/pipeline"
	"github.com/KatyarAILabs/trajectory/collector/wire"
)

func init() {
	register(&JSONL{})
	register(&NativeJSON{})
}

// JSONL imports newline-delimited native episodes — one episode per line.
//
// This is the format to reach for when a partner's export is not one of the
// vendor formats: a short script that emits one JSON episode per line is
// usually less work than writing an importer, and it exercises exactly the
// same code path.
type JSONL struct{}

func (*JSONL) Name() string { return "jsonl" }
func (*JSONL) Describe() string {
	return "newline-delimited native episodes, one JSON episode per line"
}

func (*JSONL) Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error) {
	var st Stats

	sc := bufio.NewScanner(r)
	// Episodes carry whole prompts, so the default 64 KiB line limit is far
	// too small; a truncated line would be a silently corrupted import.
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)

	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		eps, err := wire.DecodeEpisodes([]byte(text))
		if err != nil {
			st.Warn("line %d: %v", line, err)
			continue
		}

		for _, ep := range eps {
			envs, err := ep.ToEnvelopes(sourceName, "")
			if err != nil {
				st.Warn("line %d: %v", line, err)
				continue
			}
			for _, env := range envs {
				if err := emit(env); err != nil {
					return st, err
				}
				st.Spans++
			}
			st.Records++
		}
	}

	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("read: %w", err)
	}
	return st, nil
}

// NativeJSON imports a single JSON document: one episode, or an array of them.
type NativeJSON struct{}

func (*NativeJSON) Name() string { return "native" }
func (*NativeJSON) Describe() string {
	return "a single JSON document: one native episode or an array of them"
}

func (*NativeJSON) Import(r io.Reader, sourceName string, emit func(pipeline.Envelope) error) (Stats, error) {
	var st Stats

	body, err := io.ReadAll(r)
	if err != nil {
		return st, fmt.Errorf("read: %w", err)
	}

	eps, err := wire.DecodeEpisodes(body)
	if err != nil {
		return st, err
	}

	for i, ep := range eps {
		envs, err := ep.ToEnvelopes(sourceName, "")
		if err != nil {
			st.Warn("episode %d: %v", i, err)
			continue
		}
		for _, env := range envs {
			if err := emit(env); err != nil {
				return st, err
			}
			st.Spans++
		}
		st.Records++
	}
	return st, nil
}

// decodeLoose is shared by the vendor importers, which all wrap their records
// in some envelope or other and are inconsistent about whether an export is an
// array, an object with a data key, or newline-delimited.
func decodeLoose(body []byte, dataKeys ...string) ([]json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, fmt.Errorf("empty input")
	}

	if trimmed[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(body, &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}

	if trimmed[0] == '{' {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(body, &obj); err == nil {
			for _, k := range dataKeys {
				if raw, ok := obj[k]; ok {
					var arr []json.RawMessage
					if err := json.Unmarshal(raw, &arr); err == nil {
						return arr, nil
					}
				}
			}
			// A bare object is a single record.
			return []json.RawMessage{json.RawMessage(body)}, nil
		}
	}

	// Fall back to newline-delimited, which several tools emit despite
	// documenting an array.
	var out []json.RawMessage
	for _, line := range strings.Split(trimmed, "\n") {
		l := strings.TrimSpace(line)
		if l == "" {
			continue
		}
		out = append(out, json.RawMessage(l))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("could not parse input as an array, object or newline-delimited JSON")
	}
	return out, nil
}
