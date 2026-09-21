// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/ohler55/ojg/jp"
	"gopkg.in/yaml.v3"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// Mapping is a declarative gateway mapping file (F-1.3, F-2.4).
//
// Gateways post JSON bodies rather than span attributes, so a field is located
// by JSONPath into the callback body. The mapping is data: when a gateway
// renames a field, the fix is an edit to a YAML file, not a release.
type Mapping struct {
	Name      string              `yaml:"name"`
	Version   string              `yaml:"version"`
	Kind      string              `yaml:"kind"`
	FixedKind string              `yaml:"fixed_kind"`
	Fields    map[string][]string `yaml:"fields"`
	// ToolSteps rebuilds tool steps from conversation history. The only
	// supported value is "openai_messages". A gateway never sees a tool
	// run, but it does see the arguments the model asked for and the
	// result fed back, and those are what entity extraction needs.
	ToolSteps string `yaml:"tool_steps"`
	// Exclude lists top-level fields never kept in raw — typically ones
	// that identify the caller rather than the trajectory.
	Exclude []string `yaml:"exclude"`

	compiled map[string][]jp.Expr
	// roots are the top-level keys the mapping reads, so everything else
	// can be preserved in raw (§9.3: "unknown fields land in raw").
	roots map[string]bool
}

// knownFields are the canonical fields a gateway mapping may populate.
var knownFields = map[string]bool{
	"span_id": true, "session_key": true, "group_id": true, "task_type": true,
	"input": true, "output": true,
	"model": true, "provider": true, "finish_reason": true,
	"temperature": true, "top_p": true, "max_tokens": true, "seed": true,
	"token_input": true, "token_output": true, "cost": true, "error": true, "episode_end": true,
	"started_at": true, "ended_at": true,
}

// LoadMapping reads and compiles a mapping file.
func LoadMapping(path string) (*Mapping, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("mapping %s: %w", path, err)
	}
	return ParseMapping(b, path)
}

// ParseMapping compiles mapping YAML. An unknown field or a path that does not
// parse is an error at startup, not a silently empty column later.
func ParseMapping(b []byte, name string) (*Mapping, error) {
	var m Mapping
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("mapping %s: %w", name, err)
	}
	if m.Name == "" {
		return nil, fmt.Errorf("mapping %s: name is required", name)
	}
	if m.Kind != "fixed" {
		return nil, fmt.Errorf("mapping %s: kind must be \"fixed\" for a gateway mapping", name)
	}
	switch m.FixedKind {
	case record.KindLLM, record.KindTool, record.KindRetrieval, record.KindHuman, record.KindOther:
	default:
		return nil, fmt.Errorf("mapping %s: fixed_kind %q is not a step kind", name, m.FixedKind)
	}

	switch m.ToolSteps {
	case "", "openai_messages":
	default:
		return nil, fmt.Errorf("mapping %s: tool_steps must be \"openai_messages\" or empty, got %q",
			name, m.ToolSteps)
	}

	m.compiled = map[string][]jp.Expr{}
	m.roots = map[string]bool{}
	for _, e := range m.Exclude {
		m.roots[e] = true // excluded fields are treated as consumed: never kept in raw
	}

	fields := make([]string, 0, len(m.Fields))
	for f := range m.Fields {
		fields = append(fields, f)
	}
	sort.Strings(fields)

	for _, field := range fields {
		if !knownFields[field] {
			return nil, fmt.Errorf("mapping %s: unknown field %q", name, field)
		}
		for _, src := range m.Fields[field] {
			expr, err := jp.ParseString(src)
			if err != nil {
				return nil, fmt.Errorf("mapping %s: field %s: cannot parse %q: %w", name, field, src, err)
			}
			m.compiled[field] = append(m.compiled[field], expr)
			if root := rootKey(src); root != "" {
				m.roots[root] = true
			}
		}
	}
	return &m, nil
}

// rootKey returns the top-level key a path reads: $.a.b -> a.
func rootKey(path string) string {
	p := strings.TrimPrefix(path, "$.")
	for i, c := range p {
		if c == '.' || c == '[' {
			return p[:i]
		}
	}
	return p
}

// get returns the first path that yields a value.
func (m *Mapping) get(doc any, field string) (any, bool) {
	for _, expr := range m.compiled[field] {
		if res := expr.Get(doc); len(res) > 0 && res[0] != nil {
			return res[0], true
		}
	}
	return nil, false
}
