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
	"token_input": true, "token_output": true, "cost": true,
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

	m.compiled = map[string][]jp.Expr{}
	m.roots = map[string]bool{}

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
