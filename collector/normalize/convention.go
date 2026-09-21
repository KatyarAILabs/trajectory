// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package normalize

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed all:conventions
var builtinFS embed.FS

// Canonical field names. A mapping file maps producer attributes onto these,
// and nothing else in the pipeline knows a producer's attribute names.
const (
	FSessionKey       = "session_key"
	FGroupID          = "group_id"
	FTaskType         = "task_type"
	FEpisodeEnd       = "episode_end"
	FInput            = "input"
	FOutput           = "output"
	FRole             = "role"
	FModel            = "model"
	FProvider         = "provider"
	FFinishReason     = "finish_reason"
	FToolName         = "tool_name"
	FToolVersion      = "tool_version"
	FToolArgs         = "tool_args"
	FInvocationParams = "invocation_params"
	FTemperature      = "temperature"
	FTopP             = "top_p"
	FMaxTokens        = "max_tokens"
	FSeed             = "seed"
	FStopSequences    = "stop_sequences"
	FTokenInput       = "token_input"
	FTokenOutput      = "token_output"
	FTokenCached      = "token_cached"
	FTokenReasoning   = "token_reasoning"
	FCost             = "cost"
)

// knownFields is every canonical field the mapper understands. A mapping file
// naming anything else is a config error rather than a silently ignored line —
// a typo'd field name would otherwise look like working config that quietly
// drops data.
var knownFields = map[string]bool{
	FSessionKey: true, FGroupID: true, FTaskType: true, FEpisodeEnd: true,
	FInput: true, FOutput: true, FRole: true,
	FModel: true, FProvider: true, FFinishReason: true,
	FToolName: true, FToolVersion: true, FToolArgs: true,
	FInvocationParams: true,
	FTemperature:      true, FTopP: true, FMaxTokens: true, FSeed: true, FStopSequences: true,
	FTokenInput: true, FTokenOutput: true, FTokenCached: true, FTokenReasoning: true,
	FCost: true,
}

// KindMapping maps a producer's span-kind vocabulary onto canonical kinds.
type KindMapping struct {
	Attribute string            `yaml:"attribute"`
	Default   string            `yaml:"default"`
	Values    map[string]string `yaml:"values"`
}

// Convention is one producer convention, loaded from a mapping file.
type Convention struct {
	Name    string      `yaml:"name"`
	Version string      `yaml:"version"`
	Detect  []string    `yaml:"detect"`
	Kind    KindMapping `yaml:"kind"`
	// Fields maps a canonical field name onto the producer attributes that
	// may carry it, in precedence order.
	Fields map[string][]string `yaml:"fields"`

	// consumed is every attribute this convention reads, so the rest can be
	// preserved in raw (F-2.2).
	consumed map[string]bool
}

// Registry holds the loaded conventions and picks one per span.
type Registry struct {
	conventions []*Convention
}

// LoadBuiltins loads the conventions compiled into the binary.
func LoadBuiltins() (*Registry, error) {
	var files []string
	err := fs.WalkDir(builtinFS, "conventions", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".yaml") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	r := &Registry{}
	for _, f := range files {
		b, err := builtinFS.ReadFile(f)
		if err != nil {
			return nil, err
		}
		c, err := parseConvention(b, f)
		if err != nil {
			return nil, err
		}
		r.conventions = append(r.conventions, c)
	}
	return r, nil
}

// LoadDir loads conventions from a directory, replacing any builtin with the
// same name. This is how an operator pins a mapping against a producer that has
// moved ahead of the shipped one, without waiting for a release.
func (r *Registry) LoadDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("mappings dir %s: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		c, err := parseConvention(b, path)
		if err != nil {
			return err
		}
		r.replace(c)
	}
	return nil
}

func (r *Registry) replace(c *Convention) {
	for i, existing := range r.conventions {
		if existing.Name == c.Name {
			r.conventions[i] = c
			return
		}
	}
	r.conventions = append(r.conventions, c)
}

// Names lists loaded conventions, for startup logging and `cc validate`.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.conventions))
	for _, c := range r.conventions {
		out = append(out, c.Name+"@"+c.Version)
	}
	return out
}

func parseConvention(b []byte, path string) (*Convention, error) {
	var c Convention
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("mapping %s: %w", path, err)
	}

	if c.Name == "" {
		return nil, fmt.Errorf("mapping %s: name is required", path)
	}
	if c.Kind.Default == "" {
		// Defaulting to "other" rather than erroring, because F-2.5 is
		// unambiguous that an unknown kind is retained.
		c.Kind.Default = "other"
	}

	c.consumed = map[string]bool{}
	if c.Kind.Attribute != "" {
		c.consumed[c.Kind.Attribute] = true
	}

	for field, attrs := range c.Fields {
		if !knownFields[field] {
			return nil, fmt.Errorf(
				"mapping %s: unknown canonical field %q; known fields are %s",
				path, field, strings.Join(sortedFields(), ", "))
		}
		for _, a := range attrs {
			c.consumed[a] = true
		}
	}
	return &c, nil
}

func sortedFields() []string {
	out := make([]string, 0, len(knownFields))
	for f := range knownFields {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// Select picks the convention for a span.
//
// Detection is by attribute presence rather than by instrumentation scope name,
// because a scope name is free text that a producer can set to anything, while
// the presence of a convention's own attributes is what actually determines
// whether its mapping applies.
//
// A span matching nothing still normalises: it gets the first convention as a
// fallback, which maps nothing and therefore preserves every attribute in raw.
// Dropping it would lose evidence (F-2.5).
func (r *Registry) Select(attrs map[string]string) *Convention {
	for _, c := range r.conventions {
		for _, d := range c.Detect {
			if _, ok := attrs[d]; ok {
				return c
			}
		}
	}
	if len(r.conventions) > 0 {
		return r.conventions[0]
	}
	return &Convention{Name: "unknown", Kind: KindMapping{Default: "other"}, consumed: map[string]bool{}}
}

// Get returns the first present attribute for a canonical field.
func (c *Convention) Get(attrs map[string]string, field string) (string, bool) {
	for _, a := range c.Fields[field] {
		if v, ok := attrs[a]; ok && v != "" {
			return v, true
		}
	}
	return "", false
}

// MapKind resolves the canonical step kind, defaulting rather than dropping
// (F-2.5).
func (c *Convention) MapKind(attrs map[string]string) string {
	if c.Kind.Attribute == "" {
		return c.Kind.Default
	}
	raw, ok := attrs[c.Kind.Attribute]
	if !ok {
		return c.Kind.Default
	}

	// Producers are inconsistent about case and padding for what is
	// nominally a closed enum, so match leniently before falling back.
	if v, ok := c.Kind.Values[strings.TrimSpace(raw)]; ok {
		return v
	}
	up := strings.ToUpper(strings.TrimSpace(raw))
	for k, v := range c.Kind.Values {
		if strings.ToUpper(k) == up {
			return v
		}
	}
	return c.Kind.Default
}

// Unmapped returns the attributes this convention did not consume, which are
// preserved verbatim so normalisation stays lossless (F-2.2).
func (c *Convention) Unmapped(attrs map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range attrs {
		if !c.consumed[k] {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
