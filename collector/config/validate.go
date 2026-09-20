// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/trajectory-project/trajectory/internal/version"
)

// Validate reports every problem it can find, not just the first, so a user
// fixes one round of errors rather than discovering them one restart at a
// time. Each message names the key and the reason (F-11.6).
func (c *Config) Validate(path string) error {
	var errs []string
	bad := func(key, reason string, args ...any) {
		errs = append(errs, fmt.Sprintf("  %s: %s", key, fmt.Sprintf(reason, args...)))
	}

	if c.SchemaVersion == "" {
		bad("schema_version", "required")
	} else if c.SchemaVersion != version.Schema {
		// A mismatch means the operator expects a different record shape
		// than this binary writes. Refusing is safer than writing files
		// they will not be able to read as they expect.
		bad("schema_version", "is %q but this build writes %q",
			c.SchemaVersion, version.Schema)
	}
	if c.Tenant == "" {
		bad("tenant", "required; v1 is single-tenant per deployment")
	}

	c.validateSources(bad)
	c.validateAssembly(bad)
	c.validateRedaction(bad)
	c.validateEntities(bad)
	c.validateSinks(bad)

	if len(errs) > 0 {
		return fmt.Errorf("config %s is invalid:\n%s", path, strings.Join(errs, "\n"))
	}
	return nil
}

func (c *Config) validateSources(bad func(string, string, ...any)) {
	if len(c.Sources) == 0 {
		bad("sources", "at least one source is required")
	}
	seen := map[string]bool{}
	for i, s := range c.Sources {
		key := fmt.Sprintf("sources[%d]", i)
		if s.Name == "" {
			bad(key+".name", "required; the name is attributed onto every record this source produces")
		}
		if seen[s.Name] {
			bad(key+".name", "duplicate source name %q; names must be unique to stay attributable", s.Name)
		}
		seen[s.Name] = true

		if s.Type != "otlp" {
			bad(key+".type", "%q is not supported in this build; only \"otlp\" is", s.Type)
		}
		if s.HTTP.Listen == "" {
			bad(key+".http.listen", "required")
		}
	}
}

func (c *Config) validateAssembly(bad func(string, string, ...any)) {
	if c.Assembly.Window <= 0 {
		bad("assembly.window", "must be positive")
	}
	if c.Assembly.MaxInFlight <= 0 {
		bad("assembly.max_in_flight", "must be positive; an unbounded assembly buffer is an OOM (F-3.7)")
	}
	if c.Assembly.SettleAfterTerminal < 0 {
		bad("assembly.settle_after_terminal", "must not be negative")
	}
	if c.Assembly.SettleAfterTerminal >= c.Assembly.Window {
		bad("assembly.settle_after_terminal",
			"(%s) must be shorter than assembly.window (%s), or a terminal marker "+
				"gains nothing over waiting for the window to expire",
			c.Assembly.SettleAfterTerminal, c.Assembly.Window)
	}
}

func (c *Config) validateRedaction(bad func(string, string, ...any)) {
	switch c.Redaction.Default {
	case "deny", "allow":
	default:
		bad("redaction.default", "must be \"deny\" or \"allow\", got %q", c.Redaction.Default)
	}

	switch c.Redaction.OnError {
	case "quarantine", "pass":
	default:
		bad("redaction.on_error", "must be \"quarantine\" or \"pass\", got %q", c.Redaction.OnError)
	}
	// "pass" is legal but it is the one setting that turns a policy failure
	// into a leak, so it must never be reached by a copied config or a
	// careless default. Requiring a second, out-of-band signal means an
	// operator cannot enable it without knowing they did.
	if c.Redaction.OnError == "pass" && os.Getenv("CC_ALLOW_REDACTION_PASS") != "1" {
		bad("redaction.on_error", "\"pass\" disables fail-closed (F-5.5); "+
			"if that is deliberate, set CC_ALLOW_REDACTION_PASS=1 as well")
	}

	needsKey := false
	for i, r := range c.Redaction.Rules {
		key := fmt.Sprintf("redaction.rules[%d]", i)
		if r.ID == "" {
			bad(key+".id", "required; the id appears in the redaction manifest and in metrics")
		}
		if r.Match.Regex == "" {
			bad(key+".match.regex", "required")
		} else if _, err := regexp.Compile(r.Match.Regex); err != nil {
			bad(key+".match.regex", "does not compile: %v", err)
		}
		switch r.Action {
		case "tokenize":
			needsKey = true
		case "drop":
		default:
			bad(key+".action", "must be \"tokenize\" or \"drop\", got %q", r.Action)
		}
	}

	if needsKey {
		env := c.Redaction.Tokenization.KeyEnv
		if env == "" {
			bad("redaction.tokenization.key_env", "required when any rule uses action \"tokenize\"")
		} else if os.Getenv(env) == "" {
			bad("redaction.tokenization.key_env",
				"environment variable %q is unset or empty; "+
					"tokenization without a key would produce unjoinable records", env)
		}
	}
}

func (c *Config) validateEntities(bad func(string, string, ...any)) {
	for i, e := range c.Entities {
		key := fmt.Sprintf("entities[%d]", i)
		if e.Tool == "" {
			bad(key+".tool", "required")
		}
		if len(e.Keys) == 0 {
			bad(key+".keys", "required; an entity with no keys extracts nothing")
		}
		for name, expr := range e.Keys {
			if !strings.HasPrefix(expr, "$") {
				bad(fmt.Sprintf("%s.keys.%s", key, name),
					"must be a JSONPath starting with $, got %q", expr)
			}
		}
	}
}

func (c *Config) validateSinks(bad func(string, string, ...any)) {
	if len(c.Sinks) == 0 {
		bad("sinks", "at least one sink is required")
	}
	for i, s := range c.Sinks {
		key := fmt.Sprintf("sinks[%d]", i)
		if s.Name == "" {
			bad(key+".name", "required")
		}
		if s.Type != "fs" {
			bad(key+".type", "%q is not supported in this build; only \"fs\" is", s.Type)
		}
		if s.Dir == "" {
			bad(key+".dir", "required")
		}
		if s.BlobThresholdBytes < 0 {
			bad(key+".blob_threshold_bytes", "must not be negative")
		}
		if s.MaxPayloadBytes > 0 && s.MaxPayloadBytes < s.BlobThresholdBytes {
			bad(key+".max_payload_bytes",
				"(%d) is below blob_threshold_bytes (%d), so every externalised payload would be truncated",
				s.MaxPayloadBytes, s.BlobThresholdBytes)
		}
		for _, p := range s.PartitionBy {
			switch p {
			case "dt", "tenant", "task_type":
			default:
				bad(key+".partition_by",
					"%q is not a partition key this build understands (dt, tenant, task_type)", p)
			}
		}
	}
}
