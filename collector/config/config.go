// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package config loads and validates the collector's YAML configuration.
//
// Decoding is strict: an unknown key is an error, not a warning (§10). A typo
// in a redaction rule that silently does nothing is exactly the failure this
// prevents, and it is why yaml.v3 with KnownFields is used rather than a
// lenient loader.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole configuration surface the walking skeleton supports.
// Phase 1 accepts one source, one sink and one redaction policy; the shape is
// already plural so widening does not change the file format.
type Config struct {
	SchemaVersion string `yaml:"schema_version"`
	Tenant        string `yaml:"tenant"`

	Sources   []Source  `yaml:"sources"`
	Assembly  Assembly  `yaml:"assembly"`
	Redaction Redaction `yaml:"redaction"`
	Entities  []Entity  `yaml:"entities"`
	Sinks     []Sink    `yaml:"sinks"`
	Telemetry Telemetry `yaml:"telemetry"`
}

type Source struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	HTTP struct {
		Listen string `yaml:"listen"`
	} `yaml:"http"`
	// MaxRequestBytes rejects oversized requests with a clear error and a
	// metric rather than buffering them (F-1.7).
	MaxRequestBytes int64 `yaml:"max_request_bytes"`
}

type Assembly struct {
	// SessionKey is an ordered list of attributes to group by; the first
	// one present on a span wins (F-3.1).
	SessionKey []string `yaml:"session_key"`
	// Window tolerates out-of-order arrival (F-3.2).
	Window time.Duration `yaml:"window"`
	// MaxInFlight bounds the assembly buffer. At capacity the oldest
	// episode is emitted early marked `evicted` rather than growing
	// memory (F-3.7).
	MaxInFlight int `yaml:"max_in_flight"`
	// SettleAfterTerminal keeps an episode open briefly after its terminal
	// marker so spans still in flight are absorbed rather than splitting
	// into a second episode. See assemble.Options for why this exists.
	SettleAfterTerminal time.Duration `yaml:"settle_after_terminal"`
}

type Redaction struct {
	// Default is `deny` or `allow`. Deny-by-default means only
	// allow-listed paths survive.
	Default string `yaml:"default"`
	// OnError is `quarantine` or `pass`. Closed is the default (F-5.5).
	OnError string   `yaml:"on_error"`
	Allow   []string `yaml:"allow"`
	Rules   []Rule   `yaml:"rules"`

	Tokenization struct {
		// KeyEnv names the environment variable holding the HMAC key.
		// Secrets come from env, file or secret manager, never inline
		// (F-11.1).
		KeyEnv string `yaml:"key_env"`
	} `yaml:"tokenization"`
}

type Rule struct {
	ID    string `yaml:"id"`
	Match struct {
		Regex string `yaml:"regex"`
	} `yaml:"match"`
	// Action is `tokenize` or `drop`.
	Action string `yaml:"action"`
}

type Entity struct {
	Tool string            `yaml:"tool"`
	Keys map[string]string `yaml:"keys"`
}

type Sink struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Dir  string `yaml:"dir"`
	// PartitionBy is the directory partitioning under each table prefix
	// (F-9.2).
	PartitionBy []string `yaml:"partition_by"`
	// BlobThresholdBytes is the payload size above which content is
	// externalised to a content-addressed blob (F-9.3).
	BlobThresholdBytes int `yaml:"blob_threshold_bytes"`
	// MaxPayloadBytes truncates with an explicit marker rather than
	// failing the record (F-9.7).
	MaxPayloadBytes int `yaml:"max_payload_bytes"`
}

type Telemetry struct {
	Metrics struct {
		Listen string `yaml:"listen"`
	} `yaml:"metrics"`
	LogLevel string `yaml:"log_level"`
}

// envRef matches ${VAR} for interpolation (F-11.1).
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load reads, interpolates, decodes and validates a config file.
//
// Every error names the file, and where possible the key and the reason
// (F-11.6). The collector refuses to start rather than running a config it
// only partly understood.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}

	interpolated, err := interpolate(string(raw), path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(interpolated))
	// This is the whole point of §10's "unknown keys are an error, not a
	// warning": a misspelled key must not silently disable a policy.
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(path); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// interpolate expands ${VAR} from the environment. An unset variable is an
// error: a config that silently resolves a secret to the empty string is worse
// than one that refuses to start.
func interpolate(s, path string) (string, error) {
	var missing []string

	out := envRef.ReplaceAllStringFunc(s, func(m string) string {
		name := envRef.FindStringSubmatch(m)[1]
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
			return m
		}
		return v
	})

	if len(missing) > 0 {
		return "", fmt.Errorf("config %s: undefined environment variables: %s",
			path, strings.Join(missing, ", "))
	}
	return out, nil
}

func (c *Config) applyDefaults() {
	if c.Assembly.Window == 0 {
		c.Assembly.Window = 5 * time.Minute
	}
	if c.Assembly.MaxInFlight == 0 {
		c.Assembly.MaxInFlight = 50000
	}
	if c.Assembly.SettleAfterTerminal == 0 {
		c.Assembly.SettleAfterTerminal = 5 * time.Second
	}
	if len(c.Assembly.SessionKey) == 0 {
		c.Assembly.SessionKey = []string{"session.id", "gen_ai.conversation.id", "trace_id"}
	}
	// Closed is the default (F-5.5).
	if c.Redaction.Default == "" {
		c.Redaction.Default = "deny"
	}
	if c.Redaction.OnError == "" {
		c.Redaction.OnError = "quarantine"
	}
	if c.Telemetry.LogLevel == "" {
		c.Telemetry.LogLevel = "info"
	}
	for i := range c.Sinks {
		if c.Sinks[i].BlobThresholdBytes == 0 {
			c.Sinks[i].BlobThresholdBytes = 8192
		}
		if c.Sinks[i].MaxPayloadBytes == 0 {
			c.Sinks[i].MaxPayloadBytes = 8 << 20
		}
		if len(c.Sinks[i].PartitionBy) == 0 {
			c.Sinks[i].PartitionBy = []string{"dt", "tenant", "task_type"}
		}
	}
	for i := range c.Sources {
		if c.Sources[i].MaxRequestBytes == 0 {
			c.Sources[i].MaxRequestBytes = 16 << 20
		}
	}
}
