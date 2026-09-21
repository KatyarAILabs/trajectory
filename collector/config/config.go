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

	"github.com/trajectory-project/trajectory/collector/tlsconf"
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
	Sampling  Sampling  `yaml:"sampling"`
	Buffer    Buffer    `yaml:"buffer"`
	// ShutdownTimeout bounds graceful shutdown: stop accepting, drain
	// assembly, deliver what the sink will take, flush (F-11.5). Anything
	// undelivered at the deadline stays in the buffer for the next start.
	// Keep it below the orchestrator's grace period, or the process is
	// killed mid-flush.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	Sinks           []Sink        `yaml:"sinks"`
	Telemetry       Telemetry     `yaml:"telemetry"`
}

type Source struct {
	Name string `yaml:"name"`
	// Type is "otlp", "native", "webhook" or "file".
	Type string `yaml:"type"`

	// Webhook: the callback path and the mapping file that interprets it
	// (F-1.3). The path defaults to /v1/hooks/<mapping name>.
	Path    string `yaml:"path"`
	Mapping string `yaml:"mapping"`

	// File: glob patterns to tail, and where to start in files that
	// already exist on first run (F-1.4).
	Include      []string      `yaml:"include"`
	StartAt      string        `yaml:"start_at"`
	PollInterval time.Duration `yaml:"poll_interval"`

	HTTP struct {
		Listen string         `yaml:"listen"`
		TLS    tlsconf.Config `yaml:"tls"`
	} `yaml:"http"`
	// GRPC is the OTLP/gRPC listener. Both transports are independent, so
	// a deployment may expose either or both.
	GRPC struct {
		Listen string         `yaml:"listen"`
		TLS    tlsconf.Config `yaml:"tls"`
	} `yaml:"grpc"`
	// Auth is the per-source credential (F-12.1). The token itself comes
	// from the environment, never inline (F-11.1).
	Auth struct {
		Type     string `yaml:"type"`
		TokenEnv string `yaml:"token_env"`
	} `yaml:"auth"`
	// MaxRequestBytes rejects oversized requests with a clear error and a
	// metric rather than buffering them (F-1.7).
	MaxRequestBytes int64 `yaml:"max_request_bytes"`
	// HeadSampleRate overrides sampling.head.rate for this source (F-7.1).
	// Nil means the global rate applies.
	HeadSampleRate *float64 `yaml:"head_sample_rate"`
	// RateLimit bounds how fast this source may send (F-7.3). Exceeding it
	// is answered with 429 and counted as shed, rather than letting one
	// noisy producer fill the buffer for everyone.
	RateLimit struct {
		// RecordsPerSecond is the sustained rate. Zero means unlimited.
		RecordsPerSecond float64 `yaml:"records_per_second"`
		// Burst is how far above the rate a producer may briefly go.
		Burst int `yaml:"burst"`
	} `yaml:"rate_limit"`
	// Redaction, when set, replaces the global policy for this source
	// (F-5.7). It replaces wholesale rather than merging: a merged policy
	// is one nobody can read in a single place, and redaction is the one
	// setting a reviewer must be able to read in a single place.
	Redaction *Redaction `yaml:"redaction"`
	// MappingsDir overrides builtin convention mappings by name, so an
	// operator can track a producer that has moved ahead of the shipped
	// tables without waiting for a release (F-2.4).
	MappingsDir string `yaml:"mappings_dir"`
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
	// PatchMemory is how many recently emitted sessions to remember so a
	// late span becomes a patch on the right episode rather than a new
	// episode built from a fragment (F-3.5).
	PatchMemory int `yaml:"patch_memory"`
}

type Redaction struct {
	// Default is `deny` or `allow`. Deny-by-default means only
	// allow-listed paths survive.
	Default string `yaml:"default"`
	// OnError is `quarantine` or `pass`. Closed is the default (F-5.5).
	OnError string `yaml:"on_error"`
	// Allow lists the field paths that survive under deny-by-default.
	Allow []string `yaml:"allow"`
	// Deny removes field paths even when they would otherwise be allowed.
	// An explicit deny always wins, so a broad allow prefix can be carved
	// out without rewriting it.
	Deny  []string `yaml:"deny"`
	Rules []Rule   `yaml:"rules"`
	// Detector calls an external entity-detection service for what no
	// regex catches, such as names and addresses (F-5.8). It receives
	// payload text, so it must run inside the same perimeter.
	Detector struct {
		Endpoint string        `yaml:"endpoint"`
		Language string        `yaml:"language"`
		Entities []string      `yaml:"entities"`
		MinScore float64       `yaml:"min_score"`
		Action   string        `yaml:"action"`
		Timeout  time.Duration `yaml:"timeout"`
	} `yaml:"detector"`
	// MetadataOnly discards every payload, keeping only structure and
	// metadata (F-5.6). It overrides allow and rules entirely: there is no
	// combination of other settings that lets a payload through when this
	// is set.
	MetadataOnly bool `yaml:"metadata_only"`

	Tokenization struct {
		// KeyEnv names the environment variable holding the HMAC key.
		// Secrets come from env, file or secret manager, never inline
		// (F-11.1).
		KeyEnv string `yaml:"key_env"`
	} `yaml:"tokenization"`
}

type Rule struct {
	ID    string    `yaml:"id"`
	Match RuleMatch `yaml:"match"`
	// Action is `tokenize` or `drop`.
	Action string `yaml:"action"`
	// Fields narrows a rule to particular field paths. Empty means every
	// field the policy lets through.
	Fields []string `yaml:"fields"`
}

// RuleMatch selects what a rule applies to. Regex and Path may be combined:
// the path narrows the rule to part of a JSON payload, and the regex then
// selects within it. A rule with neither matches nothing and is rejected at
// validation, because a rule that silently never fires is worse than absent.
type RuleMatch struct {
	// Regex matches anywhere in a field value.
	Regex string `yaml:"regex"`
	// Path is a JSONPath into a payload that parses as JSON. Matched nodes
	// are redacted whole, which is how a known-sensitive field is handled
	// without writing a regex that might also hit something else.
	Path string `yaml:"path"`
}

type Entity struct {
	Tool string            `yaml:"tool"`
	Keys map[string]string `yaml:"keys"`
}

// Sampling controls volume (F-7). The unit of sampling is always the assembled
// episode, never the span: a half-sampled trajectory looks complete to a reader
// and is worse than no trajectory at all.
type Sampling struct {
	Head struct {
		// Rate in [0,1]. Nil means keep everything. The decision is a
		// deterministic hash of the session key, so every span of a
		// session shares one answer.
		Rate *float64 `yaml:"rate"`
	} `yaml:"head"`
	Tail struct {
		// KeepIf are CEL expressions evaluated after assembly. Any one
		// matching keeps the episode.
		KeepIf []string `yaml:"keep_if"`
		// OtherwiseRate applies to episodes no keep_if rule matched.
		OtherwiseRate *float64 `yaml:"otherwise_rate"`
	} `yaml:"tail"`
}

// Buffer is the durable queue between the processors and a sink (F-8). It is
// what makes acknowledged data survive a sink outage or a process kill.
type Buffer struct {
	Dir string `yaml:"dir"`
	// MaxBytes and MaxAge bound the buffer. The eviction policy is explicit
	// and observable (F-8.5): the oldest sealed segment is dropped and
	// counted.
	MaxBytes ByteSize      `yaml:"max_bytes"`
	MaxAge   time.Duration `yaml:"max_age"`
	// SegmentBytes is the size at which a segment is sealed.
	SegmentBytes ByteSize `yaml:"segment_bytes"`
	// BackpressureAt is the fraction of max_bytes at which sources are
	// asked to slow down (F-8.2).
	BackpressureAt float64 `yaml:"backpressure_at"`
	// MaxAttempts before a record is dead-lettered (F-8.3).
	MaxAttempts int `yaml:"max_attempts"`
	// BatchSize is how many records are handed to the sink before it is
	// asked to commit. One commit per record made delivery far slower than
	// ingest, so the backlog grew under normal load.
	BatchSize int `yaml:"batch_size"`
	// RetryBaseDelay doubles per attempt, with full jitter.
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	RetryMaxDelay  time.Duration `yaml:"retry_max_delay"`
	// DeadLetterDir receives records that exhausted their attempts. Empty
	// means the quarantine prefix under the sink.
	DeadLetterDir string `yaml:"dead_letter_dir"`
	// Encryption encrypts buffered records at rest (F-8.6). The key comes
	// from the environment, never from this file.
	Encryption struct {
		KeyEnv string `yaml:"key_env"`
	} `yaml:"encryption"`
}

type Sink struct {
	Name string `yaml:"name"`
	// Type is "fs" or "s3".
	Type string `yaml:"type"`
	Dir  string `yaml:"dir"`

	// S3-compatible settings (AWS, GCS, Azure via S3 interop, R2, MinIO).
	Bucket   string `yaml:"bucket"`
	Prefix   string `yaml:"prefix"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"`
	// PathStyle is required by MinIO and most self-hosted gateways.
	PathStyle bool `yaml:"path_style"`
	// Credentials come from the environment via ${VAR} interpolation,
	// never inline (F-11.1). Empty means the default credential chain.
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	// SSE configures bucket-side encryption with customer-managed keys
	// (F-12.3).
	SSE struct {
		Type  string `yaml:"type"`
		KeyID string `yaml:"key_id"`
	} `yaml:"sse"`

	// TargetFileBytes and RollInterval drive files toward a
	// compaction-friendly size (F-9.5).
	TargetFileBytes ByteSize      `yaml:"target_file_bytes"`
	RollInterval    time.Duration `yaml:"roll_interval"`
	Compression     string        `yaml:"compression"`
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
	// Traces sends the collector's own spans to an OTLP endpoint (F-11.2).
	// Empty endpoint means off, and no connection is attempted (F-12.6).
	Traces struct {
		Endpoint   string  `yaml:"endpoint"`
		Protocol   string  `yaml:"protocol"`
		Insecure   bool    `yaml:"insecure"`
		SampleRate float64 `yaml:"sample_rate"`
	} `yaml:"traces"`
	// Pprof exposes Go profiling on the telemetry listener. Off by
	// default: /debug/pprof reveals command-line arguments and memory
	// contents, so it must never be on by accident.
	Pprof    bool   `yaml:"pprof"`
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
	return Parse(raw, path)
}

// Parse decodes and validates config bytes. name labels errors. It is the one
// path every consumer of the config goes through — the CLI, and embedding hosts
// such as the OTel Collector exporter — so a config means the same thing
// everywhere it is accepted.
func Parse(raw []byte, path string) (*Config, error) {
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
	if c.Assembly.PatchMemory == 0 {
		c.Assembly.PatchMemory = 10000
	}
	if len(c.Assembly.SessionKey) == 0 {
		c.Assembly.SessionKey = []string{"session.id", "gen_ai.conversation.id", "trace_id"}
	}
	applyRedactionDefaults(&c.Redaction)
	for i := range c.Sources {
		if c.Sources[i].Redaction != nil {
			applyRedactionDefaults(c.Sources[i].Redaction)
		}
	}
	if c.Telemetry.LogLevel == "" {
		c.Telemetry.LogLevel = "info"
	}
	for i := range c.Sinks {
		s := &c.Sinks[i]
		if s.BlobThresholdBytes == 0 {
			s.BlobThresholdBytes = 8192
		}
		if s.MaxPayloadBytes == 0 {
			s.MaxPayloadBytes = 8 << 20
		}
		if len(s.PartitionBy) == 0 {
			s.PartitionBy = []string{"dt", "tenant", "task_type"}
		}
		if s.TargetFileBytes == 0 {
			// 256 MiB sits in the middle of the 128-512 MB band
			// F-9.5 asks for.
			s.TargetFileBytes = 256 << 20
		}
		if s.RollInterval == 0 {
			s.RollInterval = 15 * time.Minute
		}
		if s.Compression == "" {
			s.Compression = "zstd"
		}
	}

	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}

	b := &c.Buffer
	if b.MaxBytes == 0 {
		b.MaxBytes = 8 << 30
	}
	if b.MaxAge == 0 {
		b.MaxAge = 24 * time.Hour
	}
	if b.SegmentBytes == 0 {
		b.SegmentBytes = 64 << 20
	}
	if b.BackpressureAt == 0 {
		b.BackpressureAt = 0.8
	}
	if b.MaxAttempts == 0 {
		b.MaxAttempts = 8
	}
	if b.BatchSize == 0 {
		b.BatchSize = 256
	}
	if b.RetryBaseDelay == 0 {
		b.RetryBaseDelay = 500 * time.Millisecond
	}
	if b.RetryMaxDelay == 0 {
		b.RetryMaxDelay = 60 * time.Second
	}
	for i := range c.Sources {
		if c.Sources[i].MaxRequestBytes == 0 {
			c.Sources[i].MaxRequestBytes = 16 << 20
		}
	}
}

// applyRedactionDefaults fills unset policy fields. Closed is the default on
// both axes (F-5.5): deny what is not allowed, quarantine what cannot be
// evaluated.
func applyRedactionDefaults(r *Redaction) {
	if r.Default == "" {
		r.Default = "deny"
	}
	if r.OnError == "" {
		r.OnError = "quarantine"
	}
}
