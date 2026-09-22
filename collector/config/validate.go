// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/KatyarAILabs/trajectory/internal/version"
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
	} else if !compatibleSchema(c.SchemaVersion, version.Schema) {
		// A config names the record shape its operator expects. Within a
		// major version this build writes a superset of every earlier
		// minor (F-10.1), so an older minor is fine. A different major,
		// or a newer minor than this build knows, means the operator
		// expects a shape this binary cannot write.
		bad("schema_version", "is %q but this build writes %q; a config may name "+
			"this version or an earlier minor of the same major",
			c.SchemaVersion, version.Schema)
	}
	if c.Tenant == "" {
		bad("tenant", "required; v1 is single-tenant per deployment")
	}

	if c.ShutdownTimeout < 0 {
		bad("shutdown_timeout", "must not be negative")
	}

	c.validateSources(bad)
	c.validateAssembly(bad)
	c.validateRedaction(bad)
	c.validateEntities(bad)
	c.validateSampling(bad)
	c.validateBuffer(bad)
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
	listeners := map[string]string{}
	for i, s := range c.Sources {
		key := fmt.Sprintf("sources[%d]", i)
		if s.Name == "" {
			bad(key+".name", "required; the name is attributed onto every record this source produces")
		}
		if seen[s.Name] {
			bad(key+".name", "duplicate source name %q; names must be unique to stay attributable", s.Name)
		}
		seen[s.Name] = true

		switch s.Type {
		case "otlp":
			if s.HTTP.Listen == "" && s.GRPC.Listen == "" {
				bad(key, "an otlp source needs http.listen, grpc.listen, or both")
			}
		case "native":
			if s.HTTP.Listen == "" {
				bad(key+".http.listen", "required for a native source")
			}
			if s.GRPC.Listen != "" {
				bad(key+".grpc.listen",
					"the native API is HTTP only; remove this or use type: otlp")
			}
		case "webhook":
			if s.HTTP.Listen == "" {
				bad(key+".http.listen", "required for a webhook source")
			}
			if s.Mapping == "" {
				bad(key+".mapping", "required: a webhook source is interpreted by its mapping file")
			} else if _, err := os.Stat(s.Mapping); err != nil {
				bad(key+".mapping", "cannot read %s: %v", s.Mapping, err)
			}
		case "file":
			if len(s.Include) == 0 {
				bad(key+".include", "required: at least one glob pattern to tail")
			}
			for j, pat := range s.Include {
				if _, err := filepath.Match(pat, ""); err != nil {
					bad(fmt.Sprintf("%s.include[%d]", key, j), "invalid glob %q: %v", pat, err)
				}
			}
			switch s.StartAt {
			case "", "end", "beginning":
			default:
				bad(key+".start_at", "must be \"end\" or \"beginning\", got %q", s.StartAt)
			}
			if s.HTTP.Listen != "" || s.GRPC.Listen != "" {
				bad(key, "a file source does not listen; remove http/grpc")
			}
		default:
			bad(key+".type", "%q is not supported; use otlp, native, webhook or file", s.Type)
		}

		if r := s.HeadSampleRate; r != nil && (*r < 0 || *r > 1) {
			bad(key+".head_sample_rate", "must be between 0 and 1, got %v", *r)
		}
		if s.RateLimit.RecordsPerSecond < 0 {
			bad(key+".rate_limit.records_per_second", "must not be negative")
		}
		if s.RateLimit.RecordsPerSecond > 0 && s.RateLimit.Burst <= 0 {
			bad(key+".rate_limit.burst",
				"must be positive when a rate is set; a burst of 0 admits nothing")
		}

		s.HTTP.TLS.Validate(key+".http.tls", bad)
		s.GRPC.TLS.Validate(key+".grpc.tls", bad)

		// A bearer token over plaintext is a credential on the wire.
		// Saying so at config time is cheaper than a security review
		// finding it later.
		if s.Auth.Type == "bearer" && s.HTTP.Listen != "" && !s.HTTP.TLS.Enabled() &&
			!isLoopback(s.HTTP.Listen) {
			bad(key+".auth",
				"a bearer token is configured on a non-loopback plaintext listener (%s); "+
					"set http.tls or bind to localhost", s.HTTP.Listen)
		}

		switch s.Auth.Type {
		case "", "none":
		case "bearer":
			if s.Auth.TokenEnv == "" {
				bad(key+".auth.token_env", "required when auth.type is \"bearer\"")
			} else if os.Getenv(s.Auth.TokenEnv) == "" {
				bad(key+".auth.token_env",
					"environment variable %q is unset or empty; the source would "+
						"reject every request", s.Auth.TokenEnv)
			}
		default:
			bad(key+".auth.type", "must be \"bearer\" or \"none\", got %q", s.Auth.Type)
		}

		if s.MappingsDir != "" {
			if fi, err := os.Stat(s.MappingsDir); err != nil {
				bad(key+".mappings_dir", "cannot read %s: %v", s.MappingsDir, err)
			} else if !fi.IsDir() {
				bad(key+".mappings_dir", "%s is not a directory", s.MappingsDir)
			}
		}

		// Two sources cannot share a listener; the second would fail at
		// bind time with a message that does not mention config.
		for _, addr := range []string{s.HTTP.Listen, s.GRPC.Listen} {
			if addr == "" {
				continue
			}
			if owner, taken := listeners[addr]; taken {
				bad(key, "listen address %s is already used by source %q", addr, owner)
			}
			listeners[addr] = s.Name
		}
	}
}

// isLoopback reports whether a listen address is reachable only from the host.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	validateRedactionPolicy("redaction", c.Redaction, bad)

	// Per-source overrides get exactly the same checks (F-5.7). An override
	// is a whole policy, so a weaker one is still a policy a reviewer has to
	// be able to trust.
	for i, s := range c.Sources {
		if s.Redaction != nil {
			pol := *s.Redaction
			applyRedactionDefaults(&pol)
			validateRedactionPolicy(fmt.Sprintf("sources[%d].redaction", i), pol, bad)
		}
	}
}

func validateRedactionPolicy(prefix string, r Redaction, bad func(string, string, ...any)) {
	switch r.Default {
	case "deny", "allow":
	default:
		bad(prefix+".default", "must be \"deny\" or \"allow\", got %q", r.Default)
	}

	switch r.OnError {
	case "quarantine", "pass":
	default:
		bad(prefix+".on_error", "must be \"quarantine\" or \"pass\", got %q", r.OnError)
	}
	// "pass" is legal but it is the one setting that turns a policy failure
	// into a leak, so it must never be reached by a copied config or a
	// careless default. Requiring a second, out-of-band signal means an
	// operator cannot enable it without knowing they did.
	if r.OnError == "pass" && os.Getenv("CC_ALLOW_REDACTION_PASS") != "1" {
		bad(prefix+".on_error", "\"pass\" disables fail-closed (F-5.5); "+
			"if that is deliberate, set CC_ALLOW_REDACTION_PASS=1 as well")
	}

	needsKey := false
	for i, r := range r.Rules {
		key := fmt.Sprintf("%s.rules[%d]", prefix, i)
		if r.ID == "" {
			bad(key+".id", "required; the id appears in the redaction manifest and in metrics")
		}
		if r.Match.Regex == "" && r.Match.Path == "" {
			bad(key+".match",
				"needs regex, path, or both; a rule that matches nothing "+
					"never fires and is worse than not being there")
		}
		if r.Match.Regex != "" {
			if _, err := regexp.Compile(r.Match.Regex); err != nil {
				bad(key+".match.regex", "does not compile: %v", err)
			}
		}
		if r.Match.Path != "" && !strings.HasPrefix(r.Match.Path, "$") {
			bad(key+".match.path", "must be a JSONPath starting with $, got %q", r.Match.Path)
		}
		switch r.Action {
		case "tokenize":
			needsKey = true
		case "drop":
		default:
			bad(key+".action", "must be \"tokenize\" or \"drop\", got %q", r.Action)
		}
	}

	// Metadata-only discards payloads outright, so payload-shaped settings
	// alongside it are a sign the operator expects them to do something.
	if r.MetadataOnly && (len(r.Allow) > 0 || len(r.Rules) > 0) {
		bad(prefix+".metadata_only",
			"is true, which discards every payload, but allow paths or rules are "+
				"also configured; remove one so the intent is unambiguous")
	}

	if needsKey && !r.MetadataOnly {
		env := r.Tokenization.KeyEnv
		if env == "" {
			bad(prefix+".tokenization.key_env", "required when any rule uses action \"tokenize\"")
		} else if os.Getenv(env) == "" {
			bad(prefix+".tokenization.key_env",
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

func (c *Config) validateSampling(bad func(string, string, ...any)) {
	if r := c.Sampling.Head.Rate; r != nil && (*r < 0 || *r > 1) {
		bad("sampling.head.rate", "must be between 0 and 1, got %v", *r)
	}
	if r := c.Sampling.Tail.OtherwiseRate; r != nil && (*r < 0 || *r > 1) {
		bad("sampling.tail.otherwise_rate", "must be between 0 and 1, got %v", *r)
	}

	// Head sampling composes multiplicatively with tail sampling, and an
	// operator who sets head 0.1 and tail 0.1 expecting 10%% will actually
	// get 1%%. Refusing silence here is cheaper than discovering the corpus
	// is a tenth the expected size months later.
	if h := c.Sampling.Head.Rate; h != nil && *h < 1 {
		if t := c.Sampling.Tail.OtherwiseRate; t != nil && *t < 1 {
			bad("sampling",
				"head.rate (%v) and tail.otherwise_rate (%v) compose multiplicatively, "+
					"keeping roughly %.1f%% of unmatched episodes; set one to 1.0 if that is not intended",
				*h, *t, *h**t*100)
		}
	}
}

func (c *Config) validateBuffer(bad func(string, string, ...any)) {
	b := c.Buffer
	if b.Dir == "" {
		// A collector with no buffer loses everything in flight when a
		// sink is unreachable or the process is killed, which is
		// exactly what G-4 forbids. Refusing is better than a silent
		// downgrade in durability.
		bad("buffer.dir", "required; without a disk buffer no acknowledged data survives "+
			"a sink outage or a restart (F-8.1)")
	}
	if b.BackpressureAt <= 0 || b.BackpressureAt > 1 {
		bad("buffer.backpressure_at", "must be between 0 and 1, got %v", b.BackpressureAt)
	}
	if b.MaxAttempts <= 0 {
		bad("buffer.max_attempts", "must be positive; a record must eventually be "+
			"dead-lettered rather than blocking everything behind it (F-8.3)")
	}
	if b.SegmentBytes > 0 && b.MaxBytes > 0 && b.SegmentBytes > b.MaxBytes {
		bad("buffer.segment_bytes", "(%s) exceeds buffer.max_bytes (%s), so no segment "+
			"could ever be sealed and reclaimed", b.SegmentBytes, b.MaxBytes)
	}
	if env := b.Encryption.KeyEnv; env != "" {
		v := os.Getenv(env)
		switch {
		case v == "":
			bad("buffer.encryption.key_env", "environment variable %q is unset or empty", env)
		case len(v) != 32 && len(strings.TrimSpace(v)) != 64:
			bad("buffer.encryption.key_env",
				"%q must hold 32 bytes or 64 hex characters (AES-256), got %d bytes", env, len(v))
		}
	}
	if b.RetryBaseDelay > b.RetryMaxDelay && b.RetryMaxDelay > 0 {
		bad("buffer.retry_base_delay", "(%s) exceeds retry_max_delay (%s)",
			b.RetryBaseDelay, b.RetryMaxDelay)
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

		switch s.Type {
		case "fs":
			if s.Dir == "" {
				bad(key+".dir", "required for a filesystem sink")
			}
		case "s3":
			if s.Bucket == "" {
				bad(key+".bucket", "required for an s3 sink")
			}
			if s.Region == "" && s.Endpoint == "" {
				bad(key+".region", "required unless an endpoint is set")
			}
			switch s.SSE.Type {
			case "", "AES256":
			case "aws:kms":
				if s.SSE.KeyID == "" {
					bad(key+".sse.key_id",
						"required when sse.type is aws:kms; without it the bucket "+
							"default key is used, which defeats customer-managed keys (F-12.3)")
				}
			default:
				bad(key+".sse.type", "must be \"AES256\" or \"aws:kms\", got %q", s.SSE.Type)
			}
			if s.AccessKeyID != "" && s.SecretAccessKey == "" {
				bad(key+".secret_access_key", "required when access_key_id is set")
			}
		default:
			bad(key+".type", "%q is not supported; use \"fs\" or \"s3\"", s.Type)
		}

		switch s.Compression {
		case "", "zstd", "snappy", "none", "uncompressed":
		default:
			bad(key+".compression", "must be zstd, snappy or none, got %q", s.Compression)
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

// compatibleSchema reports whether a build writing `have` satisfies a config
// that expects `want`: same major, and want's minor no newer than have's.
func compatibleSchema(want, have string) bool {
	wm, wn, ok1 := majorMinor(want)
	hm, hn, ok2 := majorMinor(have)
	return ok1 && ok2 && wm == hm && wn <= hn
}

func majorMinor(v string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	maj, err1 := strconv.Atoi(parts[0])
	min, err2 := strconv.Atoi(parts[1])
	return maj, min, err1 == nil && err2 == nil
}
