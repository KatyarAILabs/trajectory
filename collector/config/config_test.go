// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cc.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const valid = `
schema_version: "0.1.0"
tenant: acme
sources:
  - name: otlp
    type: otlp
    http: {listen: "0.0.0.0:4318"}
redaction:
  default: deny
  allow: ["steps[*].content_inline"]
  rules:
    - id: email
      match: {regex: "[\\w.+-]+@[\\w-]+\\.[\\w.]+"}
      action: tokenize
  tokenization: {key_env: TEST_HMAC_KEY}
buffer:
  dir: /tmp/buf
sinks:
  - name: local
    type: fs
    dir: /tmp/lake
`

func TestValidConfigLoads(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	cfg, err := Load(write(t, valid))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Defaults applied.
	if cfg.Assembly.Window == 0 || cfg.Assembly.MaxInFlight == 0 {
		t.Error("assembly defaults not applied")
	}
	if cfg.Redaction.OnError != "quarantine" {
		t.Errorf("on_error default = %q, want quarantine (fail closed)", cfg.Redaction.OnError)
	}
	if cfg.Sinks[0].BlobThresholdBytes == 0 {
		t.Error("blob threshold default not applied")
	}
}

// §10: unknown keys are an error, not a warning. A misspelled key that
// silently does nothing is exactly the failure this prevents.
func TestUnknownKeyIsAnError(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "tenant: acme", "tenant: acme\ntenat: typo", 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("unknown key accepted; a typo would silently disable config")
	}
	if !strings.Contains(err.Error(), "tenat") {
		t.Errorf("error does not name the offending key: %v", err)
	}
}

// F-11.6: refuse to start on invalid config with a message naming the key and
// the reason.
func TestValidationNamesKeyAndReason(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "action: tokenize", "action: tokenise", 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("invalid action accepted")
	}
	msg := err.Error()
	for _, want := range []string{"redaction.rules[0].action", "tokenise"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %v", want, msg)
		}
	}
}

// Tokenization without a key would produce unjoinable records, so it must be
// caught at config time rather than at the first matching payload.
func TestMissingTokenizationKeyIsCaught(t *testing.T) {
	os.Unsetenv("TEST_HMAC_KEY")
	_, err := Load(write(t, valid))
	if err == nil {
		t.Fatal("tokenize rule accepted with no key present")
	}
	if !strings.Contains(err.Error(), "TEST_HMAC_KEY") {
		t.Errorf("error does not name the missing variable: %v", err)
	}
}

// F-5.5: disabling fail-closed must require a second, deliberate signal, so it
// cannot arrive by copying someone else's config.
func TestFailOpenRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "default: deny", "default: deny\n  on_error: pass", 1)

	if _, err := Load(write(t, body)); err == nil {
		t.Fatal("on_error: pass accepted without the explicit opt-in")
	}

	t.Setenv("CC_ALLOW_REDACTION_PASS", "1")
	if _, err := Load(write(t, body)); err != nil {
		t.Fatalf("on_error: pass rejected despite the opt-in: %v", err)
	}
}

// F-11.1: secrets come from the environment. An unset variable must fail
// loudly rather than resolving to an empty string.
func TestUndefinedEnvVarIsAnError(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "dir: /tmp/lake", "dir: ${CC_UNDEFINED_LAKE_DIR}", 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("undefined ${VAR} resolved silently")
	}
	if !strings.Contains(err.Error(), "CC_UNDEFINED_LAKE_DIR") {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestEnvInterpolation(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	t.Setenv("CC_LAKE_DIR", "/data/lake")
	body := strings.Replace(valid, "dir: /tmp/lake", "dir: ${CC_LAKE_DIR}", 1)

	cfg, err := Load(write(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sinks[0].Dir != "/data/lake" {
		t.Errorf("dir = %q, want /data/lake", cfg.Sinks[0].Dir)
	}
}

// A schema mismatch means the operator expects a different record shape than
// this binary writes.
func TestSchemaVersionMismatchRefused(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, `schema_version: "0.1.0"`, `schema_version: "9.9.9"`, 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("mismatched schema_version accepted")
	}
}

// Every problem is reported at once, so a user fixes one round rather than
// discovering them one restart at a time.
func TestAllErrorsReportedTogether(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.NewReplacer(
		"tenant: acme", "",
		"type: fs", "type: unsupported",
		"action: tokenize", "action: nonsense",
	).Replace(valid)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("invalid config accepted")
	}
	msg := err.Error()
	for _, want := range []string{"tenant", "sinks[0].type", "redaction.rules[0].action"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error omits %q; it should report every problem at once:\n%v", want, msg)
		}
	}
}

// A collector with no buffer loses everything in flight when a sink is
// unreachable, which is exactly what G-4 forbids. Refusing to start is better
// than a silent downgrade in durability.
func TestBufferDirRequired(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "buffer:\n  dir: /tmp/buf\n", "", 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("a config with no buffer was accepted")
	}
	if !strings.Contains(err.Error(), "buffer.dir") {
		t.Errorf("error does not name buffer.dir: %v", err)
	}
}

// §10's example writes `max_bytes: 8Gi`. Accepting only a raw byte count would
// make the published example invalid.
func TestByteSizeSuffixes(t *testing.T) {
	cases := map[string]int64{
		"8Gi":   8 << 30,
		"512Mi": 512 << 20,
		"64KiB": 64 << 10,
		"1024":  1024,
		"2GB":   2 * 1000 * 1000 * 1000,
	}
	for in, want := range cases {
		got, err := ParseByteSize(in)
		if err != nil {
			t.Errorf("ParseByteSize(%q): %v", in, err)
			continue
		}
		if int64(got) != want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", in, got, want)
		}
	}

	if _, err := ParseByteSize("banana"); err == nil {
		t.Error("an unparseable size was accepted")
	}
	if _, err := ParseByteSize("-5Gi"); err == nil {
		t.Error("a negative size was accepted")
	}
}

func TestByteSizeInConfig(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "buffer:\n  dir: /tmp/buf",
		"buffer:\n  dir: /tmp/buf\n  max_bytes: 8Gi\n  segment_bytes: 64Mi", 1)

	cfg, err := Load(write(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if int64(cfg.Buffer.MaxBytes) != 8<<30 {
		t.Errorf("max_bytes = %d, want %d", cfg.Buffer.MaxBytes, int64(8)<<30)
	}
	if int64(cfg.Buffer.SegmentBytes) != 64<<20 {
		t.Errorf("segment_bytes = %d", cfg.Buffer.SegmentBytes)
	}
}

// F-12.3: aws:kms without a key id silently falls back to the bucket default
// key, which defeats the point of customer-managed keys.
func TestKMSRequiresKeyID(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, `sinks:
  - name: local
    type: fs
    dir: /tmp/lake`, `sinks:
  - name: lake
    type: s3
    bucket: acme-lake
    region: us-east-1
    sse: {type: "aws:kms"}`, 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("aws:kms with no key_id was accepted")
	}
	if !strings.Contains(err.Error(), "sse.key_id") {
		t.Errorf("error does not name sse.key_id: %v", err)
	}
}

// A segment larger than the whole buffer could never be sealed and reclaimed,
// so the buffer would grow without bound despite the configured limit.
func TestSegmentLargerThanBufferRejected(t *testing.T) {
	t.Setenv("TEST_HMAC_KEY", "k")
	body := strings.Replace(valid, "buffer:\n  dir: /tmp/buf",
		"buffer:\n  dir: /tmp/buf\n  max_bytes: 10Mi\n  segment_bytes: 100Mi", 1)

	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("segment_bytes larger than max_bytes was accepted")
	}
}
