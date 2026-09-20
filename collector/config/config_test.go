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
		"type: fs", "type: s3",
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
