// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests build the real binary and run it, because exit codes and output
// are the interface an operator scripts against — a deploy gate that calls
// `cc validate` depends on its exit status, not on a Go function returning an
// error.

var ccBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cc-cli-test")
	if err != nil {
		panic(err)
	}
	ccBin = filepath.Join(dir, "cc")
	if out, err := exec.Command("go", "build", "-o", ccBin, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build cc: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	code           int
	stdout, stderr string
}

func cc(t *testing.T, env []string, args ...string) result {
	t.Helper()
	cmd := exec.Command(ccBin, args...)
	cmd.Env = append(os.Environ(), env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return result{code, out.String(), errb.String()}
}

var keyEnv = []string{"CC_HMAC_KEY=cli-test-key"}

func writeConfig(t *testing.T, dir, extra string) string {
	t.Helper()
	cfg := fmt.Sprintf(`schema_version: "0.1.0"
tenant: cli
sources:
  - name: sdk
    type: native
    http: {listen: "127.0.0.1:44350"}
assembly: {window: 5m, settle_after_terminal: 100ms}
redaction:
  default: deny
  allow: ["steps[*].content_inline"]
  rules:
    - id: email
      match: {regex: "[\\w.+-]+@[\\w-]+\\.[\\w.]+"}
      action: tokenize
  tokenization: {key_env: CC_HMAC_KEY}
entities:
  - tool: zendesk.update_ticket
    keys: {ticket_id: "$.args.id"}
buffer: {dir: %s}
sinks:
  - name: lake
    type: fs
    dir: %s
    blob_threshold_bytes: 64
telemetry: {metrics: {listen: "127.0.0.1:44351"}}
%s`, filepath.Join(dir, "buffer"), filepath.Join(dir, "lake"), extra)
	p := filepath.Join(dir, "cc.yaml")
	if err := os.WriteFile(p, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const sampleEpisode = `{"episode_id":"cli-1","started_at":1758362400000000,"steps":[
  {"kind":"llm","content":"{\"input\":\"mail ivan@example.com about the refund, it is urgent and long enough to become a blob\"}"},
  {"kind":"tool","tool_name":"zendesk.update_ticket","content":"{\"args\":{\"id\":\"TKT-5\"}}"}]}`

// F-14.2 and F-11.6: validate exits 0 on a good config and non-zero on a bad
// one, naming the key.
func TestValidateExitCodes(t *testing.T) {
	dir := t.TempDir()
	good := writeConfig(t, dir, "")
	if r := cc(t, keyEnv, "validate", "-config", good); r.code != 0 {
		t.Fatalf("valid config: exit %d\n%s", r.code, r.stderr)
	}

	bad := filepath.Join(dir, "bad.yaml")
	b, _ := os.ReadFile(good)
	os.WriteFile(bad, bytes.Replace(b, []byte("tenant: cli"), []byte("tenant: cli\ntenat: typo"), 1), 0o600)
	r := cc(t, keyEnv, "validate", "-config", bad)
	if r.code == 0 {
		t.Fatal("an unknown key exited 0; a deploy gate would let it through")
	}
	if !strings.Contains(r.stderr, "tenat") {
		t.Errorf("error does not name the key: %s", r.stderr)
	}
}

// F-11.3: a dry run reports decisions without writing, and never prints a
// payload value.
func TestValidateSampleDryRun(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir, "")
	sample := filepath.Join(dir, "ep.json")
	os.WriteFile(sample, []byte(sampleEpisode), 0o600)

	r := cc(t, keyEnv, "validate", "-config", cfg, "-sample", sample)
	if r.code != 0 {
		t.Fatalf("exit %d: %s", r.code, r.stderr)
	}
	for _, want := range []string{"dry run", "email", "ticket_id=TKT-5", "externalised to a blob"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("dry run output missing %q:\n%s", want, r.stdout)
		}
	}
	if strings.Contains(r.stdout, "ivan@example.com") {
		t.Error("the dry-run report printed a payload value")
	}
	if _, err := os.Stat(filepath.Join(dir, "lake", "episodes")); err == nil {
		t.Error("a dry run wrote data")
	}
}

// F-5.9 / F-14.5: redact --test shows rule ids and counts, never values.
func TestRedactTest(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir, "")
	sample := filepath.Join(dir, "ep.json")
	os.WriteFile(sample, []byte(sampleEpisode), 0o600)

	r := cc(t, keyEnv, "redact", "-config", cfg, "--test", sample)
	if r.code != 0 {
		t.Fatalf("exit %d: %s", r.code, r.stderr)
	}
	if !strings.Contains(r.stdout, "email") || !strings.Contains(r.stdout, "tokenize x1") {
		t.Errorf("output does not report the email rule:\n%s", r.stdout)
	}
	if strings.Contains(r.stdout, "ivan@example.com") {
		t.Error("redact --test printed the value it would redact")
	}
}

// F-14.1, F-11.5: run serves, becomes ready, and exits 0 on SIGTERM after
// draining — then import, inspect, replay and conform work on what it wrote.
func TestRunThenInspectReplayConform(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir, "")

	cmd := exec.Command(ccBin, "run", "-config", cfg)
	cmd.Env = append(os.Environ(), keyEnv...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	ready := false
	for i := 0; i < 100; i++ {
		if resp, err := http.Get("http://127.0.0.1:44351/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		cmd.Process.Kill()
		t.Fatalf("never became ready\n%s", stderr.String())
	}

	resp, err := http.Post("http://127.0.0.1:44350/v1/episodes", "application/json", strings.NewReader(sampleEpisode))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run did not exit cleanly on SIGTERM: %v\n%s", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		cmd.Process.Kill()
		t.Fatal("run did not exit within the shutdown deadline")
	}
	if !strings.Contains(stderr.String(), "shutdown complete") {
		t.Errorf("no graceful shutdown in the log:\n%s", stderr.String())
	}

	lake := filepath.Join(dir, "lake")

	// F-14.4: inspect a Parquet file, a blob (with hash verification) and a
	// manifest, with no query engine.
	epFiles, _ := filepath.Glob(filepath.Join(lake, "episodes", "*", "*", "*", "*.parquet"))
	if len(epFiles) == 0 {
		t.Fatal("run wrote no episodes")
	}
	if r := cc(t, nil, "inspect", epFiles[0]); r.code != 0 || !strings.Contains(r.stdout, "rows         1") {
		t.Errorf("inspect parquet: exit %d\n%s%s", r.code, r.stdout, r.stderr)
	}
	var blob string
	filepath.Walk(filepath.Join(lake, "blobs", "sha256"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			blob = p
		}
		return nil
	})
	if blob == "" {
		t.Fatal("no blob written despite a payload over the threshold")
	}
	if r := cc(t, nil, "inspect", blob); !strings.Contains(r.stdout, "sha256     verified") {
		t.Errorf("inspect blob did not verify the hash:\n%s", r.stdout)
	}

	// F-14.6: replay resolves the blob and prints the trajectory.
	r := cc(t, nil, "replay", "-lake", lake, "cli-1")
	if r.code != 0 {
		t.Fatalf("replay: exit %d %s", r.code, r.stderr)
	}
	for _, want := range []string{"episode  cli-1", "zendesk.update_ticket", "tok_"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("replay output missing %q", want)
		}
	}
	if strings.Contains(r.stdout, "ivan@example.com") {
		t.Error("replay shows an unredacted email; redaction did not reach the lake")
	}

	// Conformance: 0 on a good lake, non-zero on a broken one.
	if r := cc(t, nil, "conform", lake); r.code != 0 {
		t.Errorf("conform on a good lake: exit %d\n%s", r.code, r.stdout)
	}
	os.Remove(blob)
	if r := cc(t, nil, "conform", lake); r.code == 0 {
		t.Error("conform exited 0 on a lake with a dangling content_ref")
	}
}

// F-14.3: import backfills an export through the same pipeline.
func TestImport(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir, "")
	r := cc(t, keyEnv, "import", "-config", cfg, "-from",
		filepath.Join("..", "..", "spec", "testdata", "langfuse-export.json"), "langfuse")
	if r.code != 0 {
		t.Fatalf("import: exit %d\n%s", r.code, r.stderr)
	}
	if !strings.Contains(r.stdout, "episodes built    2") {
		t.Errorf("import did not report two episodes:\n%s", r.stdout)
	}
	if r := cc(t, nil, "import", "-config", cfg, "-from", "x", "no-such-format"); r.code == 0 {
		t.Error("an unknown format exited 0")
	}
}
