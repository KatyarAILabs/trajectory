// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package trajectoryexporter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/trajectory-project/trajectory/collector/acceptance"
)

func settings(dir string) map[string]any {
	return map[string]any{
		"tenant": "otel",
		"assembly": map[string]any{
			"window": "5m", "settle_after_terminal": "10ms",
		},
		"redaction": map[string]any{
			"default": "deny",
			"allow":   []any{"steps[*].content_inline"},
			"rules": []any{map[string]any{
				"id": "email", "action": "tokenize",
				"match": map[string]any{"regex": `[\w.+-]+@[\w-]+\.[\w.]+`},
			}},
			"tokenization": map[string]any{"key_env": "CC_HMAC_KEY"},
		},
		"buffer": map[string]any{"dir": filepath.Join(dir, "buffer")},
		"sinks": []any{map[string]any{
			"name": "lake", "type": "fs", "dir": filepath.Join(dir, "lake"),
		}},
	}
}

// F-13.3: the exporter works as an OTel Collector component — created by the
// factory, fed through the Traces interface the collector uses, and producing
// the same lake as the standalone binary.
func TestFactoryEndToEnd(t *testing.T) {
	t.Setenv("CC_HMAC_KEY", "otel-test-key")
	dir := t.TempDir()

	f := NewFactory()
	if f.Type().String() != "trajectory" {
		t.Fatalf("type = %s", f.Type())
	}

	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Settings = settings(dir)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	ctx := context.Background()
	exp, err := f.CreateTraces(ctx, exportertest.NewNopSettings(f.Type()), cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	if err := exp.ConsumeTraces(ctx, acceptance.OpenInferenceFixture()); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := exp.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	var parquet int
	leaked := false
	filepath.Walk(filepath.Join(dir, "lake"), func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(p, ".parquet") {
			parquet++
		}
		b, _ := os.ReadFile(p)
		if strings.Contains(string(b), "alice@example.com") {
			leaked = true
		}
		return nil
	})
	if parquet == 0 {
		t.Fatal("the exporter wrote no Parquet")
	}
	if leaked {
		t.Fatal("seeded email reached the lake through the exporter path")
	}
}

// The exporter shares the standalone parser, so it inherits strictness: an
// unknown key is an error here too.
func TestUnknownKeyRejected(t *testing.T) {
	t.Setenv("CC_HMAC_KEY", "k")
	s := settings(t.TempDir())
	s["redactoin"] = map[string]any{} // typo
	if err := (&Config{Settings: s}).Validate(); err == nil {
		t.Fatal("a misspelled key was accepted")
	}
}

// `sources` makes no sense when the host owns the receivers, and silently
// ignoring it would mislead whoever wrote it.
func TestSourcesRejected(t *testing.T) {
	t.Setenv("CC_HMAC_KEY", "k")
	s := settings(t.TempDir())
	s["sources"] = []any{}
	if err := (&Config{Settings: s}).Validate(); err == nil {
		t.Fatal("sources was accepted in exporter config")
	}
}
