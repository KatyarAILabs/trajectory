// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package trajectoryexporter

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/KatyarAILabs/trajectory/collector/config"
	"github.com/KatyarAILabs/trajectory/internal/version"
)

// Config holds the exporter's settings in exactly the format `cc` takes.
//
// It is deliberately not a parallel set of mapstructure-tagged structs. A
// second schema for the same settings would drift, and the settings in
// question include the redaction policy — the one thing that must mean exactly
// the same in both deployments. The OTel Collector hands us a map; it is
// rendered to YAML and put through the same strict parser and validator as the
// standalone binary, including rejecting unknown keys.
type Config struct {
	Settings map[string]any `mapstructure:",remain"`
}

// Validate implements the OTel Collector's config validation hook.
func (c *Config) Validate() error {
	_, err := c.parse()
	return err
}

// parse renders the settings and runs the shared parser.
//
// `sources` is filled in because the host collector owns the receivers; a
// `sources` key supplied here is an error rather than something silently
// ignored.
func (c *Config) parse() (*config.Config, error) {
	settings := map[string]any{}
	for k, v := range c.Settings {
		settings[k] = v
	}

	if _, ok := settings["sources"]; ok {
		return nil, fmt.Errorf("trajectory exporter: `sources` is not allowed here; " +
			"the OpenTelemetry Collector's receivers feed this exporter")
	}
	if _, ok := settings["schema_version"]; !ok {
		settings["schema_version"] = version.Schema
	}
	settings["sources"] = []any{map[string]any{
		"name": "otelcol", "type": "otlp",
		"http": map[string]any{"listen": "127.0.0.1:0"},
	}}

	raw, err := yaml.Marshal(settings)
	if err != nil {
		return nil, fmt.Errorf("trajectory exporter: %w", err)
	}
	return config.Parse(raw, "exporters::trajectory")
}
