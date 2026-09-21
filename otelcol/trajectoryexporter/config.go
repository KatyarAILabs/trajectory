// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package trajectoryexporter exports OTLP traces through the trajectory
// pipeline (F-13.3).
//
// It reuses collector/* directly rather than reimplementing normalisation,
// redaction or storage. A second implementation would drift from the standalone
// binary, and the fidelity and redaction guarantees are the entire product —
// two subtly different redaction paths is exactly the bug nobody finds until a
// payload turns up in a lake.
package trajectoryexporter

import (
	"fmt"

	"github.com/trajectory-project/trajectory/collector/config"
)

// Config is the exporter's configuration.
//
// It is the collector's own config minus `sources`, because in this deployment
// the host OTel Collector owns the receivers. Everything else is shared, and
// validated by the same code, so a policy that passes `cc validate` means the
// same thing here.
type Config struct {
	Tenant string `mapstructure:"tenant"`

	Assembly  config.Assembly  `mapstructure:"assembly"`
	Redaction config.Redaction `mapstructure:"redaction"`
	Entities  []config.Entity  `mapstructure:"entities"`
	Sampling  config.Sampling  `mapstructure:"sampling"`
	Buffer    config.Buffer    `mapstructure:"buffer"`
	Sinks     []config.Sink    `mapstructure:"sinks"`

	// SessionKey is the attribute precedence for grouping spans into
	// episodes (F-3.1).
	SessionKey []string `mapstructure:"session_key"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Tenant == "" {
		return fmt.Errorf("tenant is required; it is recorded on every record and is a partition key")
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	if c.Buffer.Dir == "" {
		return fmt.Errorf(
			"buffer.dir is required; without a disk buffer no acknowledged data survives " +
				"a sink outage or a restart")
	}

	// Reuse the standalone validator so the rules cannot diverge. It works
	// on a whole Config, so the exporter's fields are lifted into one.
	full := &config.Config{
		SchemaVersion: schemaVersionForValidation(),
		Tenant:        c.Tenant,
		Assembly:      c.Assembly,
		Redaction:     c.Redaction,
		Entities:      c.Entities,
		Sampling:      c.Sampling,
		Buffer:        c.Buffer,
		Sinks:         c.Sinks,
		// A synthetic source satisfies the "at least one source"
		// requirement, which does not apply when the host collector
		// owns the receivers.
		Sources: []config.Source{syntheticSource()},
	}
	return full.Validate("exporter/trajectory")
}

func syntheticSource() config.Source {
	s := config.Source{Name: "otelcol", Type: "otlp"}
	s.HTTP.Listen = "127.0.0.1:0"
	return s
}
