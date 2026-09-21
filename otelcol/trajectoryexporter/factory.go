// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package trajectoryexporter

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// typeStr is the name used under `exporters:` in an OTel Collector config.
const typeStr = "trajectory"

// NewFactory returns the exporter factory the OTel Collector builder links in
// (F-13.3).
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		func() component.Config { return &Config{Settings: map[string]any{}} },
		exporter.WithTraces(createTraces, component.StabilityLevelAlpha),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	full, err := c.parse()
	if err != nil {
		return nil, err
	}

	exp, err := NewExporter(full, slog.Default())
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraces(ctx, set, cfg, exp.ConsumeTraces,
		exporterhelper.WithStart(func(ctx context.Context, _ component.Host) error {
			return exp.Start(ctx)
		}),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		// The host collector's queue and retry sit in front of this
		// exporter. They are disabled so there is exactly one durable
		// queue — the trajectory buffer — rather than two layers of
		// retry whose interaction nobody can reason about during an
		// outage.
		exporterhelper.WithQueue(configoptional.None[exporterhelper.QueueBatchConfig]()),
		exporterhelper.WithRetry(configretry.BackOffConfig{Enabled: false}),
	)
}
