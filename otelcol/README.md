# OpenTelemetry Collector components

Q-2 resolved as "both": a standalone `cc` binary, and components that drop into
an OpenTelemetry Collector distribution (F-13.3).

The reasoning is that these serve different people. A team already running an
OTel Collector does not want a second agent to deploy, monitor and upgrade — for
them, trajectory should be an exporter in a pipeline they already have. A team
without one should not have to learn OTel Collector configuration to try this,
which is why the standalone binary exists and why `make demo` needs nothing but
a checkout.

## What is here

`trajectoryexporter` is a traces exporter that runs the same normalise →
assemble → redact → extract → sink pipeline as the standalone binary, reusing
the packages directly rather than reimplementing them. A second implementation
would drift, and the fidelity guarantees are the whole product.

## Building a distribution

```sh
go install go.opentelemetry.io/collector/cmd/builder@latest
builder --config otelcol/builder-config.yaml
```

## Configuration

```yaml
exporters:
  trajectory:
    tenant: acme
    buffer:
      dir: /var/lib/cc/buffer
    redaction:
      default: deny
      allow: ["steps[*].content_inline"]
      rules:
        - id: email
          match: {regex: "[\\w.+-]+@[\\w-]+\\.[\\w.]+"}
          action: tokenize
      tokenization: {key_env: CC_HMAC_KEY}
    sinks:
      - name: lake
        type: s3
        bucket: acme-agent-lake
        region: us-east-1

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [trajectory]
```

The `sources` section has no meaning here: the OTel Collector owns the
receivers. Everything else is the same configuration the standalone binary
takes, and is validated by the same code.

## A caveat worth knowing before you choose this

Running as an exporter puts the trajectory pipeline behind the host collector's
queueing and retry, which is configured separately and does not know about the
trajectory buffer. Two layers of retry can interact badly — the host collector
may drop a batch the trajectory buffer would have held.

If durability across a sink outage is the property you care about most, the
standalone binary is the simpler thing to reason about.
