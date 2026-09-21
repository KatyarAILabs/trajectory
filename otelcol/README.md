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

Settings are exactly the `cc` config format, minus `sources`, and go through
the same strict parser — an unknown key is an error here too.

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

## Queueing and retry

The exporter disables the host collector's own queue and retry. The trajectory
buffer is already a durable, bounded queue with backoff and dead-lettering; two
layers of retry in series are hard to reason about during an outage, and the
host's in-memory queue would drop on restart what the buffer would have held.

## It is a separate Go module

`otelcol/trajectoryexporter` has its own `go.mod`, so the OpenTelemetry
Collector framework is not pulled into the standalone `cc` binary. Test it with:

```sh
cd otelcol/trajectoryexporter && go test ./...
```
