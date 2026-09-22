# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0
#
# The OTel Collector distribution with the trajectory exporter.
# Build from the repository root, since the builder manifest resolves both
# modules from this checkout:
#   docker build -f otelcol/Dockerfile -t trajectory-otelcol .

FROM golang:1.26-alpine AS build

ARG BUILDER_VERSION=v0.161.0
RUN go install go.opentelemetry.io/collector/cmd/builder@${BUILDER_VERSION}

WORKDIR /src
COPY . .

WORKDIR /src/otelcol
# CGO off gives a static binary that runs in a distroless image with no libc.
RUN mkdir -p dist && CGO_ENABLED=0 builder --config builder-config.yaml

# Runtime stage: same posture as the standalone image (F-12.5).
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /src/otelcol/dist/trajectory-otelcol/trajectory-otelcol /usr/local/bin/trajectory-otelcol

VOLUME ["/var/lib/cc"]
USER 65532:65532

EXPOSE 4317 4318

ENTRYPOINT ["/usr/local/bin/trajectory-otelcol"]
CMD ["--config", "/etc/otelcol/config.yaml"]
