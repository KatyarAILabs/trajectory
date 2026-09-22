# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0

# Build stage.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, so a source-only change does not refetch them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

# CGO off gives a static binary that runs in a distroless image with no libc.
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/KatyarAILabs/trajectory/internal/version.Collector=${VERSION} \
        -X github.com/KatyarAILabs/trajectory/internal/version.Commit=${COMMIT}" \
      -o /out/cc ./cmd/cc

# Runtime stage.
#
# F-12.5: non-root, distroless, read-only root filesystem apart from the buffer
# volume. `static-debian12` has no shell and no package manager, so a collector
# compromised through a parsing bug has nothing to pivot with — there is no sh
# to exec and nothing to install.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/cc /usr/local/bin/cc

# The buffer is the only path that needs to be writable. Mount a volume here;
# everything else can run read-only.
VOLUME ["/var/lib/cc"]

# 65532 is the distroless `nonroot` user.
USER 65532:65532

EXPOSE 4317 4318 4319 9464

ENTRYPOINT ["/usr/local/bin/cc"]
CMD ["run", "-config", "/etc/cc/config.yaml"]
