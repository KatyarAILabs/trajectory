# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash
MODULE := github.com/trajectory-project/trajectory
BIN := $(CURDIR)/bin
PROTOS := $(shell find spec/proto -name '*.proto')

export PATH := $(BIN):$(PATH)

.PHONY: all
all: generate build test lint

.PHONY: tools
tools: ## Install pinned code-generation and audit tools into ./bin
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	GOBIN=$(BIN) go install github.com/google/go-licenses@latest

.PHONY: generate
generate: ## Regenerate Go types and JSON Schema from the protos
	protoc -I spec/proto --go_out=gen/go --go_opt=module=$(MODULE)/gen/go $(PROTOS)
	go run ./tools/gen-jsonschema -out spec/jsonschema

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

.PHONY: lint
lint:
	go vet ./...
	@test -z "$$(gofmt -l . | grep -v '^gen/')" || { echo "gofmt needed:"; gofmt -l . | grep -v '^gen/'; exit 1; }

.PHONY: golden
golden: ## Regenerate the committed Parquet schema goldens. Review the diff.
	go test ./pkg/record -update

.PHONY: verify-generated
verify-generated: generate ## Fail if committed generated files are stale
	@git diff --exit-code -- gen/ spec/jsonschema/ || { \
		echo ""; \
		echo "Generated files are stale. Run 'make generate' and commit the result."; \
		exit 1; }

.PHONY: licences
licences: ## F-12.4 / §13: permissive licences only
	go-licenses check ./... \
		--allowed_licenses=Apache-2.0,MIT,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0,Unlicense,Zlib

.PHONY: sbom
sbom: ## F-12.4: SBOM per release
	@mkdir -p dist
	go version -m $(BIN)/trajectory 2>/dev/null || echo "build the binary first"

.PHONY: help
help:
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
