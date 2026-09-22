# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0

SHELL := /bin/bash
MODULE := github.com/KatyarAILabs/trajectory
BIN := $(CURDIR)/bin
PROTOS := $(shell find spec/proto -name '*.proto')

export PATH := $(BIN):$(PATH)

.PHONY: all
all: generate build test lint

.PHONY: tools
tools: ## Install pinned code-generation and audit tools into ./bin
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	@echo "licence gate is ./tools/licensecheck; no external tool needed"

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
lint: logcheck
	go vet ./...
	@test -z "$$(gofmt -l . | grep -vE '^(gen/|otelcol/dist/|sdk/typescript/node_modules/)')" || { echo "gofmt needed:"; gofmt -l . | grep -vE '^(gen/|otelcol/dist/|sdk/typescript/node_modules/)'; exit 1; }

.PHONY: logcheck
logcheck: ## F-12.2: no log call site may write payload content
	go run ./tools/logcheck ./collector ./cmd ./importers ./pkg ./internal ./tools

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
	go run ./tools/licensecheck

.PHONY: docker
docker: ## F-12.5: distroless, non-root, read-only rootfs
	docker build \
	  --build-arg VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev) \
	  --build-arg COMMIT=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown) \
	  -t trajectory:dev .

.PHONY: sample
sample: ## Regenerate the published sample dataset (F-10.5)
	@rm -rf spec/testdata/sample-dataset
	@go build -o $(BIN)/cc ./cmd/cc
	@# The README lives outside the generated directory so regenerating the
	@# data cannot delete it; it is copied in at the end.
	@rm -rf /tmp/cc-sample-buffer
	@CC_HMAC_KEY=sample-dataset-key-published-with-the-spec \
	  $(BIN)/cc import -config spec/testdata/sample.yaml \
	    -from spec/testdata/langfuse-export.json langfuse
	@# Outcomes and rewards at a fixed as-of, so the sample is reproducible.
	@CC_HMAC_KEY=sample-dataset-key-published-with-the-spec \
	  $(BIN)/cc outcomes -config spec/testdata/sample.yaml -from spec/testdata/sample-outcomes.csv
	@$(BIN)/cc score -lake spec/testdata/sample-dataset -as-of 2026-10-31T00:00:00Z \
	  -horizon 30d -scorer examples/scorer.yaml
	@$(BIN)/cc conform spec/testdata/sample-dataset
	@cp spec/testdata/SAMPLE-DATASET.md spec/testdata/sample-dataset/README.md

.PHONY: conform
conform: ## Run the conformance suite against the sample dataset
	@go build -o $(BIN)/cc ./cmd/cc
	@$(BIN)/cc conform -v spec/testdata/sample-dataset

.PHONY: chaos
chaos: ## Chaos tests: sink outage, unclean restart, partial write
	go test ./collector/acceptance/ -run Chaos -v

.PHONY: race
race:
	go test -race ./...

.PHONY: loadtest
loadtest: ## Measure the §13 throughput and memory targets
	@echo "start the collector first:  CC_HMAC_KEY=x ./bin/cc run -config examples/local.yaml"
	go run ./tools/loadtest -duration 30s -workers 8

.PHONY: help
help:
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: demo
demo: ## UC-5: run the collector locally, send a trajectory, reconstruct it
	./scripts/demo.sh

.PHONY: demo-outcomes
demo-outcomes: ## The outcome join end to end: capture, outcomes, join, score, export
	./scripts/demo-outcomes.sh

.PHONY: acceptance
acceptance: ## Run the DoD-1 end-to-end acceptance test
	go test ./collector/acceptance/ -v -run TestDoD1
