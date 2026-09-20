#!/usr/bin/env bash
# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0
#
# UC-5, end to end: run the collector against a local filesystem sink, send it
# a recorded OpenInference trajectory, and reconstruct that trajectory from the
# files on disk with DuckDB.
#
# No cloud account, no credentials, no Kubernetes. That is the point of UC-5:
# a contributor must be able to work on this with nothing but a checkout.
#
# DuckDB is optional; without it the script still produces the lake and tells
# you where to look.

set -euo pipefail
cd "$(dirname "$0")/.."

LAKE="${LAKE:-./var/lake}"
export CC_HMAC_KEY="${CC_HMAC_KEY:-dev-key-not-for-production}"

cleanup() { [[ -n "${CC_PID:-}" ]] && kill "$CC_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "==> building"
go build -o bin/cc ./cmd/cc

echo "==> validating config"
./bin/cc validate -config examples/local.yaml

echo "==> starting collector (lake: $LAKE)"
rm -rf "$LAKE"
./bin/cc run -config examples/local.yaml >/tmp/cc-demo.log 2>&1 &
CC_PID=$!

# Wait for readiness rather than sleeping a guessed interval.
for _ in $(seq 1 100); do
  if curl -sf http://127.0.0.1:9464/readyz >/dev/null 2>&1; then break; fi
  sleep 0.1
done

echo "==> sending a recorded OpenInference trajectory"
go run ./tools/gen-traffic -endpoint http://127.0.0.1:4318/v1/traces

# The episode closes once its settle period elapses, then the sink flushes on
# its own ticker. Shutting down forces both, so the demo does not race them.
sleep 1
echo "==> draining (SIGTERM triggers graceful flush)"
kill -TERM "$CC_PID"; wait "$CC_PID" 2>/dev/null || true
unset CC_PID

echo
echo "==> what landed"
find "$LAKE" -type f | sort | sed 's|^|    |'

if ! command -v duckdb >/dev/null 2>&1; then
  echo
  echo "DuckDB is not installed, so the reconstruction step is skipped."
  echo "Install it (brew install duckdb) and re-run, or query the files above yourself."
  exit 0
fi

echo
echo "==> reconstructing the trajectory with DuckDB"
duckdb -box <<SQL
SELECT e.episode_id, e.status, e.step_count,
       e.entity_keys[1].value AS ticket,
       e.fidelity.has_params  AS has_params
FROM read_parquet('$LAKE/episodes/**/*.parquet') e;

SELECT s.step_idx AS idx, s.parent_idx AS parent, s.kind,
       coalesce(s.tool_name, s.model) AS actor,
       s.params.seed AS seed,
       CASE WHEN s.content_ref IS NOT NULL
            THEN 'blob:' || substr(s.content_ref, 1, 8) ELSE 'inline' END AS payload,
       substr(coalesce(s.content_inline, ''), 1, 44) AS preview
FROM read_parquet('$LAKE/steps/**/*.parquet') s
ORDER BY s.step_idx;
SQL

echo "==> the seeded email must appear nowhere:"
if grep -rq "alice@example.com" "$LAKE" 2>/dev/null; then
  echo "    LEAK DETECTED" >&2
  exit 1
fi
echo "    clean - it is present only as its HMAC token"
