#!/usr/bin/env bash
# Copyright The Trajectory Authors.
# SPDX-License-Identifier: Apache-2.0
#
# The outcome join, end to end on a laptop: capture a trajectory, load the
# business outcomes that followed it, label it, score it, export training data.
#
# This is the design-partner test from the differentiation doc, run on sample
# data: if the labelled output is something a partner "cannot get anywhere
# else", the bet holds.

set -euo pipefail
cd "$(dirname "$0")/.."

LAKE=./var/lake
export CC_HMAC_KEY="${CC_HMAC_KEY:-dev-key-not-for-production}"

echo "==> 1. capture a trajectory (make demo)"
./scripts/demo.sh >/dev/null
go build -o bin/cc ./cmd/cc

echo "==> 2. load the outcomes that followed it (in practice: a ticketing export)"
./bin/cc outcomes -config examples/local.yaml -from examples/outcomes.csv 2>/dev/null

# The fixture's trajectory is dated 2026-09-20; a 1-day horizon closed by
# 2026-09-22, so as of then its label is final.
AS_OF=2026-09-22T00:00:00Z

echo
echo "==> 3. join outcomes to trajectories, as of $AS_OF"
./bin/cc join -lake "$LAKE" -as-of "$AS_OF" -horizon 1d -out var/labels.jsonl

echo
echo "==> 4. score with rules"
./bin/cc score -lake "$LAKE" -as-of "$AS_OF" -horizon 1d -scorer examples/scorer.yaml

echo
echo "==> 5. export a training dataset"
./bin/cc export -lake "$LAKE" -as-of "$AS_OF" -horizon 1d -format trajectories -out var/train.jsonl

echo
echo "==> the labelled, scored trajectory"
python3 - <<'PY'
import json
t = json.loads(open("var/train.jsonl").readline())
print(f"  episode   {t['episode_id']}")
print(f"  label     {t['label_status']}")
print(f"  reward    {t.get('reward')}  via {list((t.get('clauses') or {}).keys())}")
for o in t["outcomes"]:
    print(f"  outcome   {o['kind']} = {o['value']}")
print(f"  steps     {len(t['steps'])}")
PY
