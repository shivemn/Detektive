#!/usr/bin/env bash
# Cross-check the Rust engine's degree and fan features against a brute-force
# recount in Python, on a synthetic sample, with a long and a short window.
# Runs in CI; works locally too (needs cargo and the ml package installed).
#
#   bash scripts/check_features.sh
#   ROWS=50000 bash scripts/check_features.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS=${ROWS:-20000}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

cargo build --release -q -p detektive-graph
BIN=${CARGO_TARGET_DIR:-target}/release/detektive-graph

PYTHONPATH=ml/src python3 -m detektive_ml.synthetic "$WORK/sample.csv" --rows "$ROWS" >/dev/null
for hours in 24 1; do
    echo "== ${hours}h window"
    "$BIN" "$WORK/sample.csv" --window-hours "$hours" --features "$WORK/features.csv" 2>&1 |
        grep "payments through"
    PYTHONPATH=ml/src python3 -m detektive_ml.graph_check \
        "$WORK/sample.csv" "$WORK/features.csv" --window-hours "$hours"
done
