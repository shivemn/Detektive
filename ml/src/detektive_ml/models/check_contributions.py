"""Check the Rust scorer's explanations against LightGBM's own.

    detektive-score data/raw/HI-Small_Trans.csv --model models --explain explain.jsonl --explain-every 250
    python -m detektive_ml.models.check_contributions data/processed/hi_small.parquet graph.csv \\
        models explain.jsonl

For every payment in the file, LightGBM's `predict(pred_contrib=True)` gives
each feature's share of the raw score plus the expected value. The Rust
TreeSHAP must give the same numbers. The leaf values in the ONNX file are
32-bit floats, so they agree to about seven significant digits, not exactly.
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl

from detektive_ml.data import load
from detektive_ml.models.graph_boost import build, join_graph


def compare(rust: np.ndarray, rust_expected: np.ndarray, lgbm: np.ndarray) -> dict:
    """rust: (n, features); rust_expected: (n,); lgbm: (n, features + 1)."""
    diff = np.abs(rust - lgbm[:, :-1])
    top_rust = np.argmax(np.abs(rust), axis=1)
    top_lgbm = np.argmax(np.abs(lgbm[:, :-1]), axis=1)
    return {
        "payments": len(rust),
        "max_abs_diff": float(diff.max()),
        "max_expected_diff": float(np.abs(rust_expected - lgbm[:, -1]).max()),
        "largest_contribution": float(np.abs(lgbm[:, :-1]).max()),
        "same_top_reason": int(np.sum(top_rust == top_lgbm)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path)
    parser.add_argument("graph", type=Path)
    parser.add_argument("model", type=Path, help="directory with model.txt and model.json")
    parser.add_argument("explained", type=Path, help="JSON lines written by detektive-score --explain")
    parser.add_argument("--tolerance", type=float, default=1e-5)
    args = parser.parse_args()

    lines = [json.loads(line) for line in args.explained.read_text().splitlines() if line.strip()]
    wanted = pl.DataFrame({"tx_id": [r["tx_id"] for r in lines]}, schema={"tx_id": pl.UInt32})

    df, names = join_graph(load(args.data), args.graph)
    m = build(df, names, "both")
    order = df.sort(["timestamp", "tx_id"]).with_row_index("row").select("tx_id", "row")
    rows = wanted.join(order, on="tx_id", how="left")["row"].to_numpy()

    card = json.loads((args.model / "model.json").read_text())
    if card["features"] != m.names:
        sys.exit("the model's features differ from the ones built here")
    booster = lgb.Booster(model_file=str(args.model / "model.txt"))
    lgbm = booster.predict(m.X[rows], pred_contrib=True)

    rust = np.array([r["contributions"] for r in lines])
    rust_expected = np.array([r["expected"] for r in lines])
    alerts = np.array([r["alert"] for r in lines])
    result = {
        "all": compare(rust, rust_expected, lgbm),
        "alerts": compare(rust[alerts], rust_expected[alerts], lgbm[alerts]),
    }
    # the shares add up to the score the model gave
    raw = np.log(np.array([r["score"] for r in lines]) / (1 - np.array([r["score"] for r in lines])))
    result["max_sum_diff"] = float(np.abs(rust_expected + rust.sum(axis=1) - raw).max())
    print(json.dumps(result, indent=2))
    if result["all"]["max_abs_diff"] > args.tolerance or result["max_sum_diff"] > args.tolerance:
        sys.exit(1)


if __name__ == "__main__":
    main()
