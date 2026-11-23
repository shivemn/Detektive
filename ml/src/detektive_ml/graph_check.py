"""Check the Rust engine's degree and fan features against a plain recount.

For every payment, the window is the payments up to and including it (in
time order, ties by tx_id) whose time is no more than the window length
before it. This script recounts each feature over that slice with numpy,
without any of the engine's incremental bookkeeping, and compares.

    detektive-graph sample.csv --window-hours 24 --features features.csv
    python -m detektive_ml.graph_check sample.csv features.csv --window-hours 24

Exits non-zero, printing the first disagreements, if any feature differs.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl

from detektive_ml.data import read_raw_csv

FEATURES = [
    "src_out_degree", "src_fan_out", "src_in_degree", "src_fan_in",
    "dst_in_degree", "dst_fan_in", "dst_out_degree", "dst_fan_out",
    "pair_count",
]


def recount(transactions: pl.DataFrame, window_seconds: int) -> pl.DataFrame:
    """Degree and fan features for every row, by brute force."""
    ts = transactions["timestamp"].dt.epoch("s").to_numpy()
    # account keys to small integers, so comparisons are cheap
    keys = pl.concat([transactions["src"], transactions["dst"]]).unique().sort()
    index = {k: i for i, k in enumerate(keys.to_list())}
    src = np.array([index[k] for k in transactions["src"].to_list()])
    dst = np.array([index[k] for k in transactions["dst"].to_list()])

    rows = []
    for i in range(len(ts)):
        lo = np.searchsorted(ts, ts[i] - window_seconds, side="left")
        s, d = src[lo:i + 1], dst[lo:i + 1]
        a, b = src[i], dst[i]
        a_out, a_in, b_in, b_out = s == a, d == a, d == b, s == b
        rows.append((
            a_out.sum(), len(np.unique(d[a_out])), a_in.sum(), len(np.unique(s[a_in])),
            b_in.sum(), len(np.unique(s[b_in])), b_out.sum(), len(np.unique(d[b_out])),
            (a_out & (d == b)).sum(),
        ))
    out = pl.DataFrame(np.array(rows, dtype=np.int64), schema=FEATURES, orient="row")
    return out.with_columns(transactions["tx_id"].cast(pl.Int64).alias("tx_id"))


def compare(transactions_csv: Path, features_csv: Path, window_hours: float) -> int:
    """Number of payments on which any feature differs."""
    transactions = read_raw_csv(transactions_csv)
    expected = recount(transactions, round(window_hours * 3600))
    engine = pl.read_csv(features_csv, schema_overrides={c: pl.Int64 for c in ["tx_id", *FEATURES]})

    if engine.height != expected.height:
        print(f"engine wrote {engine.height} rows, expected {expected.height}")
        return max(engine.height, expected.height)
    joined = expected.join(engine, on="tx_id", how="left", suffix="_engine")
    differs = pl.any_horizontal(
        [pl.col(f) != pl.col(f"{f}_engine") for f in FEATURES]
    ) | pl.col("src_out_degree_engine").is_null()
    bad = joined.filter(differs)
    if bad.height:
        print(f"{bad.height} of {joined.height} payments differ; first few:")
        print(bad.head(5))
    else:
        print(f"all {joined.height} payments match on {len(FEATURES)} features")
    return bad.height


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("transactions", type=Path)
    parser.add_argument("features", type=Path)
    parser.add_argument("--window-hours", type=float, default=24.0)
    args = parser.parse_args()
    sys.exit(1 if compare(args.transactions, args.features, args.window_hours) else 0)


if __name__ == "__main__":
    main()
