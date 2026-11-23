"""How many of the dataset's laundering cycles the engine sees close.

    detektive-graph data/raw/HI-Small_Trans.csv --features features.csv
    python -m detektive_ml.cycle_report data/raw/HI-Small_Trans.csv \\
        data/raw/HI-Small_Patterns.txt features.csv

For every CYCLE attempt in the patterns file, the payment that closes it is
its last one in time order. The attempt counts as caught if the engine
reports at least one cycle closed by that payment. Also reports how many of
the payments that close a cycle (or touch an account already in one) are
laundering, against the base rate.

This reads the labels, so it is evaluation only: nothing here feeds the
engine.
"""

import argparse
from pathlib import Path

import polars as pl

from detektive_ml.data import read_raw_csv
from detektive_ml.patterns import attach_tx_ids, parse


def length_band(n: int) -> str:
    return "2" if n == 2 else "3-4" if n <= 4 else "5-8" if n <= 8 else "9-12"


def report(transactions_csv: Path, patterns: Path, features_csv: Path) -> pl.DataFrame:
    tx = read_raw_csv(transactions_csv).select(
        pl.col("tx_id").cast(pl.Int64), "timestamp", "is_laundering"
    )
    features = pl.read_csv(features_csv).join(tx, on="tx_id")
    order = {t: i for i, t in enumerate(features.sort(["timestamp", "tx_id"])["tx_id"].to_list())}
    closed = dict(zip(features["tx_id"].to_list(), features["cycles"].to_list()))

    attempts = [a for a in parse(patterns) if a.kind == "CYCLE"]
    missing = attach_tx_ids(attempts, transactions_csv)
    assert missing == 0, f"{missing} pattern rows not found in {transactions_csv}"
    rows = []
    for a in attempts:
        last = max(a.tx_ids, key=lambda t: order[t])
        rows.append({
            "payments": len(a.tx_ids),
            "band": length_band(len(a.tx_ids)),
            "caught": closed[last] > 0,
        })
    per_attempt = pl.DataFrame(rows)
    by_band = (
        per_attempt.group_by("band")
        .agg(pl.len().alias("attempts"), pl.col("caught").sum().alias("caught"))
        .sort("band")
    )
    print(f"CYCLE attempts caught as they close: {per_attempt['caught'].sum()} of {per_attempt.height}")
    print(by_band)

    base = features["is_laundering"].mean()
    print(f"\nlaundering among all payments: {base:.3%}")
    for name, cond in [
        ("payments closing a cycle", pl.col("cycles") > 0),
        ("  shortest cycle 2", pl.col("shortest_cycle") == 2),
        ("  shortest cycle 3+", pl.col("shortest_cycle") >= 3),
        ("payments by or to an account in a cycle", (pl.col("src_cycles") > 0) | (pl.col("dst_cycles") > 0)),
    ]:
        part = features.filter(cond)
        rate = part["is_laundering"].mean() if part.height else 0.0
        print(f"laundering among {name}: {rate:.2%} of {part.height:,}")
    return by_band


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("transactions", type=Path)
    parser.add_argument("patterns", type=Path)
    parser.add_argument("features", type=Path)
    args = parser.parse_args()
    report(args.transactions, args.patterns, args.features)


if __name__ == "__main__":
    main()
