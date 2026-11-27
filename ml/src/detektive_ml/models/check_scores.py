"""Compare two score files (tx_id,score) row by row.

    python -m detektive_ml.models.check_scores models/scores.csv rust_scores.csv --threshold 0.16

Prints the largest difference and how many alert decisions differ, and exits
non-zero if any payment is missing or any decision clearly flips. A payment
sitting within `--margin` of the threshold is reported but not counted as a
flip: the threshold is itself one validation payment's score.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl


def check(a: pl.DataFrame, b: pl.DataFrame, threshold: float, margin: float) -> dict:
    joined = a.join(b, on="tx_id", how="full", suffix="_b", coalesce=True)
    missing = int(joined["score"].null_count() + joined["score_b"].null_count())
    joined = joined.drop_nulls()
    x, y = joined["score"].to_numpy(), joined["score_b"].to_numpy()
    diff = np.abs(x - y)
    near = np.abs(x - threshold) <= margin
    flips = (x >= threshold) != (y >= threshold)
    return {
        "rows": joined.height,
        "missing": missing,
        "max_abs_diff": float(diff.max()) if len(diff) else 0.0,
        "mean_abs_diff": float(diff.mean()) if len(diff) else 0.0,
        "alert_flips": int(np.sum(flips & ~near)),
        "on_the_threshold": int(np.sum(near)),
        "alerts": int(np.sum(x >= threshold)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("reference", type=Path)
    parser.add_argument("other", type=Path)
    parser.add_argument("--threshold", type=float, required=True)
    parser.add_argument("--margin", type=float, default=1e-6)
    parser.add_argument("--tolerance", type=float, default=1e-6)
    args = parser.parse_args()
    schema = {"tx_id": pl.UInt32, "score": pl.Float64}
    result = check(pl.read_csv(args.reference, schema=schema), pl.read_csv(args.other, schema=schema),
                   args.threshold, args.margin)
    print(result)
    if result["missing"] or result["alert_flips"] or result["max_abs_diff"] > args.tolerance:
        sys.exit(1)


if __name__ == "__main__":
    main()
