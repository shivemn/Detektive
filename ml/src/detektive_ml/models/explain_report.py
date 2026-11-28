"""What drives the alerts: the reasons behind every test-period alert, summed
up.

    detektive-score data/raw/HI-Small_Trans.csv --model models --explain explain.jsonl
    python -m detektive_ml.models.explain_report data/processed/hi_small.parquet graph.csv \\
        models/scores.csv explain.jsonl --patterns data/raw/HI-Small_Patterns.txt --raw data/raw/HI-Small_Trans.csv

For the alerts of the test period (sparse tail left out), this reports three
things:

- which features carry the most weight, as the mean absolute share of the
  log-odds;
- how often each feature is the single largest reason;
- the top reason for each kind of laundering the alerts catch, and for the
  false alarms.

On HI-Small the largest reason is the payment method for every alert,
because nearly all of IBM's listed laundering is ACH. So the per-kind table
also gives the largest reason *other than* the method, which is where the
kinds differ.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl

from detektive_ml.models.alert_budget import frame, pattern_kinds


def load_explanations(path: Path, names: list[str]) -> pl.DataFrame:
    lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    phi = np.array([r["contributions"] for r in lines])
    df = pl.DataFrame({"tx_id": [r["tx_id"] for r in lines], "alert": [r["alert"] for r in lines]},
                      schema={"tx_id": pl.UInt32, "alert": pl.Boolean})
    top = np.argmax(np.abs(phi), axis=1)
    rest = np.abs(phi).copy()
    rest[:, names.index("payment_format")] = -1
    second = np.argmax(rest, axis=1)
    df = df.with_columns(
        pl.Series("top_reason", [names[i] for i in top]),
        pl.Series("top_share", phi[np.arange(len(phi)), top]),
        pl.Series("top_other", [names[i] for i in second]),
    )
    return df.hstack(pl.DataFrame(phi, schema=[f"phi_{n}" for n in names]))


def weights(df: pl.DataFrame, names: list[str]) -> pl.DataFrame:
    return (
        pl.DataFrame({
            "feature": names,
            "mean_abs_log_odds": [df[f"phi_{n}"].abs().mean() for n in names],
            "mean_log_odds": [df[f"phi_{n}"].mean() for n in names],
        })
        .join(df.group_by("top_reason").len().rename({"top_reason": "feature", "len": "top_reason_of"}),
              on="feature", how="left")
        .with_columns(pl.col("top_reason_of").fill_null(0))
        .sort("mean_abs_log_odds", descending=True)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path)
    parser.add_argument("graph", type=Path)
    parser.add_argument("scores", type=Path)
    parser.add_argument("explained", type=Path, help="JSON lines written by detektive-score --explain")
    parser.add_argument("--patterns", type=Path)
    parser.add_argument("--raw", type=Path)
    args = parser.parse_args()

    card = json.loads(args.scores.with_name("model.json").read_text())
    names = card["features"]
    df, _ = frame(args.data, args.graph, args.scores)
    explained = load_explanations(args.explained, names).filter(pl.col("alert"))
    alerts = explained.join(df.select("tx_id", "period", "is_laundering"), on="tx_id").filter(
        pl.col("period") == "test")
    pl.Config.set_tbl_rows(40)
    pl.Config.set_float_precision(3)
    caught = int(alerts["is_laundering"].sum())
    print(f"test-period alerts: {alerts.height}, of which laundering {caught}")
    print(weights(alerts, names).head(15))

    if args.patterns and args.raw:
        kinds = pattern_kinds(args.raw, args.patterns)
        alerts = alerts.join(kinds, on="tx_id", how="left").with_columns(
            pl.when(pl.col("is_laundering") == 0).then(pl.lit("false alarm"))
            .otherwise(pl.col("kind").fill_null("not listed")).alias("kind"))
        for column, title in (("top_reason", "largest reason"),
                              ("top_other", "largest reason other than the payment method")):
            by_kind = (
                alerts.group_by("kind", column).len()
                .sort("len", descending=True)
                .group_by("kind", maintain_order=True)
                .agg(pl.col(column).head(3), pl.col("len").head(3), pl.col("len").sum().alias("alerts"))
                .sort("alerts", descending=True)
            )
            print(f"{title}, by what the alert turned out to be:")
            for row in by_kind.iter_rows(named=True):
                parts = ", ".join(f"{r} ({n})" for r, n in zip(row[column], row["len"]))
                print(f"  {row['kind']:15s} {row['alerts']:4d} alerts: {parts}")


if __name__ == "__main__":
    main()
