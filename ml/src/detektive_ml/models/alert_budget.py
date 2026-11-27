"""How good the alerts are when a team can only review so many a day, and how
the model compares with plain rules.

    python -m detektive_ml.models.alert_budget data/processed/hi_small.parquet graph.csv \\
        models/scores.csv --patterns data/raw/HI-Small_Patterns.txt --raw data/raw/HI-Small_Trans.csv

F1 at one threshold is not how an alert queue is run. A team reviews a fixed
number of alerts a day, highest first. So for each daily budget K, every
detector raises its K most suspicious payments of each test day. We then count
how many of those are laundering (precision) and how much of the period's
laundering they catch (recall).

The rules are the kind a bank might write by hand from the patterns this
engine finds. Each fires or not, and a points total ranks the payments that
fire. They were fixed from what weeks 7 and 8 measured, before any of this was
run. Those measurements covered the whole file, test days included, so if
anything the rules are favoured. Payments with equal points are ordered at
random, with a fixed seed.

The second part looks at why test F1 is below validation F1. It sets out the
laundering in each period by the pattern it belongs to, from the patterns
file, and the model's recall on each.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from detektive_ml.data import load
from detektive_ml.models.graph_boost import join_graph
from detektive_ml.split import sparse_tail_start, time_split

BUDGETS = (10, 25, 50, 100, 250, 500, 1000)

# (name, condition, points)
RULES = (
    ("closes a ring of 3+ payments", pl.col("shortest_cycle") >= 3, 4),
    ("in a scatter-gather 3+ wide", pl.col("scatter_gather") >= 3, 3),
    ("receiver paid by 10+ accounts", pl.col("dst_fan_in") >= 10, 2),
    ("first payment to this payee, across currencies",
     (pl.col("pair_count") == 1) & (pl.col("payment_currency") != pl.col("receiving_currency")), 1),
)


def frame(parquet: Path, graph: Path, scores: Path) -> tuple[pl.DataFrame, dict]:
    df, _ = join_graph(load(parquet), graph)
    df = df.select("tx_id", "timestamp", "is_laundering", "payment_currency", "receiving_currency",
                   "shortest_cycle", "scatter_gather", "dst_fan_in", "pair_count")
    score = pl.read_csv(scores, schema={"tx_id": pl.UInt32, "score": pl.Float64})
    df = df.join(score, on="tx_id", how="left")
    if df["score"].null_count():
        raise ValueError("some payments have no score; was scores.csv made from the same data?")
    split = time_split(df.select("timestamp", "is_laundering"))
    tail = sparse_tail_start(df.select("timestamp"))
    period = (
        pl.when(pl.col("timestamp") < split.val_start).then(pl.lit("train"))
        .when(pl.col("timestamp") < split.test_start).then(pl.lit("val"))
        .when(pl.lit(tail is not None) & (pl.col("timestamp") >= pl.lit(tail))).then(pl.lit("tail"))
        .otherwise(pl.lit("test"))
    )
    points = sum(pl.when(cond).then(pts).otherwise(0) for _, cond, pts in RULES)
    df = df.with_columns(period.alias("period"), points.alias("points"),
                         pl.col("timestamp").dt.date().alias("day"))
    info = {"val_start": str(split.val_start), "test_start": str(split.test_start),
            "sparse_tail_start": str(tail) if tail else None}
    return df, info


def at_budgets(part: pl.DataFrame, rank: pl.Expr, budgets=BUDGETS, seed: int = 0) -> pl.DataFrame:
    """Precision and recall when each day's top K payments by `rank` alert.
    Payments with nothing to alert on (rank 0 or below) never alert."""
    rng = np.random.default_rng(seed)
    part = part.with_columns(rank.alias("rank"), pl.Series("tie", rng.random(part.height)))
    positives = int(part["is_laundering"].sum())
    ordered = part.filter(pl.col("rank") > 0).sort(["day", "rank", "tie"], descending=[False, True, False])
    ordered = ordered.with_columns(pl.int_range(pl.len()).over("day").alias("pos"))
    days = part["day"].n_unique()
    rows = []
    for k in budgets:
        hit = ordered.filter(pl.col("pos") < k)
        alerts, tp = hit.height, int(hit["is_laundering"].sum())
        rows.append({"per_day": k, "alerts": alerts, "alerts_per_day": alerts / days, "laundering": tp,
                     "precision": tp / alerts if alerts else 0.0, "recall": tp / positives})
    return pl.DataFrame(rows)


def rules_alone(part: pl.DataFrame) -> pl.DataFrame:
    positives = int(part["is_laundering"].sum())
    rows = []
    for name, cond, _ in RULES:
        fired = part.filter(cond)
        tp = int(fired["is_laundering"].sum())
        rows.append({"rule": name, "fires": fired.height, "laundering": tp,
                     "precision": tp / max(fired.height, 1), "recall": tp / positives})
    return pl.DataFrame(rows)


def pattern_kinds(raw_csv: Path, patterns_txt: Path) -> pl.DataFrame:
    """tx_id -> the kind of the listed attempt it belongs to."""
    from detektive_ml.patterns import attach_tx_ids, parse

    attempts = parse(patterns_txt)
    attach_tx_ids(attempts, raw_csv)
    pairs = [(t, a.kind) for a in attempts for t in a.tx_ids]
    return pl.DataFrame(pairs, schema={"tx_id": pl.UInt32, "kind": pl.String}, orient="row").unique("tx_id")


def gap(df: pl.DataFrame, kinds: pl.DataFrame, threshold: float) -> pl.DataFrame:
    """For validation and test: the laundering by listed pattern kind, and the
    model's recall on each at its threshold."""
    laundering = (
        df.filter(pl.col("period").is_in(["val", "test"]) & (pl.col("is_laundering") == 1))
        .join(kinds, on="tx_id", how="left")
        .with_columns(pl.col("kind").fill_null("not listed"), (pl.col("score") >= threshold).alias("caught"))
    )
    return (
        laundering.group_by("period", "kind")
        .agg(pl.len().alias("laundering"), pl.col("caught").mean().alias("recall"))
        .with_columns((pl.col("laundering") / pl.col("laundering").sum().over("period")).alias("share"))
        .sort("period", "laundering", descending=[True, True])
    )


def period_summary(df: pl.DataFrame, threshold: float) -> pl.DataFrame:
    return (
        df.filter(pl.col("period").is_in(["val", "test"]))
        .with_columns((pl.col("score") >= threshold).alias("alert"))
        .group_by("period")
        .agg(
            pl.len().alias("payments"),
            pl.col("is_laundering").sum().alias("laundering"),
            pl.col("is_laundering").mean().alias("rate"),
            pl.col("alert").sum().alias("alerts"),
            (pl.col("alert") & (pl.col("is_laundering") == 1)).sum().alias("caught"),
            pl.col("day").n_unique().alias("days"),
        )
        .with_columns(
            (pl.col("caught") / pl.col("alerts")).alias("precision"),
            (pl.col("caught") / pl.col("laundering")).alias("recall"),
        )
        .sort("period", descending=True)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path)
    parser.add_argument("graph", type=Path)
    parser.add_argument("scores", type=Path, help="scores.csv written by models.export --scores")
    parser.add_argument("--model-card", type=Path, help="model.json, for the threshold (default: next to scores)")
    parser.add_argument("--patterns", type=Path, help="HI-Small_Patterns.txt, for the validation/test comparison")
    parser.add_argument("--raw", type=Path, help="the raw transactions CSV the patterns refer to")
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args()

    card = json.loads((args.model_card or args.scores.with_name("model.json")).read_text())
    threshold = card["threshold"]
    df, info = frame(args.data, args.graph, args.scores)
    test = df.filter(pl.col("period") == "test")
    pl.Config.set_tbl_rows(40)
    pl.Config.set_tbl_cols(12)
    pl.Config.set_float_precision(3)

    print(f"test period without the sparse tail: {test.height:,} payments over {test['day'].n_unique()} days, "
          f"{int(test['is_laundering'].sum()):,} laundering")
    model = at_budgets(test, pl.col("score"))
    rules = at_budgets(test, pl.col("points").cast(pl.Float64))
    table = model.select("per_day", "alerts_per_day",
                         pl.col("precision").alias("model_precision"), pl.col("recall").alias("model_recall")).join(
        rules.select("per_day", pl.col("alerts_per_day").alias("rules_alerts_per_day"),
                     pl.col("precision").alias("rules_precision"), pl.col("recall").alias("rules_recall")),
        on="per_day")
    print(table)
    print("each rule on its own, over the test period:")
    print(rules_alone(test))
    summary = period_summary(df, threshold)
    print(f"at the model's own threshold ({threshold:.4f}):")
    print(summary)

    record = {"created_utc": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"), "split": info,
              "threshold": threshold, "budgets": table.to_dicts(), "rules_alone": rules_alone(test).to_dicts(),
              "periods": summary.to_dicts()}
    if args.patterns and args.raw:
        by_kind = gap(df, pattern_kinds(args.raw, args.patterns), threshold)
        print("laundering by listed pattern, and the model's recall on it:")
        print(by_kind)
        record["by_pattern"] = by_kind.to_dicts()

    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"alert_budget__{args.data.stem}__{record['created_utc']}.json"
    path.write_text(json.dumps(record, indent=2, default=str))
    print(f"saved {path}")


if __name__ == "__main__":
    main()
