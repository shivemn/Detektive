"""First look at an IBM AML file: size, time range, how rare laundering is, and
which payment types and patterns it hides in.

    python -m detektive_ml.explore data/processed/hi_small.parquet --patterns data/raw/HI-Small_Patterns.txt
"""

import argparse
import re
from collections import Counter
from pathlib import Path

import polars as pl

from detektive_ml.data import load
from detektive_ml.split import time_split

ATTEMPT_START = re.compile(r"BEGIN LAUNDERING ATTEMPT - ([A-Z\- ]+?)(?::|$)")


def section(title: str) -> None:
    print(f"\n## {title}\n")


def laundering_by(df: pl.DataFrame, column: str) -> pl.DataFrame:
    return (
        df.group_by(column)
        .agg(
            pl.len().alias("transactions"),
            pl.col("is_laundering").sum().alias("laundering"),
        )
        .with_columns((pl.col("laundering") / pl.col("transactions") * 100).round(3).alias("rate_pct"))
        .sort("laundering", descending=True)
    )


def degree_stats(df: pl.DataFrame) -> pl.DataFrame:
    out_deg = df.group_by("src").len().rename({"src": "account", "len": "out"})
    in_deg = df.group_by("dst").len().rename({"dst": "account", "len": "in"})
    both = out_deg.join(in_deg, on="account", how="full", coalesce=True).fill_null(0)
    return both.select(
        pl.lit("out").alias("direction"),
        pl.col("out").median().alias("median"),
        pl.col("out").quantile(0.99).alias("p99"),
        pl.col("out").max().alias("max"),
    ).vstack(
        both.select(
            pl.lit("in").alias("direction"),
            pl.col("in").median().alias("median"),
            pl.col("in").quantile(0.99).alias("p99"),
            pl.col("in").max().alias("max"),
        )
    )


def parse_patterns(path: Path) -> Counter:
    """Count laundering attempts per pattern type in a *_Patterns.txt file."""
    counts: Counter = Counter()
    for line in path.read_text().splitlines():
        match = ATTEMPT_START.search(line)
        if match:
            counts[match.group(1).strip()] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path)
    parser.add_argument("--patterns", type=Path, help="optional *_Patterns.txt file")
    args = parser.parse_args()

    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_hide_dataframe_shape(True)
    df = load(args.data)

    section("Size and time range")
    accounts = pl.concat([df["src"], df["dst"]]).n_unique()
    start, end = df["timestamp"].min(), df["timestamp"].max()
    positives = int(df["is_laundering"].sum())
    print(f"transactions : {df.height:,}")
    print(f"accounts     : {accounts:,}")
    print(f"time range   : {start} to {end} ({(end - start).days} days)")
    print(f"laundering   : {positives:,} ({positives / df.height * 100:.3f}%, 1 in {df.height // max(positives, 1):,})")
    print(f"self-transfers: {int((df['src'] == df['dst']).sum()):,}")

    section("Transactions per day (watch for a thin tail at the end)")
    print(df.group_by(pl.col("timestamp").dt.date().alias("day")).agg(
        pl.len().alias("transactions"), pl.col("is_laundering").sum().alias("laundering")
    ).sort("day"))

    section("Laundering by payment format")
    print(laundering_by(df, "payment_format"))

    section("Laundering by payment currency")
    print(laundering_by(df, "payment_currency"))

    section("Transactions per account")
    print(degree_stats(df))

    section("The 60/20/20 time split")
    split = time_split(df)
    info = split.summary()
    print(f"validation starts {info['val_start']}, test starts {info['test_start']}")
    for name in ("train", "val", "test"):
        part = info[name]
        print(f"{name:5}: {part['rows']:>10,} rows ({part['share'] * 100:.1f}%), "
              f"{part['laundering']:>6,} laundering ({part['laundering_rate'] * 100:.3f}%)")

    if args.patterns:
        section("Laundering attempts by pattern")
        for pattern, count in parse_patterns(args.patterns).most_common():
            print(f"{pattern:16} {count:>6,}")


if __name__ == "__main__":
    main()
