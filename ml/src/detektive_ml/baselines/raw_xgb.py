"""The floor: XGBoost on what a single transaction row says about itself.

No graph, no account history. Whatever later models add is measured against
this number, so it has to be honest rather than tuned to look bad.

    python -m detektive_ml.baselines.raw_xgb data/processed/hi_small.parquet
"""

import argparse
import time
from pathlib import Path

import numpy as np
import polars as pl
import xgboost as xgb

from detektive_ml.data import load
from detektive_ml.evaluate import DEFAULT_SEEDS, Scores, format_summary, save, score_with_tail, summarise
from detektive_ml.split import sparse_tail_start, time_split

CATEGORICAL = ["payment_format", "payment_currency", "receiving_currency"]


def row_features(df: pl.DataFrame) -> pl.DataFrame:
    """Features computable from one row. Account ids are deliberately left out:
    an id the model memorised in training says nothing about a new account."""
    return df.select(
        pl.col("amount_paid").log1p().alias("log_amount_paid"),
        pl.col("amount_received").log1p().alias("log_amount_received"),
        (pl.col("amount_received") / pl.col("amount_paid")).fill_nan(1.0).alias("amount_ratio"),
        (pl.col("payment_currency") != pl.col("receiving_currency")).cast(pl.Int8).alias("cross_currency"),
        (pl.col("from_bank") == pl.col("to_bank")).cast(pl.Int8).alias("same_bank"),
        (pl.col("src") == pl.col("dst")).cast(pl.Int8).alias("self_transfer"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.weekday().alias("weekday"),
        *[pl.col(c) for c in CATEGORICAL],
    )


def encode(train: pl.DataFrame, others: list[pl.DataFrame]) -> list[np.ndarray]:
    """Integer-code categoricals with a mapping learned on training data only.
    Values never seen in training become -1."""
    out = []
    mappings = {c: {v: i for i, v in enumerate(train[c].unique().sort().to_list())} for c in CATEGORICAL}
    for frame in [train, *others]:
        coded = frame.with_columns(
            pl.col(c).replace_strict(m, default=-1, return_dtype=pl.Int32) for c, m in mappings.items()
        )
        out.append(coded.to_numpy().astype(np.float32))
    return out


def run(data_path: Path, seeds: tuple[int, ...], out_dir: Path) -> dict[int, Scores]:
    df = load(data_path)
    split = time_split(df)
    info = split.summary()
    tail_start = sparse_tail_start(df)
    info["sparse_tail_start"] = str(tail_start) if tail_start else None
    tail = (split.test["timestamp"] >= tail_start).to_numpy() if tail_start else None
    print(f"train {info['train']['rows']:,} | val {info['val']['rows']:,} | test {info['test']['rows']:,}")
    if tail is not None:
        print(f"sparse tail from {tail_start}: {int(tail.sum()):,} test rows also scored separately")

    X_train, X_val, X_test = encode(row_features(split.train), [row_features(split.val), row_features(split.test)])
    y_train, y_val, y_test = (p["is_laundering"].to_numpy() for p in (split.train, split.val, split.test))

    # weight positives up so the rare class is not ignored
    pos = max(int(y_train.sum()), 1)
    weight = (len(y_train) - pos) / pos

    runs: dict[int, Scores] = {}
    runs_without_tail: dict[int, Scores] = {}
    for seed in seeds:
        started = time.perf_counter()
        model = xgb.XGBClassifier(
            n_estimators=1000,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=weight,
            tree_method="hist",
            eval_metric="aucpr",
            early_stopping_rounds=200,
            random_state=seed,
            n_jobs=-1,
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
        result, trimmed = score_with_tail(
            y_val, model.predict_proba(X_val)[:, 1], y_test, model.predict_proba(X_test)[:, 1], tail
        )
        runs[seed] = result
        line = f"seed {seed}: F1 {result.f1 * 100:.2f}, PR-AUC {result.pr_auc * 100:.2f}"
        if trimmed:
            runs_without_tail[seed] = trimmed
            line += f" | without tail F1 {trimmed.f1 * 100:.2f}, PR-AUC {trimmed.pr_auc * 100:.2f}"
        print(f"{line}, trees {model.best_iteration + 1}, {time.perf_counter() - started:.1f}s")

    path = save(
        "raw_xgb", data_path.stem, runs, info, out_dir,
        runs_without_tail=runs_without_tail or None,
        extra={"features": row_features(split.train.head(1)).columns},
    )
    print("full test     :", format_summary(summarise(list(runs.values()))))
    if runs_without_tail:
        print("without tail  :", format_summary(summarise(list(runs_without_tail.values()))))
    print(f"saved {path}")
    return runs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path, help="prepared .parquet (or raw .csv)")
    parser.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args()
    run(args.data, tuple(args.seeds), args.out)


if __name__ == "__main__":
    main()
