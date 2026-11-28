"""Gradient-boosted trees on the row features plus the graph engine's features.

The graph features come from the Rust tool, computed in stream order, so each
payment only sees the payments that arrived before it inside the window:

    detektive-graph data/raw/HI-Small_Trans.csv --batch 4096 --features graph.csv
    python -m detektive_ml.models.graph_boost data/processed/hi_small.parquet graph.csv

Same protocol as the floor (`baselines.raw_xgb`): time split, threshold from
validation, three seeds, scores with and without the sparse tail.

Settings were picked on validation PR-AUC with seed 0, never on test. Two
things mattered. Weighting positives up by the class ratio (~1000x), as the
floor does, made both libraries unstable on these features: LightGBM stopped
after one tree. And shallow, slow, regularised trees beat fast ones. So
weighting is off by default (`--class-weight` turns it back on) and both models
use a small learning rate with an L2 penalty and a minimum leaf size.

The whole of HI-Small has to fit in a few GB, so the feature matrix is built
once as float32 and the three splits are slices of it, not copies.
"""

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from detektive_ml.baselines.raw_xgb import CATEGORICAL, row_features
from detektive_ml.data import load
from detektive_ml.evaluate import DEFAULT_SEEDS, Scores, format_summary, save, score_with_tail, summarise
from detektive_ml.split import sparse_tail_start, time_split

FEATURE_SETS = ("row", "graph", "both")
MODELS = ("xgb", "lgbm")


@dataclass
class Matrix:
    X: np.ndarray
    y: np.ndarray
    names: list[str]
    train: slice
    val: slice
    test: slice
    tail: np.ndarray | None
    info: dict
    # the integer code of each category value, learned on training rows
    categories: dict[str, dict[str, int]]
    # tx_id of each row of X
    tx_ids: np.ndarray | None = None


def join_graph(df: pl.DataFrame, graph_csv: Path) -> tuple[pl.DataFrame, list[str]]:
    """Attach the graph features by tx_id. Every payment must have a row."""
    graph = pl.read_csv(graph_csv, infer_schema_length=0).select(pl.all().cast(pl.UInt32))
    names = [c for c in graph.columns if c != "tx_id"]
    joined = df.join(graph, on="tx_id", how="left").sort(["timestamp", "tx_id"])
    missing = joined[names[0]].null_count()
    if missing:
        raise ValueError(f"{missing:,} payments have no graph features; was the file made from the same data?")
    return joined, names


def build(
    df: pl.DataFrame,
    graph_names: list[str],
    which: str,
    drop: tuple[str, ...] = (),
    only_format: str | None = None,
) -> Matrix:
    """One float32 matrix for all rows, sorted by time, plus where each split starts.

    `drop` leaves the named feature columns out. `only_format` keeps only the
    payments of one payment format (and so leaves the format column out). The
    split boundaries and the sparse tail stay those of the whole file, so the
    periods are the same calendar days with or without the filter.
    """
    if which not in FEATURE_SETS:
        raise ValueError(f"feature set must be one of {FEATURE_SETS}")
    df = df.sort(["timestamp", "tx_id"])
    split = time_split(df.select("timestamp", "is_laundering"))
    tail_start = sparse_tail_start(df.select("timestamp"))
    if only_format is not None:
        df = df.filter(pl.col("payment_format") == only_format)
        if df.is_empty():
            raise ValueError(f"no payments with format {only_format!r}")
        drop = (*drop, "payment_format")
    ts = df["timestamp"].to_numpy()
    a = int(np.searchsorted(ts, np.datetime64(split.val_start), "left"))
    b = int(np.searchsorted(ts, np.datetime64(split.test_start), "left"))
    n = df.height

    columns: list[pl.Series] = []
    categories: dict[str, dict[str, int]] = {}
    if which in ("row", "both"):
        rows = row_features(df)
        # categorical codes learned on training rows only; unseen values become -1
        for c in CATEGORICAL:
            seen = rows[c].head(a).unique().sort().to_list()
            mapping = {v: i for i, v in enumerate(seen)}
            categories[c] = mapping
            rows = rows.with_columns(pl.col(c).replace_strict(mapping, default=-1, return_dtype=pl.Int32))
        columns += rows.get_columns()
    if which in ("graph", "both"):
        columns += df.select(graph_names).get_columns()
    present = {c.name for c in columns}
    unknown = [d for d in drop if d not in present and not (d == "payment_format" and only_format)]
    if unknown:
        raise ValueError(f"cannot drop {unknown}: not among the features")
    columns = [c for c in columns if c.name not in drop]
    categories = {k: v for k, v in categories.items() if k not in drop}

    X = np.empty((n, len(columns)), dtype=np.float32)
    for j, col in enumerate(columns):
        X[:, j] = col.cast(pl.Float32).to_numpy()
    names = [c.name for c in columns]

    tail = ts[b:] >= np.datetime64(tail_start) if tail_start else None
    info = split.summary()
    info["sparse_tail_start"] = str(tail_start) if tail_start else None
    if drop:
        info["dropped"] = list(drop)
    if only_format is not None:
        info["only_format"] = only_format
        # the counts are of the kept payments, not of the whole file
        y_all = df["is_laundering"].to_numpy()
        for part, s in (("train", slice(0, a)), ("val", slice(a, b)), ("test", slice(b, n))):
            rows, laundering = s.stop - s.start, int(y_all[s].sum())
            info[part] = {"rows": rows, "share": rows / n, "laundering": laundering,
                          "laundering_rate": laundering / max(rows, 1)}
    y = df["is_laundering"].to_numpy().astype(np.int8)
    return Matrix(X, y, names, slice(0, a), slice(a, b), slice(b, n), tail, info, categories,
                  df["tx_id"].to_numpy())


def fit_xgb(m: Matrix, seed: int, weight: float):
    import xgboost as xgb

    train = xgb.QuantileDMatrix(m.X[m.train], m.y[m.train], feature_names=m.names)
    val = xgb.QuantileDMatrix(m.X[m.val], m.y[m.val], ref=train, feature_names=m.names)
    params = {
        "objective": "binary:logistic",
        "max_depth": 6,
        "eta": 0.05,
        "min_child_weight": 5,
        "lambda": 10,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": weight,
        "tree_method": "hist",
        "eval_metric": "aucpr",
        "seed": seed,
        "nthread": -1,
    }
    booster = xgb.train(params, train, num_boost_round=2000, evals=[(val, "val")],
                        early_stopping_rounds=200, verbose_eval=False)
    del train
    best = (0, booster.best_iteration + 1)

    def predict(part: slice) -> np.ndarray:
        return booster.inplace_predict(m.X[part], iteration_range=best)

    gain = booster.get_score(importance_type="gain")
    return predict, best[1], {k: float(v) for k, v in gain.items()}


def train_lgbm(m: Matrix, seed: int, weight: float = 1.0):
    """The LightGBM booster with the settings chosen on validation."""
    import lightgbm as lgb

    train = lgb.Dataset(m.X[m.train], m.y[m.train], feature_name=m.names, free_raw_data=True)
    val = lgb.Dataset(m.X[m.val], m.y[m.val], reference=train, free_raw_data=True)
    params = {
        "objective": "binary",
        "num_leaves": 31,
        "learning_rate": 0.02,
        "min_child_samples": 200,
        "lambda_l2": 10,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "feature_fraction": 0.8,
        "scale_pos_weight": weight,
        "metric": "average_precision",
        "seed": seed,
        "verbose": -1,
    }
    return lgb.train(params, train, num_boost_round=3000, valid_sets=[val],
                     callbacks=[lgb.early_stopping(200, verbose=False)])


def fit_lgbm(m: Matrix, seed: int, weight: float):
    booster = train_lgbm(m, seed, weight)

    def predict(part: slice) -> np.ndarray:
        return booster.predict(m.X[part], num_iteration=booster.best_iteration)

    gain = dict(zip(m.names, booster.feature_importance("gain").tolist()))
    return predict, booster.best_iteration, gain


def run(
    m: Matrix, model: str, seeds: tuple[int, ...], class_weight: bool = False, scores: Path | None = None
) -> tuple[dict[int, Scores], dict[int, Scores], dict]:
    """Train and score one model per seed. With `scores`, the first seed's
    score for every row is written there (tx_id,score)."""
    fit = {"xgb": fit_xgb, "lgbm": fit_lgbm}[model]
    y_train, y_val, y_test = m.y[m.train], m.y[m.val], m.y[m.test]
    pos = max(int(y_train.sum()), 1)
    weight = (len(y_train) - pos) / pos if class_weight else 1.0

    runs: dict[int, Scores] = {}
    trimmed_runs: dict[int, Scores] = {}
    gains: dict[str, float] = {}
    for seed in seeds:
        started = time.perf_counter()
        predict, trees, gain = fit(m, seed, weight)
        full, trimmed = score_with_tail(y_val, predict(m.val), y_test, predict(m.test), m.tail)
        runs[seed] = full
        line = f"  seed {seed}: F1 {full.f1 * 100:.2f}, PR-AUC {full.pr_auc * 100:.2f}"
        if trimmed:
            trimmed_runs[seed] = trimmed
            line = (f"  seed {seed}: F1 {trimmed.f1 * 100:.2f}, PR-AUC {trimmed.pr_auc * 100:.2f}"
                    f" | incl. tail F1 {full.f1 * 100:.2f}, PR-AUC {full.pr_auc * 100:.2f}")
        print(f"{line}, trees {trees}, {time.perf_counter() - started:.0f}s", flush=True)
        for k, v in gain.items():
            gains[k] = gains.get(k, 0.0) + v / len(seeds)
        if scores is not None and seed == seeds[0]:
            if m.tx_ids is None:
                raise ValueError("this matrix does not know its tx_ids, so its scores cannot be written")
            scores.parent.mkdir(parents=True, exist_ok=True)
            everything = slice(0, len(m.y))
            order = np.argsort(m.tx_ids, kind="stable")
            pl.DataFrame({"tx_id": m.tx_ids[order], "score": predict(everything)[order]}).write_csv(scores)
            print(f"  scores of seed {seed} written to {scores}", flush=True)
    return runs, trimmed_runs, gains


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path, help="prepared .parquet (or raw .csv)")
    parser.add_argument("graph", type=Path, help="features file written by detektive-graph --features")
    parser.add_argument("--features", nargs="+", choices=FEATURE_SETS, default=["both"])
    parser.add_argument("--models", nargs="+", choices=MODELS, default=list(MODELS))
    parser.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    parser.add_argument("--class-weight", action="store_true", help="weight positives by the class ratio")
    parser.add_argument("--drop", nargs="+", default=[], metavar="FEATURE", help="leave these features out")
    parser.add_argument("--only-format", metavar="FORMAT",
                        help="train and test on payments of this format only (e.g. ACH)")
    parser.add_argument("--scores", action="store_true",
                        help="write the first seed's score for every payment next to the results")
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args()

    df, graph_names = join_graph(load(args.data), args.graph)
    df = df.select("tx_id", "timestamp", "is_laundering", "amount_paid", "amount_received",
                   "payment_currency", "receiving_currency", "payment_format",
                   "from_bank", "to_bank", "src", "dst", *graph_names)
    for which in args.features:
        m = build(df, graph_names, which, tuple(args.drop), args.only_format)
        print(f"features: {which} ({len(m.names)} columns) | train {m.train.stop:,} | "
              f"val {m.val.stop - m.val.start:,} | test {m.test.stop - m.test.start:,}", flush=True)
        for model in args.models:
            print(f"{model}:", flush=True)
            name = f"{model}_{which}" + ("_weighted" if args.class_weight else "")
            name += "".join(f"_no-{d}" for d in args.drop)
            name += f"_only-{args.only_format}" if args.only_format else ""
            scores = args.out / f"scores__{name}__{args.data.stem}.csv" if args.scores else None
            runs, trimmed, gains = run(m, model, tuple(args.seeds), args.class_weight, scores)
            top = sorted(gains.items(), key=lambda kv: -kv[1])
            path = save(
                name, args.data.stem, runs, m.info, args.out,
                runs_without_tail=trimmed or None,
                extra={"features": m.names, "graph_features": str(args.graph.name),
                       "gain": {k: round(v, 1) for k, v in top}},
            )
            if trimmed:
                print("  without tail:", format_summary(summarise(list(trimmed.values()))))
            print("  full test   :", format_summary(summarise(list(runs.values()))))
            print("  top features:", ", ".join(k for k, _ in top[:8]))
            print(f"  saved {path}", flush=True)
        del m


if __name__ == "__main__":
    main()
