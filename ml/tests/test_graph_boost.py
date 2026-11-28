import json

import numpy as np
import polars as pl
import pytest

from detektive_ml.data import read_raw_csv
from detektive_ml.evaluate import summarise
from detektive_ml.models.graph_boost import build, join_graph, run
from detektive_ml.synthetic import write_sample


@pytest.fixture(scope="module")
def sample(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("graph_boost")
    df = read_raw_csv(write_sample(tmp / "sample.csv", rows=20_000, positive_rate=0.1))
    # stand-in for the Rust tool's output: one column that mostly follows the
    # label, one that is noise. Written in a shuffled order, like stream order.
    rng = np.random.default_rng(7)
    y = df["is_laundering"].to_numpy()
    graph = pl.DataFrame({
        "tx_id": df["tx_id"],
        "cycles": np.where(rng.random(len(y)) < 0.7, y, 0).astype(np.uint32),
        "src_fan_out": rng.integers(1, 9, len(y)).astype(np.uint32),
    }).sample(fraction=1.0, shuffle=True, seed=1)
    path = tmp / "graph.csv"
    graph.write_csv(path)
    return df, path


def test_every_payment_needs_graph_features(sample, tmp_path):
    df, path = sample
    partial = tmp_path / "partial.csv"
    pl.read_csv(path).head(100).write_csv(partial)
    with pytest.raises(ValueError, match="no graph features"):
        join_graph(df, partial)


def test_splits_are_time_ordered_slices(sample):
    df, path = sample
    joined, names = join_graph(df, path)
    assert names == ["cycles", "src_fan_out"]
    m = build(joined, names, "both")

    assert m.train.start == 0 and m.train.stop == m.val.start
    assert m.val.stop == m.test.start and m.test.stop == df.height
    assert m.X.dtype == np.float32 and m.X.shape == (df.height, len(m.names))
    assert m.names[-2:] == names
    # rows came back sorted by time, and labels moved with their rows
    ordered = joined.sort(["timestamp", "tx_id"])
    assert np.array_equal(m.y, ordered["is_laundering"].to_numpy())
    assert np.array_equal(m.X[:, -2], ordered["cycles"].to_numpy().astype(np.float32))
    assert m.info["train"]["rows"] == m.train.stop


def test_feature_sets_pick_columns(sample):
    df, path = sample
    joined, names = join_graph(df, path)
    row, graph, both = (build(joined, names, w) for w in ("row", "graph", "both"))
    assert graph.names == names
    assert both.names == row.names + names
    with pytest.raises(ValueError):
        build(joined, names, "everything")


def test_unseen_categories_become_minus_one(sample):
    df, path = sample
    joined, names = join_graph(df, path)
    last = joined.sort("timestamp")["tx_id"][-1]
    joined = joined.with_columns(
        pl.when(pl.col("tx_id") == last).then(pl.lit("Carrier pigeon")).otherwise(pl.col("payment_format")).alias("payment_format")
    )
    m = build(joined, names, "row")
    column = m.names.index("payment_format")
    assert m.X[-1, column] == -1
    assert (m.X[m.train, column] >= 0).all()


@pytest.mark.parametrize("model", ["xgb", "lgbm"])
def test_models_learn_the_planted_signal(sample, model):
    df, path = sample
    joined, names = join_graph(df, path)
    m = build(joined, names, "graph")
    runs, _, gains = run(m, model, seeds=(0,))
    # the planted column alone gives recall ~0.7 at near-perfect precision.
    # (the sample needs enough positives to clear the minimum leaf sizes)
    assert summarise(list(runs.values()))["f1"]["mean"] > 50
    assert max(gains, key=gains.get) == "cycles"
    json.dumps(gains)  # saved with the results, so it must serialise


def test_dropping_features_leaves_them_out(sample):
    df, path = sample
    joined, names = join_graph(df, path)
    full = build(joined, names, "both")
    less = build(joined, names, "both", drop=("payment_format", "cycles"))
    assert "payment_format" not in less.names and "cycles" not in less.names
    assert less.names == [n for n in full.names if n not in ("payment_format", "cycles")]
    assert "payment_format" not in less.categories
    assert np.array_equal(less.X[:, less.names.index("hour")], full.X[:, full.names.index("hour")])
    with pytest.raises(ValueError, match="cannot drop"):
        build(joined, names, "both", drop=("no_such_feature",))


def test_one_format_keeps_the_calendar_split(sample):
    df, path = sample
    joined, names = join_graph(df, path)
    full = build(joined, names, "both")
    fmt = joined["payment_format"].mode()[0]
    one = build(joined, names, "both", only_format=fmt)
    assert "payment_format" not in one.names
    kept = joined.filter(pl.col("payment_format") == fmt).height
    assert len(one.y) == kept < len(full.y)
    # same calendar boundaries as the whole file
    assert one.info["val_start"] == full.info["val_start"]
    assert one.info["test_start"] == full.info["test_start"]
    assert one.info["test"]["rows"] == one.test.stop - one.test.start
    assert one.info["test"]["laundering"] == int(one.y[one.test].sum())
    assert set(one.tx_ids) <= set(full.tx_ids)


def test_scores_are_written_for_every_row(sample, tmp_path):
    df, path = sample
    joined, names = join_graph(df, path)
    m = build(joined, names, "graph")
    out = tmp_path / "scores.csv"
    run(m, "lgbm", seeds=(0,), scores=out)
    written = pl.read_csv(out)
    assert written.height == len(m.y)
    assert written["tx_id"].is_sorted() and written["tx_id"].n_unique() == written.height
    assert written["score"].is_between(0, 1).all()
