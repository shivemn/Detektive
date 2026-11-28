import lightgbm as lgb
import numpy as np
import polars as pl

from detektive_ml.models.check_scores import check
from detektive_ml.models.export import compare, onnx_scores, round_thresholds_down, to_onnx


def booster_and_data(seed=0):
    rng = np.random.default_rng(seed)
    # values on a coarse grid, so many rows sit exactly on split thresholds
    X = (rng.integers(0, 40, (4000, 3)) / 7).astype(np.float32)
    X[:, 2] = np.log1p(rng.choice([351.0, 285.55, 927.04, 10.0], 4000)).astype(np.float32)
    y = ((X[:, 0] + X[:, 2] + rng.normal(0, 1, 4000)) > 9).astype(int)
    params = {"objective": "binary", "num_leaves": 15, "min_child_samples": 5, "verbose": -1, "seed": seed}
    return lgb.train(params, lgb.Dataset(X, y), 40), X


def test_rounded_thresholds_decide_as_lightgbm_does():
    booster, X = booster_and_data()
    model = to_onnx(booster, X.shape[1])
    changed = round_thresholds_down(model, booster)
    assert changed > 0, "the grid should put some thresholds between two f32 values"
    diff = np.abs(booster.predict(X) - onnx_scores(model, X))
    # onnxruntime adds in f32; a different path through a tree would be far bigger
    assert diff.max() < 1e-5


def test_round_thresholds_is_idempotent():
    booster, X = booster_and_data(1)
    model = to_onnx(booster, X.shape[1])
    round_thresholds_down(model, booster)
    assert round_thresholds_down(model, booster) == 0


def test_compare_counts_flips():
    a = np.array([0.1, 0.5, 0.9])
    assert compare(a, a, 0.5) == {"rows": 3, "max_abs_diff": 0.0, "alert_flips": 0, "on_the_threshold": 1}
    # the payment sitting on the threshold may fall either side
    assert compare(a, a - [0, 1e-7, 0], 0.5)["alert_flips"] == 0
    # one clear of it may not
    assert compare(a, a - [0, 0, 0.5], 0.5)["alert_flips"] == 1


def test_check_scores_joins_by_tx_id_and_spares_the_threshold():
    a = pl.DataFrame({"tx_id": [1, 2, 3, 4], "score": [0.1, 0.5, 0.9, 0.3]}, schema={"tx_id": pl.UInt32, "score": pl.Float64})
    b = a.sample(fraction=1.0, shuffle=True, seed=0).with_columns(
        pl.when(pl.col("tx_id") == 2).then(0.4999999).otherwise(pl.col("score")).alias("score"))
    r = check(a, b, threshold=0.5, margin=1e-6)
    assert r["missing"] == 0 and r["alert_flips"] == 0 and r["on_the_threshold"] == 1
    r = check(a, b.filter(pl.col("tx_id") != 4), threshold=0.5, margin=1e-6)
    assert r["missing"] == 1
    moved = b.with_columns(pl.when(pl.col("tx_id") == 3).then(0.2).otherwise(pl.col("score")).alias("score"))
    assert check(a, moved, threshold=0.5, margin=1e-6)["alert_flips"] == 1
