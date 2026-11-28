"""Rebuild the small model the Rust scorer's tests run against.

    python scripts/make_score_fixture.py

Writes into crates/score/tests/fixtures/:

- sample.csv: a synthetic transactions file (fake data, same format as IBM's);
- model.onnx and model.json: a LightGBM model trained on it through the same
  feature pipeline as the real model, kept small (60 trees) so the files stay
  small;
- scores.csv: LightGBM's own score for every payment;
- cover.json: training rows through each tree node, for explanations;
- contributions.csv: LightGBM's own explanation (pred_contrib) of every
  tenth payment: each feature's share of the raw score, then the expected
  value.

The Rust test reads sample.csv, computes the features and scores itself, and
must land on scores.csv. Rerun this only when the features or the export
change, and commit the result.
"""

import json
import subprocess
import tempfile
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl

from detektive_ml.data import read_raw_csv
from detektive_ml.evaluate import best_threshold
from detektive_ml.models.export import compare, onnx_scores, round_thresholds_down, to_onnx, write_cover
from detektive_ml.models.graph_boost import build, join_graph
from detektive_ml.synthetic import write_sample

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "crates/score/tests/fixtures"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sample = write_sample(OUT / "sample.csv", rows=4000, seed=3, positive_rate=0.1)
    with tempfile.TemporaryDirectory() as tmp:
        graph = Path(tmp) / "graph.csv"
        subprocess.run(
            ["cargo", "run", "--quiet", "--release", "-p", "detektive-graph", "--",
             str(sample), "--features", str(graph)],
            cwd=ROOT, check=True,
        )
        df, names = join_graph(read_raw_csv(sample), graph)
    tx_ids = df["tx_id"].to_numpy()
    m = build(df, names, "both")

    params = {"objective": "binary", "num_leaves": 15, "learning_rate": 0.1,
              "min_child_samples": 20, "seed": 0, "verbose": -1}
    booster = lgb.train(params, lgb.Dataset(m.X[m.train], m.y[m.train], feature_name=m.names), 60)
    threshold, _ = best_threshold(m.y[m.val], booster.predict(m.X[m.val]))

    model = to_onnx(booster, len(m.names))
    round_thresholds_down(model, booster)
    scores = booster.predict(m.X)
    check = compare(scores, onnx_scores(model, m.X), threshold)
    print("onnxruntime against LightGBM:", check)

    (OUT / "model.onnx").write_bytes(model.SerializeToString())
    write_cover(model, booster, OUT)
    card = {"model": "lightgbm", "note": "test fixture trained on synthetic data",
            "trees": booster.num_trees(), "features": m.names, "categories": m.categories,
            "threshold": threshold}
    (OUT / "model.json").write_text(json.dumps(card, indent=2) + "\n")
    order = np.argsort(tx_ids, kind="stable")
    pl.DataFrame({"tx_id": tx_ids[order], "score": scores[order]}).write_csv(OUT / "scores.csv")
    every_tenth = order[::10]
    contrib = booster.predict(m.X[every_tenth], pred_contrib=True)
    columns = {"tx_id": tx_ids[every_tenth]}
    columns.update({name: contrib[:, j] for j, name in enumerate(m.names)})
    columns["expected"] = contrib[:, -1]
    pl.DataFrame(columns).write_csv(OUT / "contributions.csv")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
