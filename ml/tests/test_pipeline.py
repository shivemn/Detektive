"""End to end on the synthetic sample: prepare -> split -> train -> score."""

import json

from detektive_ml.baselines.raw_xgb import run
from detektive_ml.data import read_raw_csv
from detektive_ml.synthetic import write_sample


def test_floor_baseline_runs_end_to_end(tmp_path):
    csv = write_sample(tmp_path / "sample.csv", rows=20_000, positive_rate=0.02)
    parquet = tmp_path / "sample.parquet"
    read_raw_csv(csv).write_parquet(parquet)

    runs = run(parquet, seeds=(0, 1), out_dir=tmp_path / "results")

    assert set(runs) == {0, 1}
    # the planted signal is weak but real, so the model must beat chance
    assert all(r.pr_auc > 0.05 for r in runs.values())
    saved = list((tmp_path / "results").glob("raw_xgb__sample__*.json"))
    assert len(saved) == 1
    record = json.loads(saved[0].read_text())
    assert record["summary"]["runs"] == 2
    assert record["split"]["train"]["share"] > 0.55
