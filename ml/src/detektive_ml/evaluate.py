"""The evaluation protocol. Every model goes through `score`, nothing else.

Rules (see docs/evaluation-protocol.md for the reasoning):

1. Data is split by time with `split.time_split` (60/20/20).
2. The alert threshold is chosen on the validation set only, as the threshold
   that maximises minority-class F1 there. The test set never influences it.
3. We report test minority-class F1 at that threshold, plus PR-AUC (average
   precision), which does not depend on any threshold.
4. Every model is run with several seeds; we report mean and standard deviation.
5. If the data ends in a run of near-empty days (`split.sparse_tail_start`),
   test scores are also reported without those days, and that is the headline.
"""

import json
import platform
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from sklearn.metrics import average_precision_score, precision_recall_curve

DEFAULT_SEEDS = (0, 1, 2)


@dataclass(frozen=True)
class Scores:
    threshold: float
    f1: float
    precision: float
    recall: float
    pr_auc: float
    tp: int
    fp: int
    fn: int
    tn: int
    val_f1: float


def best_threshold(y_true: np.ndarray, y_score: np.ndarray) -> tuple[float, float]:
    """Threshold that maximises F1 for the positive class, and that F1."""
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    # the last precision/recall pair has no threshold attached
    p, r = precision[:-1], recall[:-1]
    with np.errstate(divide="ignore", invalid="ignore"):
        f1 = np.where(p + r > 0, 2 * p * r / (p + r), 0.0)
    i = int(np.argmax(f1))
    return float(thresholds[i]), float(f1[i])


def score(
    y_val: np.ndarray, p_val: np.ndarray, y_test: np.ndarray, p_test: np.ndarray
) -> Scores:
    y_val, y_test = np.asarray(y_val).astype(int), np.asarray(y_test).astype(int)
    if y_val.sum() == 0 or y_test.sum() == 0:
        raise ValueError("validation and test sets both need at least one positive")

    threshold, val_f1 = best_threshold(y_val, p_val)
    pred = np.asarray(p_test) >= threshold
    tp = int(np.sum(pred & (y_test == 1)))
    fp = int(np.sum(pred & (y_test == 0)))
    fn = int(np.sum(~pred & (y_test == 1)))
    tn = int(np.sum(~pred & (y_test == 0)))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return Scores(
        threshold=threshold,
        f1=f1,
        precision=precision,
        recall=recall,
        pr_auc=float(average_precision_score(y_test, p_test)),
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        val_f1=val_f1,
    )


def summarise(runs: list[Scores]) -> dict:
    """Mean and standard deviation across seeds, in percent for F1 and PR-AUC."""
    out = {"runs": len(runs)}
    for key in ("f1", "precision", "recall", "pr_auc"):
        values = np.array([getattr(r, key) for r in runs]) * 100
        out[key] = {"mean": round(float(values.mean()), 2), "std": round(float(values.std()), 2)}
    return out


def score_with_tail(
    y_val: np.ndarray, p_val: np.ndarray, y_test: np.ndarray, p_test: np.ndarray, tail: np.ndarray | None
) -> tuple[Scores, Scores | None]:
    """Scores on the full test set and, if a sparse tail exists, without it.
    The threshold is the same for both: it comes from validation."""
    full = score(y_val, p_val, y_test, p_test)
    if tail is None or not tail.any():
        return full, None
    keep = ~np.asarray(tail)
    return full, score(y_val, p_val, np.asarray(y_test)[keep], np.asarray(p_test)[keep])


def save(
    model: str,
    dataset: str,
    runs: dict[int, Scores],
    split_info: dict,
    out_dir: Path,
    runs_without_tail: dict[int, Scores] | None = None,
    extra: dict | None = None,
) -> Path:
    """Write one JSON file per experiment so results can be compared later."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{model}__{dataset}__{stamp}.json"
    record = {
        "model": model,
        "dataset": dataset,
        "created_utc": stamp,
        "python": platform.python_version(),
        "split": split_info,
        "summary": summarise(list(runs.values())),
        "seeds": {str(seed): asdict(s) for seed, s in runs.items()},
    }
    if runs_without_tail:
        record["summary_without_tail"] = summarise(list(runs_without_tail.values()))
        record["seeds_without_tail"] = {str(seed): asdict(s) for seed, s in runs_without_tail.items()}
    record.update(extra or {})
    path.write_text(json.dumps(record, indent=2))
    return path


def format_summary(summary: dict) -> str:
    def fmt(key: str) -> str:
        return f"{summary[key]['mean']:.2f} ± {summary[key]['std']:.2f}"

    return (
        f"F1 {fmt('f1')} | precision {fmt('precision')} | "
        f"recall {fmt('recall')} | PR-AUC {fmt('pr_auc')}  ({summary['runs']} seeds)"
    )
