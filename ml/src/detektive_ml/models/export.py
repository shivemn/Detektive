"""Train the model the stream will run, and export it to ONNX.

    python -m detektive_ml.models.export data/processed/hi_small.parquet graph.csv models/

Writes these files:

- `model.onnx`: the LightGBM trees as an ONNX TreeEnsembleClassifier.
- `cover.json`: how many training rows reached each tree node, which the
  scorer needs to explain a score (TreeSHAP) and ONNX cannot hold.
- `model.txt`: the same trees in LightGBM's own format, for reference.
- `model.json`: everything the scorer needs besides the trees. That is the
  feature order, the category codes, the alert threshold chosen on validation,
  and the scores this model got, so a deployed model can be traced back to its
  evaluation.
- `scores.csv` (with `--scores`): tx_id and LightGBM's score for every
  payment, so the Rust scorer can be checked against it row by row.

Before writing, the export is checked against LightGBM on the validation and
test sets with onnxruntime. If any score moves by more than `TOLERANCE`, or any
alert decision flips, the export fails. onnxruntime adds the trees up in 32-bit
floats, so on a model of several hundred trees its scores drift from
LightGBM's in the fifth decimal place. The Rust scorer adds in 64 bits and
lands much closer (see docs/scoring.md).
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from detektive_ml.data import load
from detektive_ml.evaluate import best_threshold, score_with_tail
from detektive_ml.models.graph_boost import build, join_graph, train_lgbm

TOLERANCE = 1e-4


def to_onnx(booster, n_features: int):
    import onnxmltools
    from onnxmltools.convert.common.data_types import FloatTensorType

    return onnxmltools.convert_lightgbm(
        booster,
        initial_types=[("features", FloatTensorType([None, n_features]))],
        zipmap=False,
        target_opset=15,
    )


def _ensemble(model):
    node = next(n for n in model.graph.node if n.op_type == "TreeEnsembleClassifier")
    return {a.name: a for a in node.attribute}


def match_nodes(model, booster) -> list[tuple[int, dict]]:
    """Pair every ONNX node with the LightGBM node it came from.

    The ONNX trees are walked side by side with LightGBM's own dump, so each
    ONNX node (by its position k in the ONNX node lists) is matched to the
    split or leaf it came from, not guessed by position. Raises if the two
    disagree anywhere.
    """
    attr = _ensemble(model)
    trees = list(attr["nodes_treeids"].ints)
    ids = list(attr["nodes_nodeids"].ints)
    features = list(attr["nodes_featureids"].ints)
    modes = [m.decode() for m in attr["nodes_modes"].strings]
    yes, no = list(attr["nodes_truenodeids"].ints), list(attr["nodes_falsenodeids"].ints)
    at = {(t, i): k for k, (t, i) in enumerate(zip(trees, ids))}
    children = {(trees[k], c) for k in range(len(trees)) if modes[k] != "LEAF" for c in (yes[k], no[k])}
    roots = {trees[k]: k for k in range(len(trees)) if (trees[k], ids[k]) not in children}

    pairs = []
    for tree, info in enumerate(booster.dump_model()["tree_info"]):
        stack = [(roots[tree], info["tree_structure"])]
        while stack:
            k, node = stack.pop()
            pairs.append((k, node))
            if "leaf_value" in node:
                if modes[k] != "LEAF":
                    raise ValueError(f"tree {tree}: ONNX has a split where LightGBM has a leaf")
                continue
            if modes[k] != "BRANCH_LEQ" or node["decision_type"] != "<=" or features[k] != node["split_feature"]:
                raise ValueError(f"tree {tree}: ONNX node {ids[k]} does not match LightGBM's split")
            stack.append((at[(tree, yes[k])], node["left_child"]))
            stack.append((at[(tree, no[k])], node["right_child"]))
    if len(pairs) != len(trees):
        raise ValueError(f"matched {len(pairs)} nodes, the ONNX model has {len(trees)}")
    return pairs


def round_thresholds_down(model, booster) -> int:
    """Make the ONNX splits decide exactly as LightGBM's do.

    LightGBM keeps split thresholds as doubles and compares the f32 feature,
    widened to f64, with them. ONNX stores thresholds as f32, and onnxmltools
    rounds each one to the nearest f32. When that rounds up, a feature value
    equal to the rounded threshold goes left in ONNX but right in LightGBM. On
    HI-Small that changed the path of 29 payments, all with amounts like 351.00
    whose log lands exactly on a split. For an f32 feature x and a double
    threshold t, `x <= t` holds exactly when `x <= t'`, where t' is t rounded
    down to f32. So each threshold is replaced with that.

    Returns how many thresholds changed.
    """
    attr = _ensemble(model)
    values = np.array(attr["nodes_values"].floats, dtype=np.float32)
    changed = 0
    for k, node in match_nodes(model, booster):
        if "leaf_value" in node:
            continue
        t = float(node["threshold"])
        down = np.float32(t)
        if float(down) > t:
            down = np.nextafter(down, np.float32(-np.inf))
        if down != values[k]:
            changed += 1
            values[k] = down
    del attr["nodes_values"].floats[:]
    attr["nodes_values"].floats.extend(values.tolist())
    return changed


def node_cover(model, booster) -> list[int]:
    """How many training rows reached each ONNX node, in ONNX node order.

    ONNX has nowhere to keep this, but TreeSHAP needs it: a feature's share of
    a score depends on how the training data split at each node. These are
    LightGBM's own counts (`internal_count`, `leaf_count`), the ones its
    `pred_contrib` uses.
    """
    cover = [0] * len(_ensemble(model)["nodes_treeids"].ints)
    for k, node in match_nodes(model, booster):
        cover[k] = int(node["leaf_count"] if "leaf_value" in node else node["internal_count"])
    return cover


def write_cover(model, booster, out: Path) -> None:
    cover = node_cover(model, booster)
    (out / "cover.json").write_text(json.dumps({"nodes": len(cover), "cover": cover}, separators=(",", ":")))


def onnx_scores(model, X: np.ndarray, batch: int = 200_000) -> np.ndarray:
    import onnxruntime as ort

    options = ort.SessionOptions()
    options.log_severity_level = 3  # it warns about the label output's declared shape on every batch
    session = ort.InferenceSession(model.SerializeToString(), options, providers=["CPUExecutionProvider"])
    name = session.get_inputs()[0].name
    out = [session.run(None, {name: X[i : i + batch]})[1][:, 1] for i in range(0, len(X), batch)]
    return np.concatenate(out)


def compare(reference: np.ndarray, exported: np.ndarray, threshold: float, margin: float = 1e-6) -> dict:
    """Largest score difference, and alert decisions that differ. The threshold
    is itself one validation payment's score, so a payment within `margin` of
    it may land either side; it is counted apart, not as a flip."""
    diff = np.abs(reference.astype(np.float64) - exported.astype(np.float64))
    near = np.abs(reference - threshold) <= margin
    flipped = (reference >= threshold) != (exported >= threshold)
    return {"rows": len(reference), "max_abs_diff": float(diff.max()),
            "alert_flips": int(np.sum(flipped & ~near)), "on_the_threshold": int(np.sum(near))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data", type=Path, help="prepared .parquet (or raw .csv)")
    parser.add_argument("graph", type=Path, help="features file written by detektive-graph --features")
    parser.add_argument("out", type=Path, help="directory for model.onnx and model.json")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--scores", action="store_true", help="also write scores.csv for every payment")
    args = parser.parse_args()

    df, graph_names = join_graph(load(args.data), args.graph)
    tx_ids = df["tx_id"].to_numpy()  # join_graph returns rows sorted as build() sorts them
    m = build(df, graph_names, "both")
    del df

    booster = train_lgbm(m, args.seed)
    best = booster.best_iteration
    p_val = booster.predict(m.X[m.val], num_iteration=best)
    p_test = booster.predict(m.X[m.test], num_iteration=best)
    threshold, val_f1 = best_threshold(m.y[m.val], p_val)
    full, trimmed = score_with_tail(m.y[m.val], p_val, m.y[m.test], p_test, m.tail)
    print(f"trees {best} | threshold {threshold:.4f} | val F1 {val_f1 * 100:.2f} | "
          f"test F1 {trimmed.f1 * 100:.2f} (incl. tail {full.f1 * 100:.2f})", flush=True)

    # the exported model keeps only the trees early stopping chose
    import lightgbm as lgb

    trimmed_booster = lgb.Booster(model_str=booster.model_to_string(num_iteration=best))
    model = to_onnx(trimmed_booster, len(m.names))
    changed = round_thresholds_down(model, trimmed_booster)
    print(f"{changed:,} split thresholds rounded down to match LightGBM exactly", flush=True)
    checks = {
        "val": compare(p_val, onnx_scores(model, m.X[m.val]), threshold),
        "test": compare(p_test, onnx_scores(model, m.X[m.test]), threshold),
    }
    print("onnxruntime against LightGBM:", checks, flush=True)
    for part, c in checks.items():
        if c["max_abs_diff"] > TOLERANCE or c["alert_flips"]:
            raise SystemExit(f"export does not match LightGBM on {part}: {c}")

    import lightgbm

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "model.onnx").write_bytes(model.SerializeToString())
    write_cover(model, trimmed_booster, args.out)
    trimmed_booster.save_model(str(args.out / "model.txt"))
    card = {
        "model": "lightgbm",
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lightgbm": lightgbm.__version__,
        "dataset": args.data.stem,
        "graph_features": args.graph.name,
        "seed": args.seed,
        "trees": best,
        "features": m.names,
        "categories": m.categories,
        "threshold": threshold,
        "validation_f1": val_f1,
        "test": {"f1": trimmed.f1, "precision": trimmed.precision, "recall": trimmed.recall,
                 "pr_auc": trimmed.pr_auc, "f1_incl_tail": full.f1},
        "onnx_check": checks,
        "split": m.info,
    }
    (args.out / "model.json").write_text(json.dumps(card, indent=2))
    print(f"wrote {args.out / 'model.onnx'} and {args.out / 'model.json'}")

    if args.scores:
        scores = booster.predict(m.X, num_iteration=best)
        pl.DataFrame({"tx_id": tx_ids, "score": scores}).sort("tx_id").write_csv(args.out / "scores.csv")
        print(f"wrote {args.out / 'scores.csv'}")


if __name__ == "__main__":
    main()
