"""Write cover.json for a model exported before explanations existed.

    python -m detektive_ml.models.cover models/

Reads model.onnx and model.txt from the directory and writes cover.json next
to them, which is what `models.export` now does on its own. It takes seconds,
against minutes to train the model again.
"""

import argparse
from pathlib import Path

import lightgbm as lgb
import onnx

from detektive_ml.models.export import write_cover


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("model", type=Path, help="directory with model.onnx and model.txt")
    args = parser.parse_args()
    model = onnx.load(str(args.model / "model.onnx"))
    booster = lgb.Booster(model_file=str(args.model / "model.txt"))
    write_cover(model, booster, args.model)
    print(f"wrote {args.model / 'cover.json'}")


if __name__ == "__main__":
    main()
