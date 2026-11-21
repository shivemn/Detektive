"""Convert a raw IBM CSV into a typed Parquet file.

    python -m detektive_ml.prepare data/raw/HI-Small_Trans.csv data/processed/hi_small.parquet
"""

import argparse
import time
from pathlib import Path

from detektive_ml.data import read_raw_csv


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("csv", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()

    started = time.perf_counter()
    df = read_raw_csv(args.csv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.out)
    took = time.perf_counter() - started

    print(f"{df.height:,} transactions -> {args.out} in {took:.1f}s")


if __name__ == "__main__":
    main()
