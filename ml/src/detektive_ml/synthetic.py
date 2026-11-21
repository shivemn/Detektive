"""A tiny fake dataset in the exact IBM CSV layout, for tests and CI.

It is NOT a stand-in for the real data. It only has a weak planted signal
(laundering prefers ACH payments and round-ish large amounts) so that a model
can learn something and the pipeline can be checked end to end.

    python -m detektive_ml.synthetic tests_sample.csv --rows 50000
"""

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

HEADER = [
    "Timestamp", "From Bank", "Account", "To Bank", "Account",
    "Amount Received", "Receiving Currency", "Amount Paid", "Payment Currency",
    "Payment Format", "Is Laundering",
]
FORMATS = ["Cheque", "Credit Card", "ACH", "Cash", "Wire", "Reinvestment", "Bitcoin"]
CURRENCIES = ["US Dollar", "Euro", "Rupee", "Yuan", "UK Pound"]


def write_sample(path: Path, rows: int = 20_000, seed: int = 7, positive_rate: float = 0.01) -> Path:
    rng = np.random.default_rng(seed)
    start = datetime(2022, 9, 1)
    minutes = np.sort(rng.integers(0, 10 * 24 * 60, size=rows))
    n_accounts = max(rows // 10, 50)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for m in minutes:
            laundering = rng.random() < positive_rate
            fmt = "ACH" if laundering and rng.random() < 0.7 else str(rng.choice(FORMATS))
            amount = float(rng.lognormal(8.5 if laundering else 6.0, 1.0))
            if laundering and rng.random() < 0.5:
                amount = round(amount, -3) or 1000.0
            currency = str(rng.choice(CURRENCIES))
            src_bank, dst_bank = rng.integers(1, 30, size=2)
            src, dst = rng.integers(0, n_accounts, size=2)
            writer.writerow([
                (start + timedelta(minutes=int(m))).strftime("%Y/%m/%d %H:%M"),
                f"{src_bank:03d}", f"{src:09X}", f"{dst_bank:03d}", f"{dst:09X}",
                f"{amount:.2f}", currency, f"{amount:.2f}", currency,
                fmt, int(laundering),
            ])
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    print(write_sample(args.out, args.rows, args.seed))


if __name__ == "__main__":
    main()
