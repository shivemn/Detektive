# Detektive

Real-time detection of money-laundering rings in a payment stream.

Detektive keeps a live graph of who paid whom, looks for the shapes laundering
leaves behind (money going round in a cycle, many new accounts paying into
one, funds split out and gathered back), and scores each payment as it
arrives. Every alert says which pattern it matched and why.

The engine will be written in Rust. Models are trained in Python and served
from Rust through ONNX.

> **Status: week 2 of about 24.** What exists today is the data pipeline, the
> evaluation protocol, a floor baseline and a Rust replayer that streams the
> data in time order. There is no graph engine yet.

## Data

Development uses IBM's [Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
datasets. They are **synthetic**, produced by an agent-based simulator, not
real bank data. See [data/README.md](data/README.md) for where to put the files.

## How results are measured

One protocol for every model: a 60/20/20 split by time, the alert threshold
chosen on validation only, minority-class F1 and PR-AUC on test, three seeds,
with and without the data's sparse tail.
The reasoning is in [docs/evaluation-protocol.md](docs/evaluation-protocol.md).

## Results

HI-Small, test set, mean ± std over 3 seeds, in percent.

| Model | F1 | PR-AUC | F1 incl. tail | PR-AUC incl. tail |
| --- | --- | --- | --- | --- |
| XGBoost on single-row features (floor) | 12.38 ± 0.49 | 5.16 ± 0.22 | 23.07 ± 0.33 | 11.53 ± 0.38 |

The main columns leave out the near-empty last days of the data, which are
mostly laundering and easy to catch; including them nearly doubles every
score. Why, and what else the data looks like:
[docs/data-notes-hi-small.md](docs/data-notes-hi-small.md).

## The stream

`crates/replay` replays the transactions in time order, at real time, 100× or
full speed. On HI-Small it loads and sorts 5,078,345 rows in 3.9 s and writes
about 2.7 million events per second, in exactly the order the Python loader
uses. Laundering labels are kept off the stream. Details and design notes:
[docs/replayer.md](docs/replayer.md).

## Running it

Python needs 3.10+; Rust needs a stable toolchain (`rustup`).

```bash
cd ml
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest

cd ..
python -m detektive_ml.prepare data/raw/HI-Small_Trans.csv data/processed/hi_small.parquet
python -m detektive_ml.explore data/processed/hi_small.parquet --patterns data/raw/HI-Small_Patterns.txt
python -m detektive_ml.baselines.raw_xgb data/processed/hi_small.parquet
```

```bash
cargo test
cargo run --release -p detektive-replay -- data/raw/HI-Small_Trans.csv --speed 100 --limit 20 --india
```

No dataset yet? `python -m detektive_ml.synthetic sample.csv` writes a small
fake file in the same format, good enough to check the pipeline runs.

## Layout

```
crates/    Rust: replay (the payment stream)
ml/        Python: data loading, time split, evaluation, baselines
docs/      design notes and the evaluation protocol
data/      datasets (not committed)
results/   one JSON file per experiment (not committed)
```

## Licence

MIT. The IBM datasets carry their own licence (CDLA-Sharing 1.0).
