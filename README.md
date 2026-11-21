# Detektive

Real-time detection of money-laundering rings in a payment stream.

Detektive keeps a live graph of who paid whom, looks for the shapes laundering
leaves behind (money going round in a cycle, many new accounts paying into
one, funds split out and gathered back), and scores each payment as it
arrives. Every alert says which pattern it matched and why.

The engine will be written in Rust. Models are trained in Python and served
from Rust through ONNX.

> **Status: week 1 of about 24.** What exists today is the data pipeline, the
> evaluation protocol and a floor baseline. There is no graph engine yet.

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

## Running it

Needs Python 3.10+.

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

No dataset yet? `python -m detektive_ml.synthetic sample.csv` writes a small
fake file in the same format, good enough to check the pipeline runs.

## Layout

```
ml/        Python: data loading, time split, evaluation, baselines
docs/      design notes and the evaluation protocol
data/      datasets (not committed)
results/   one JSON file per experiment (not committed)
```

## Licence

MIT. The IBM datasets carry their own licence (CDLA-Sharing 1.0).
