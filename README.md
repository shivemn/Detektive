# Detektive

Real-time detection of money-laundering rings in a payment stream.

Detektive keeps a live graph of who paid whom, looks for the shapes laundering
leaves behind (money going round in a cycle, many new accounts paying into
one, funds split out and gathered back), and scores each payment as it
arrives. Every alert says which pattern it matched and why.

The engine will be written in Rust. Models are trained in Python and served
from Rust through ONNX.

> **Status: week 6 of about 24.** What exists today is the data pipeline, the
> evaluation protocol, a floor baseline, a Rust replayer that streams the data
> in time order, a consumer service reading that stream from Redpanda,
> Prometheus metrics with a Grafana dashboard, and the start of the graph
> engine: a time-windowed payment graph with per-partition watermarks and
> degree and fan features for every payment. No pattern detectors or models
> on graph features yet.

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

The replayer publishes to Redpanda, and `crates/consumer` reads it with
at-least-once delivery: offsets are stored only after a message is handled,
undecodable messages go to a dead-letter topic with the reason attached, and
repeats are skipped by transaction id. The end-to-end tests run the real
Kafka clients against librdkafka's mock cluster, so they need no Docker.
Delivery guarantees and how to run it: [docs/streaming.md](docs/streaming.md).

The consumer exports Prometheus metrics (throughput by outcome, lag per
partition, end-to-end latency, event-time lateness, handling time) and
`docker compose up` brings up Prometheus and a provisioned Grafana dashboard
with them. On a laptop it reads about 150,000 messages per second from
Redpanda. On a full replay of HI-Small, 95.4% of events arrive in event-time
order and none is more than 2 hours late; a consumer catching up on a backlog
sees days of disorder, which is why the graph engine keeps a watermark per
partition. What each metric means, and why event time and processing time are
kept apart: [docs/observability.md](docs/observability.md).

## The graph engine

`crates/graph` keeps a live graph of who paid whom over a sliding window of
event time (24 hours by default) and evicts what leaves it, so memory follows
the window, not the length of the stream: about 76 MB for the fullest
24-hour window of HI-Small (1.1 million payments). Eviction follows a
watermark kept per partition, so a partition that is behind never loses
payments that are still on their way. For every payment it reads nine
features off the window (degree, fan-in, fan-out and repeats, for both
accounts) in constant time, using counts kept up to date as payments enter
and leave; 1.2 million payments a second on one core. Every feature matches
an independent brute-force recount in Python. On HI-Small, 83% of laundering
payments are the sender's first payment to that receiver in 24 hours, against
39% of normal ones. Design, measurements and tests:
[docs/graph-engine.md](docs/graph-engine.md).

## Running it

Python needs 3.10+; Rust needs a stable toolchain (`rustup`). The Redpanda
parts need Docker and, on Windows, WSL (see [docs/streaming.md](docs/streaming.md)).

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
cargo test --workspace --all-features
cargo run --release -p detektive-replay -- data/raw/HI-Small_Trans.csv --speed 100 --limit 20 --india

docker compose up -d --build          # Redpanda, consumer, Prometheus, Grafana
docker compose run --rm replay --speed 3600
# dashboard: http://localhost:3000

# the graph window over the whole file, with each payment's features
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --features features.csv
bash scripts/check_features.sh       # features against a brute-force recount
```

No dataset yet? `python -m detektive_ml.synthetic sample.csv` writes a small
fake file in the same format, good enough to check the pipeline runs.

## Layout

```
crates/    Rust: replay (the payment stream), consumer (reads it from Redpanda),
           graph (the time-windowed graph and its features)
observability/  Prometheus config, Grafana provisioning and dashboard
ml/        Python: data loading, time split, evaluation, baselines
docs/      design notes and the evaluation protocol
data/      datasets (not committed)
results/   one JSON file per experiment (not committed)
```

## Licence

MIT. The IBM datasets carry their own licence (CDLA-Sharing 1.0).
