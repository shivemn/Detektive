# Detektive

Real-time detection of money-laundering rings in a payment stream.

Detektive keeps a live graph of who paid whom, looks for the shapes laundering
leaves behind (money going round in a cycle, many new accounts paying into
one, funds split out and gathered back), and scores each payment as it
arrives. Every alert says which pattern it matched and why.

The engine will be written in Rust. Models are trained in Python and served
from Rust through ONNX.

> **Status: week 12 of about 24.** What exists today is the data pipeline, the
> evaluation protocol, a floor baseline, a Rust replayer that streams the data
> in time order, a consumer service reading that stream from Redpanda,
> Prometheus metrics with a Grafana dashboard, and the graph engine's features
> (a time-windowed payment graph with per-partition watermarks; degree and fan
> features, temporal cycles and their shape, scatter-gather and gather-scatter
> for every payment). On top of those sit a boosted-tree model trained on the
> features and exported to ONNX, and a Rust scorer that runs it on every
> payment in the stream and publishes alerts. Each alert says why it fired.

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
| XGBoost on row + graph features | 41.80 ± 0.99 | 38.68 ± 0.82 | 57.38 ± 0.80 | 58.17 ± 0.83 |
| LightGBM on row + graph features | **42.79 ± 0.73** | **39.90 ± 0.53** | **58.61 ± 0.59** | **59.39 ± 0.50** |

The main columns leave out the near-empty last days of the data, which are
mostly laundering and easy to catch; including them nearly doubles every
score. Why, and what else the data looks like:
[docs/data-notes-hi-small.md](docs/data-notes-hi-small.md).

The graph features are computed in stream order, so each payment only sees the
payments before it, as a live system would. They account for about 27 of the
30 points between the floor and the graph models; the other 3 come from
better tree settings, which lift single-row features to about 15 on their own.
Published results on HI-Small reach about 63 F1 with tree models on graph
features computed over the whole test graph, future payments included. The
comparison, ablations and how the settings were chosen:
[docs/models.md](docs/models.md).

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
event time (four days by default) and evicts what leaves it, so memory follows
the window, not the length of the stream: about 183 MB for the fullest
four-day window of HI-Small (2.3 million payments). Eviction follows a
watermark kept per partition, so a partition that is behind never loses
payments that are still on their way.

For every payment it reads degree, fan-in, fan-out and repeats for both
accounts off the window in constant time, from counts kept up to date as
payments enter and leave; every value matches an independent brute-force
recount in Python. On HI-Small, 83% of laundering payments are the sender's
first payment to that receiver in 24 hours, against 39% of normal ones.

It also finds the temporal cycles each payment closes (money coming back to
where it started, each payment no earlier than the one before), up to 12
payments long. It caught 49 of the 54 laundering cycles listed with HI-Small,
every one whose payments are in time order, as each closed. Payments that
close a ring of three or more are laundering 61.5% of the time, against a
base rate of 0.10%.

And it measures the widest scatter-gather each payment is part of (one
account paying several that each pay the same final account), from a
neighbour-set intersection. The last payment of 43 of the 44 scatter-gather
attempts in HI-Small sees the attempt's full width; payments in a
scatter-gather 5 accounts wide are laundering 78.8% of the time. The whole
engine runs at about 270,000 payments a second on one core. A batch mode
searches for cycles on several threads and gives exactly the same features,
byte for byte on all of HI-Small. Every structural feature also matches
NetworkX exactly. Design, measurements and tests:
[docs/graph-engine.md](docs/graph-engine.md).

## Scoring

The model is exported from Python to ONNX and run by `crates/score`, which
walks the trees itself:

- **It matches LightGBM.** On all 5,078,345 HI-Small payments its scores agree
  with LightGBM's to within 6 × 10⁻⁸, with no alert decision different.
  Getting there meant fixing how the export rounds split thresholds.
- **It is fast.** About 14 µs per payment for the model (p99 34 µs), and 21 µs
  with the graph (p99 63 µs), on one core.

The consumer scores every payment and writes alerts to an `alerts` topic. Each
alert is confirmed before its payment's offset is stored.

At a review budget of 100 alerts a day, 78% of the model's alerts are
laundering, and it catches 20.5% of the test period's laundering. Hand-written
rules on the same graph patterns manage 22% and 5.8%.

38% of HI-Small's laundering belongs to no pattern IBM lists, and the model
catches almost none of it. That caps recall at about 60% for features like
these. Details, timings and the validation-to-test gap:
[docs/scoring.md](docs/scoring.md).

## Why an alert fired

Each alert carries the five features that moved its score most, in words, and
how far each one pushed it:

```
  +4.58  paid by ACH
  +2.35  the sender's first payment to this receiver in the last 4 days
  +1.48  the sender's money reached the receiver through 13 different middle accounts
```

These come from TreeSHAP, computed in Rust. On 23,408 HI-Small payments they
match LightGBM's own `pred_contrib` to within 2 × 10⁻⁷, with the same top
reason on every one. Explaining takes about 5 ms a payment, so only alerts are
explained.

Summed over the test alerts, the explanations show what the model really
learned. ACH is the largest reason for every alert, and a first payment to a
new payee is usually the second; on HI-Small, ACH carries 48 times the
laundering rate of the other methods. The rings and scatter-gathers are among
the top five reasons for only 1 alert in 5, but those alerts are laundering
86% of the time, against 64% for the rest. What that means for the
project: [docs/explanations.md](docs/explanations.md).

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

# boosted trees on row + graph features (about 6 GB of memory)
cd ml && python -m detektive_ml.models.graph_boost ../data/processed/hi_small.parquet ../features.csv

# the model the stream runs: export it, then score the whole file in Rust
python -m detektive_ml.models.export ../data/processed/hi_small.parquet ../features.csv ../models --scores
cd .. && cargo run --release -p detektive-score -- data/raw/HI-Small_Trans.csv --model models
cargo run --release -p detektive-score -- data/raw/HI-Small_Trans.csv --model models --explain explain.jsonl
DETEKTIVE_MODEL=/models docker compose up -d   # the consumer scores and alerts
```

No dataset yet? `python -m detektive_ml.synthetic sample.csv` writes a small
fake file in the same format, good enough to check the pipeline runs.

## Layout

```
crates/    Rust: replay (the payment stream), consumer (reads it from Redpanda),
           graph (the time-windowed graph and its features), score (runs
           the exported model)
observability/  Prometheus config, Grafana provisioning and dashboard
ml/        Python: data loading, time split, evaluation, baselines, models
docs/      design notes and the evaluation protocol
data/      datasets (not committed)
models/    trained models (not committed)
results/   one JSON file per experiment (not committed)
```

## Licence

MIT. The IBM datasets carry their own licence (CDLA-Sharing 1.0).
