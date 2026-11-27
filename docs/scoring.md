# Scoring the stream

Week 11 takes the week 10 model out of Python and into the stream. The
consumer now scores every payment as it arrives and raises an alert when the
score reaches the model's threshold.

```
payment --> graph window --> 19 graph features --+
        \--------------------> 11 row features --+--> trees --> score --> alert?
```

All numbers here are from the cloud VM (2 cores), on IBM's HI-Small. That
data is synthetic.

## From LightGBM to the stream

```bash
# features for every payment, then the model trained on them
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --batch 4096 --features graph.csv
cd ml && python -m detektive_ml.models.export ../data/processed/hi_small.parquet ../graph.csv ../models --scores

# the same file through the Rust scorer, checked row by row against LightGBM
cargo run --release -p detektive-score -- data/raw/HI-Small_Trans.csv --model models --scores rust_scores.csv
python -m detektive_ml.models.check_scores models/scores.csv rust_scores.csv --threshold 0.1611
```

`models.export` trains LightGBM with the settings from week 10 (seed 0) and
picks the alert threshold on validation. It writes these files:

- `model.onnx`: the trees as an ONNX TreeEnsembleClassifier.
- `model.json`: the feature order, the category codes, the threshold, and the
  scores the model got.
- `model.txt`: the same trees in LightGBM's own format.

The exported model has 774 trees and 47,214 nodes. Its threshold is 0.1611.
On the test set it gets F1 42.93 (58.99 including the tail), so it is one of
the three seeds from week 10.

`crates/score` reads the ONNX file and walks the trees itself. The consumer
loads the model at start-up and refuses it in three cases:

- the model was trained on other features;
- it was trained on the same features in another order;
- `model.json` and `model.onnx` disagree on the number of trees.

### Two things the export got wrong

**Split thresholds that rounded up.** LightGBM stores each split threshold as
a 64-bit double. ONNX stores it as a 32-bit float, and `onnxmltools` rounds
each double to the *nearest* float. When that rounds up, a feature value
exactly equal to the rounded threshold goes left in ONNX and right in
LightGBM. The first full comparison caught this on 29 payments, all with
amounts like 351.00, 285.55 or 927.04, whose logarithm lands exactly on a
split. Their scores moved by up to 0.0013.

The fix uses one fact about rounding. For a 32-bit feature x and a double
threshold t, `x <= t` holds exactly when `x <= t'`, where t' is t rounded
*down* to 32 bits. The export therefore walks every ONNX tree alongside
LightGBM's own dump, matching each node to the split it came from rather than
guessing by position, and rounds each threshold down. On this model that
changed 2,133 of about 23,000 thresholds.

**Missing values.** LightGBM sends a missing value in a feature that had none
in training to the same side as 0. The ONNX file instead sends it down each
split's default branch. The scorer therefore never passes a missing value. A
payment whose bank or currency the event does not state gets 0 for
`same_bank` and `cross_currency`, which is what LightGBM would do with NaN.

### Parity

Every one of HI-Small's 5,078,345 payments was run through the graph engine and
the Rust scorer, and the result compared with LightGBM's own `predict`:

| | Largest difference | Mean difference | Alert decisions that differ |
| --- | --- | --- | --- |
| Rust scorer | 5.8 × 10⁻⁸ | 5.9 × 10⁻¹¹ | 0 |
| onnxruntime (val + test only) | 8.1 × 10⁻⁵ | | 0 |

What remains for Rust is the file format: ONNX stores each leaf's value as a
32-bit float, about seven significant digits. A payment that took a different
path through any tree would be off by far more than this.

One payment sits exactly on the threshold, because the threshold is itself
one validation payment's score. It is the only one that could land on either
side, and the checks say so instead of counting it as a disagreement.
onnxruntime adds the trees up in 32-bit floats, so it drifts further from
LightGBM. The Rust scorer adds in 64 bits, as LightGBM does.

`crates/score/tests/parity.rs` repeats this check in CI on a small model,
trained by `scripts/make_score_fixture.py` on a synthetic file. The consumer's
end-to-end test then runs that file through a broker and checks two things:

- the `alerts` topic holds exactly the payments LightGBM scores above the
  threshold;
- each alert is confirmed before its payment's offset is stored.

## How fast

All times are per payment, for the model alone, on the HI-Small model:

| Way of running the trees | Mean | p99 |
| --- | --- | --- |
| tract (pure-Rust ONNX runtime), on a similar 700-tree model | 30 ms | |
| LightGBM `predict`, one payment per call | 115 µs | |
| onnxruntime, one payment per call (from Python, one thread) | 22 µs | |
| This crate: first version, one tree at a time | 44.6 µs | 92.8 µs |
| … nodes packed into 16 bytes, depth first | 37.3 µs | 69.8 µs |
| … leaves kept apart from splits | 28.1 µs | 60.1 µs |
| … eight trees walked at once | **14.4 µs** | **34.2 µs** |

tract was the first choice, because it is pure Rust. It is built for neural
networks, though, and its tree operator is thousands of times slower than
walking the trees directly. A tree ensemble is simple enough that this crate
reads the ONNX file with a few protobuf message definitions and walks the
trees itself.

Each step down a tree waits for the node load before it, so walking one tree
at a time leaves the core mostly idle. Walking eight trees together keeps
eight loads in flight. Sixteen was slower again (23 µs), presumably because
the bookkeeping for that many walks no longer fits in registers.

The graph and the model together, one payment at a time, as the consumer runs
them:

| | Mean | p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- |
| Graph and model | 21.1 µs | 18.2 µs | 62.7 µs | 282 µs |

That is about 47,000 payments a second on one core, and a p99 far below the
10 ms target. The slowest payment took 33 ms, which is most likely the VM
pausing rather than the code; the p99.9 is under 0.3 ms. Inside the consumer,
`detektive_consumer_handle_seconds` adds JSON decoding on top of this. It has
not been measured through Redpanda on a laptop yet (see below).

## How good the alerts are

F1 at one threshold is not how an alert queue is run. A team reviews a fixed
number of alerts a day, highest first. `models.alert_budget` therefore gives
each detector a daily budget K: every detector raises its K most suspicious
payments of each test day. It then counts how many of those are laundering
(precision) and how much of the period's laundering they catch (recall).

The test period without the sparse tail is three days: 1,014,774 payments, of
which 1,143 are laundering.

```bash
python -m detektive_ml.models.alert_budget ../data/processed/hi_small.parquet ../graph.csv \
    ../models/scores.csv --patterns ../data/raw/HI-Small_Patterns.txt --raw ../data/raw/HI-Small_Trans.csv
```

| Alerts a day | Model precision | Model recall | Rules precision | Rules recall |
| --- | --- | --- | --- | --- |
| 10 | 100% | 2.6% | 43% | 1.1% |
| 25 | 96% | 6.3% | 49% | 3.2% |
| 50 | 91% | 11.9% | 37% | 4.9% |
| 100 | 78% | 20.5% | 22% | 5.8% |
| 250 | 55% | 35.8% | 9% | 5.9% |
| 500 | 35% | 45.3% | 5% | 6.5% |
| 1,000 | 20% | 53.2% | 3% | 7.1% |

At its own threshold the model raises about 175 alerts a day. Two in three of
them are laundering, and together they catch 31% of it.

**The rules.** These are rules a bank might write by hand from the patterns
the engine finds. Each scores points when it fires, and payments are ranked by
their total (ties at random, with a fixed seed). They were fixed from what weeks 7 and 8
measured, before any of this was run. Those measurements covered the whole
file, test days included, so if anything the rules are favoured. On the test
period, each rule on its own does this:

| Rule | Points | Fires | Of which laundering | Recall |
| --- | --- | --- | --- | --- |
| Closes a ring of 3+ payments | 4 | 23 | 21 (91%) | 1.8% |
| In a scatter-gather 3+ wide | 3 | 97 | 48 (49%) | 4.2% |
| Receiver paid by 10+ accounts | 2 | 11,009 | 65 (0.6%) | 5.7% |
| First payment to this payee, across currencies | 1 | 1,932 | 0 | 0% |

The rings and scatter-gathers are precise but rare. The fan-in rule fires on
thousands of ordinary payments. Together, at up to 1,000 alerts a day, the
rules reach 7% of the laundering. At 100 alerts a day the model finds 3.5
times as much as the rules, at 3.5 times the precision. A model on the same features, then,
does what no hand-set threshold can.

## Why test scores are below validation

At the threshold, the seed-0 model's F1 is 55.6 on validation and 42.9 on
test. Splitting each period's laundering by the pattern it belongs to, from
IBM's patterns file, shows two things:

| Laundering that is... | Validation share | Validation recall | Test share | Test recall |
| --- | --- | --- | --- | --- |
| not part of any listed pattern | 36% | 0.0% | 40% | 0.7% |
| SCATTER-GATHER | 14% | 86% | 11% | 65% |
| GATHER-SCATTER | 9% | 74% | 13% | 65% |
| STACK | 15% | 58% | 9% | 33% |
| CYCLE | 6% | 58% | 6% | 49% |
| FAN-OUT | 6% | 79% | 7% | 69% |
| FAN-IN | 6% | 52% | 6% | 34% |
| BIPARTITE | 5% | 60% | 5% | 37% |
| RANDOM | 4% | 73% | 4% | 42% |

**38% of HI-Small's laundering belongs to no listed pattern, and the model
catches almost none of it.** That is 1,968 of 5,177 laundering payments.
Almost none of them touch an account from a listed attempt (42 do), and they
use every payment format, while listed laundering is nearly all ACH (3,208 of
3,209). They look like laundering accounts' one-off payments, which no graph
shape points to. That caps recall at about 60% for anything built on these
features, until something models accounts' own histories.

**Recall also falls on every listed pattern, not just one.** So the mix
changing between periods is not the main cause. One likely factor is that
validation covers weekdays only (6 to 8 September), while test runs into
Saturday the 10th. HI-Small's weekend days have about twice the laundering
rate of weekdays, with 60% fewer payments. Taking hour and weekday out of the
model did not help: test F1 fell to 34.4 (one seed). So the calendar features
are useful, and the gap is not them overfitting. Another candidate is
attempts that run into the excluded tail, whose earlier payments carry weaker
structure; it is not tested yet. The practical consequence is that the
threshold chosen on validation is too strict for test. A live system would
recalibrate it on recent data, and an alert budget avoids the problem
altogether.

## Running it in the stack

```bash
# put model.onnx and model.json in ./models (the export above), then
DETEKTIVE_MODEL=/models docker compose up -d --build
docker compose run --rm replay --speed 3600
docker compose exec redpanda rpk topic consume alerts -n 3
```

The dashboard has a row for the model: payments scored, alerts and alert
rate, the threshold, time to score, and alerts per minute. Without
`DETEKTIVE_MODEL` the consumer builds the graph and scores nothing. The smoke
test in CI runs the stack with the small test model and checks that every
payment was scored.

An alert carries the payment, its score, the threshold and the graph facts
behind it: pair count, fan-in and fan-out, cycles and the ring, ring repeats,
and scatter-gather width. These are the features as computed, not an
explanation of the score. Explaining which features drove a score is week 12.

## Known gaps

- **Latency through Redpanda.** The p99 has not been measured inside the
  consumer with Redpanda, on a laptop. The numbers above are the graph and
  the model without the broker or JSON.
- **The threshold is fixed.** It comes from validation and never moves; the
  gap above says it should be recalibrated.
- **One model, loaded at start-up.** Swapping it means a restart. Model
  versions, shadow scoring and rollback come with the deployment work in
  weeks 23–24.
