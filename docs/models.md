# Models on graph features

Week 10 puts the graph engine's 19 features next to the 11 single-row
features and trains gradient-boosted trees on them, under the same protocol
as the floor ([evaluation-protocol.md](evaluation-protocol.md)): a 60/20/20
split by time, the threshold chosen on validation, three seeds, scored with
and without the sparse tail.

```bash
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --batch 4096 --features graph.csv
python -m detektive_ml.models.graph_boost data/processed/hi_small.parquet graph.csv --features both row graph
```

The features for all 5,078,345 payments take about 30 seconds to compute.
Each payment's features come from the payments before it in the stream, inside
the four-day window, exactly as the live consumer sees them. Nothing is computed
from the future, and labels never reach the engine.

## Results

HI-Small test set, mean ± std over seeds 0, 1 and 2, in percent.

| Features | Model | F1 | PR-AUC | F1 incl. tail | PR-AUC incl. tail |
| --- | --- | --- | --- | --- | --- |
| Single row (the floor, class-weighted) | XGBoost | 12.38 ± 0.49 | 5.16 ± 0.22 | 23.07 ± 0.33 | 11.53 ± 0.38 |
| Single row | XGBoost | 15.30 ± 0.36 | 7.32 ± 0.10 | 27.64 ± 0.45 | 17.35 ± 0.20 |
| Single row | LightGBM | 14.92 ± 0.24 | 7.35 ± 0.05 | 26.84 ± 0.59 | 17.62 ± 0.24 |
| Graph only | XGBoost | 7.40 ± 0.24 | 11.14 ± 0.19 | 12.13 ± 0.33 | 15.76 ± 0.28 |
| Graph only | LightGBM | 6.97 ± 0.16 | 10.94 ± 0.13 | 11.35 ± 0.25 | 15.41 ± 0.12 |
| Row + graph | XGBoost | 41.80 ± 0.99 | 38.68 ± 0.82 | 57.38 ± 0.80 | 58.17 ± 0.83 |
| **Row + graph** | **LightGBM** | **42.79 ± 0.73** | **39.90 ± 0.53** | **58.61 ± 0.59** | **59.39 ± 0.50** |

At its threshold, the LightGBM model on row + graph features flags payments
that are laundering about two times in three (precision 67%). It finds about
a third of all laundering (recall 32%).

**Where the gain comes from.** With the settings below, single-row features
alone go from 12.4 to about 15 F1, so tuning is worth about 3 points. Adding
the graph features takes the same models to 42–43, which is the other 27.
PR-AUC goes from 7 to 40.

**Neither half works alone.** The graph features by themselves give F1 7 and
PR-AUC 11. That is a better ranking than single-row features (PR-AUC 7), but
it yields a worse F1, because the threshold carries over badly from validation.
Put together, the two halves reach 42. A structural signal means little until
the model also knows the payment's format, currency and amount. For example,
a cycle closed by a cross-currency ACH payment is a different thing from one
closed by a cheque.

**Which features the trees lean on.** Ranked by total gain, the top features
for LightGBM are:

- payment format;
- how often this sender has paid this receiver in the window (`pair_count`);
- the sender's fan-out and in-degree;
- the weekday and the amount;
- the receiver's fan-in and cycle count.

XGBoost ranks payment format first, then:

- cross-currency;
- `pair_count` and `ring_repeats`;
- self-transfer and same bank;
- the cycle counts.

**Validation and test disagree.** At the chosen threshold, validation F1 is
55–57, while test F1 without the tail is 42–44. The thresholds themselves are
stable across seeds (0.13–0.19), so this looks like the data changing between
the two periods rather than noise. The tail cut cannot explain it either,
because the validation set has no tail. This is worth understanding before the
alert-budget work in week 11.

## How the settings were chosen

The settings were picked on validation PR-AUC, using seed 0 only. The test set
was not used to choose them. Two things mattered:

- **Class weighting hurt.** The floor weights positives by the class ratio
  (about 1000×). On the full feature set this made both libraries unstable.
  LightGBM's validation PR-AUC was best after a single tree (under 9%), and
  early stopping kept that tree. Weighting is therefore off by default;
  `--class-weight` turns it back on. The threshold is tuned on validation
  anyway, so an unweighted model loses nothing by being badly calibrated.
- **Slow, regularised trees beat fast ones.** For LightGBM:

  | Setting | Learning rate | Leaves | Other limits | Validation PR-AUC |
  | --- | --- | --- | --- | --- |
  | Fast | 0.1 | 63 (max depth 6) | minimum leaf 100 | about 39 at best, falling to 27 by the time it stopped |
  | Slow | 0.02 | 31 | minimum leaf 200, L2 penalty 10 | 52 and still rising |

  XGBoost went from a validation PR-AUC of 48.6 to 51.1 with a learning rate
  of 0.05, a minimum child weight of 5 and an L2 penalty of 10.

Both libraries keep their final settings in `graph_boost.py`. With
row + graph features, early stopping ends training after 350–850 trees, well
under the caps of 2,000 (XGBoost) and 3,000 (LightGBM) rounds. Graph features
alone train much longer: XGBoost needs 1,400–1,700 trees and LightGBM gets
within 100 of its cap. That ablation might gain a little from a higher cap,
but not enough to change the conclusion.

## Against published numbers

Egressy et al. (AAAI 2024, "Provably Powerful Graph Neural Networks for
Directed Multigraphs") report minority-class F1 on HI-Small with the same
60/20/20 time split:

| Model | F1 |
| --- | --- |
| XGBoost on their graph features | 63.2 |
| LightGBM on their graph features | 62.9 |
| Multi-PNA with edge updates | 68.2 |

Our comparable number is the one that includes the tail, because they do not
remove it: 58.6. Two differences make this gap expected rather than alarming:

- Their test graph holds all transactions, and each payment's features are
  computed on that graph, so a test payment's features can use payments that
  come after it. Ours use only what had arrived before the payment, as a live
  system must.
- Their feature set is larger. It includes a more general cycle search, which
  is not bound by our time order and four-day window, and more pattern types.

A non-causal version of our features would close part of the gap, but it could
never be deployed, so it is not worth building. Closing the gap with features
that work on a stream is the job of weeks 13–16 (GNNs). The open decision on a
time-independent ring search also bears on it.

## Memory

The run fits in about 6 GB:

- The 30 features for all 5 million rows are built once, as a single float32
  matrix (about 600 MB).
- The splits are slices of that matrix, not copies.
- XGBoost reads it through a `QuantileDMatrix`.

An earlier attempt that copied each split separately was killed for running
out of memory on the 7 GB cloud VM.

## Without the payment method

Week 12's explanations showed the model leaning hardest on one fact: the payment
was made by ACH. On HI-Small that is a strong clue, because ACH has 48 times the
laundering rate of the other methods, and 87% of the test period's laundering
(999 of 1,143 payments) is ACH. That comes from how IBM generated the data,
not from how laundering works. So the real question for this project is what
the graph features are worth when the method gives nothing away. Two
experiments answer it, under the same protocol as everything above:

```bash
# the payment method left out
python -m detektive_ml.models.graph_boost data/processed/hi_small.parquet graph.csv \
    --models lgbm --features both row --drop payment_format --scores
# ACH payments only: train, validate and test on them alone
python -m detektive_ml.models.graph_boost data/processed/hi_small.parquet graph.csv \
    --models lgbm --features both row graph --only-format ACH
```

`--only-format` keeps the calendar split of the whole file, so the periods are
the same days as in every other result.

**All payments, the payment method left out** (LightGBM, seeds 0, 1 and 2, in
percent):

| Features | F1 | PR-AUC | F1 incl. tail | PR-AUC incl. tail |
| --- | --- | --- | --- | --- |
| Row + graph, with the method (week 10) | 42.79 ± 0.73 | 39.90 ± 0.53 | 58.61 ± 0.59 | 59.39 ± 0.50 |
| Single row, without the method | 3.92 ± 0.23 | 0.93 ± 0.01 | 7.16 ± 0.12 | 2.55 ± 0.04 |
| Graph only (never had the method, week 10) | 6.97 ± 0.16 | 10.94 ± 0.13 | 11.35 ± 0.25 | 15.41 ± 0.12 |
| **Row + graph, without the method** | **31.59 ± 0.62** | **28.66 ± 0.12** | **49.98 ± 0.50** | **49.93 ± 0.20** |

**ACH payments only** (138,141 test payments, 999 of them laundering, a rate of
0.72%; a model guessing at random would have a PR-AUC of about 0.7):

| Features | F1 | PR-AUC | F1 incl. tail | PR-AUC incl. tail |
| --- | --- | --- | --- | --- |
| Single row | 15.38 ± 0.10 | 8.28 ± 0.13 | 26.76 ± 0.24 | 19.14 ± 0.10 |
| Graph only | 30.95 ± 0.50 | 32.03 ± 0.29 | 41.82 ± 0.49 | 43.27 ± 0.31 |
| **Row + graph** | **46.68 ± 0.44** | **45.00 ± 0.57** | **61.73 ± 0.47** | **64.23 ± 0.50** |

What this says:

- **The method was worth about 11 F1 points, not the whole model.** Without
  it, F1 falls from 42.8 to 31.6 and PR-AUC from 39.9 to 28.7. That is a real
  loss, but most of the model stands.
- **What stands is the graph features.** Without the method, single-row
  features are close to useless: a PR-AUC of 0.9, against 0.11 for random
  guessing. Adding the graph features takes F1 from 3.9 to 31.6.
- **Inside ACH, where the method can tell the model nothing, the same holds.**
  The graph features alone beat the single-row features (PR-AUC 32 against
  8), and together they reach 45. So the rings, scatter-gathers, fan-ins and
  first payments to a new payee separate laundering from ordinary ACH
  payments. IBM's ACH quirk does not explain them away.
- **Neither half works alone; together they do.** Row features without the
  method (3.9) and graph features alone (7.0) both look weak. Combined they
  reach 31.6. Amount, currency and time of day matter once the structure
  around a payment is known, and not before. Week 10 found the same thing
  with the method included.

**At a review budget**, the model without the method still beats the
hand-written rules from week 11 by a wide margin. Seed 0, test period without
the tail:

| Alerts a day | Precision, without the method | Recall | Precision, with the method | Recall | Precision, rules | Recall |
| --- | --- | --- | --- | --- | --- | --- |
| 25 | 91% | 5.9% | 96% | 6.3% | 49% | 3.2% |
| 100 | 62% | 16.3% | 78% | 20.5% | 22% | 5.8% |
| 1,000 | 16% | 42.0% | 20% | 53.2% | 3% | 7.1% |

**What it leans on instead.** These are the 337 test alerts of the seed-0
model without the method, explained with LightGBM's `pred_contrib`:

- A first payment to a new payee (`pair_count`) is the largest reason for 262
  of them.
- Next come the receiver being in a ring found earlier (`dst_cycles`, 45) and
  a scatter-gather (22).
- A graph feature is the largest reason for 330 of the 337 alerts.
- The weekday is the second reason for 160. That is worth watching. HI-Small's
  weekends have twice the weekday laundering rate, which may be another trait
  of the generator rather than of laundering.
- As with the method included, alerts with a ring or scatter-gather among
  their top five reasons are laundering more often: 83% against 66%.

A model trained without the method is what a bank would want to start from,
because its signal would carry over to data where laundering does not favour
one method. It is not yet the model the stream runs, which keeps the method;
week 11's scorer checks that a model's features match its own exactly. The
GNN weeks should be compared with both models, and with this one above all.

## What came next

Week 11 exports the seed-0 LightGBM model to ONNX and scores it in the stream,
from Rust. It also scores alerts at daily review budgets and looks at why test
scores fall below validation: [scoring.md](scoring.md).

## Tests

`ml/tests/test_graph_boost.py` checks the following:

- A payment with no graph row is an error.
- The splits come out as time-ordered slices, with labels and features still
  attached to the right rows.
- Each feature set picks the right columns.
- A category not seen in training is encoded as -1.
- Both models find a planted feature in a synthetic sample and rank it first.
