# Evaluation protocol

Every number this project publishes comes from the same protocol, implemented
in `ml/src/detektive_ml/split.py` and `ml/src/detektive_ml/evaluate.py`.
Results from other papers are never compared against ours directly; we re-run
baselines here instead, because the same method scores very differently under
different setups.

## Split

Transactions are sorted by timestamp and cut at two points so that roughly
60% fall in training, 20% in validation and 20% in test. The cuts land on
timestamp boundaries, so transactions from the same minute never end up on
both sides of a cut.

A random split would let the model train on transactions that happen after
the ones it is tested on. That leaks the future and inflates scores. A bank
never gets to train on tomorrow.

## Metric

Laundering is about 0.1% of transactions in HI-Small. Accuracy is useless at
that rate (predicting "never laundering" scores 99.9%), and ROC-AUC looks
flattering because it rewards ranking the huge mass of normal transactions.
So we report:

- **Minority-class F1** on the test set, at a fixed alert threshold.
- **PR-AUC** (average precision) on the test set, which does not depend on a
  threshold.
- Precision, recall and the confusion counts behind them.

## Threshold

The threshold is the one that maximises F1 on the **validation** set. The test
set is only scored, never used to choose anything. Picking the threshold on
test would quietly overstate F1.

## The sparse tail

HI-Small ends with eight near-empty days (11–18 September) whose 1,108
transactions are 59% laundering. They all land in the test set, where they
hold 0.1% of rows but 36% of laundering, and they are easy to catch. On the
floor baseline they raise test F1 from 12.4 to 23.1.

`split.sparse_tail_start` finds such a tail automatically: the run of days at
the end of the data with under 1% of a typical day's volume. When one exists,
every model is scored twice with the same threshold, on the full test set and
without the tail. **The number without the tail is the headline**; the full
number is kept so results stay comparable with papers that use this split.

## Seeds

Each model runs with seeds 0, 1 and 2 by default. We report mean ± standard
deviation. A gap between two models smaller than their spread is not a result.

## Output

Each run writes `results/<model>__<dataset>__<utc time>.json` with the split
sizes, per-seed scores and the summary, so any number in the README can be
traced back to a file.
