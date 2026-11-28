# Why an alert fired

Week 12 makes every alert explain itself. Each alert now carries the five
features that moved its score most, in words, with how far each one pushed
it. The numbers are checked against LightGBM's own explanations.

An alert from the test period, a payment IBM lists as part of a scatter-gather:

```
score 0.485 (alerts at 0.161); log-odds -11.11 before anything is known about it
  +4.58  paid by ACH
  +2.35  the sender's first payment to this receiver in the last 4 days
  +1.48  the sender's money reached the receiver through 13 different middle accounts
  +1.01  the receiver received 19 payments in the last 4 days
  +0.76  the sender received 12 payments in the last 4 days
```

In the alert JSON these sit under `explanation.reasons`, each with its
`feature`, `text` and `log_odds`.

## How it works

The method is path-dependent TreeSHAP (Lundberg, Erion and Lee, 2018), the
same one behind LightGBM's `predict(pred_contrib=True)`. For each tree, the
leaf's value is shared among the features split on along the way. A split
counts for a feature as much as the payment's branch differs from what the
training data did at that node on average. The shares are additive. The
expected log-odds (what the model says before it knows anything about the
payment) plus every feature's share is exactly the payment's log-odds. An
explanation therefore never leaves part of a score unaccounted for.

TreeSHAP needs to know how many training rows reached each node, and ONNX has
nowhere to keep that. The export therefore writes it next to the model, as
`cover.json`. It walks the ONNX trees side by side with LightGBM's dump, as
the threshold fix in week 11 does, so each count lands on the node it belongs
to. For a model exported before week 12, `python -m detektive_ml.models.cover
models/` adds the file in seconds. A model without it still scores; its
alerts just carry no reasons.

The algorithm is in `crates/score/src/shap.rs`. Its unit tests check it
against Shapley values computed by brute force, from the definition, over
every order of the features. The test tree splits on one feature twice along
a path, which exercises the hardest step of the algorithm. `reasons.rs` turns
each feature and its value into words.

## Against LightGBM

`detektive-score --explain` was run over HI-Small, explaining all 3,106
alerts and every 250th payment that did not alert: 23,408 payments in all.
`models.check_contributions` then compared each of them with LightGBM's
`pred_contrib`:

| | All 23,408 | The 3,106 alerts |
| --- | --- | --- |
| Largest difference in any feature's share | 2.1 × 10⁻⁷ | 1.4 × 10⁻⁷ |
| Largest difference in the expected value | 1.2 × 10⁻⁷ | 1.2 × 10⁻⁷ |
| Same largest reason as LightGBM | all | all |
| Shares plus expected value against the score's log-odds | 8.6 × 10⁻¹⁴ | |

Shares run up to 6 log-odds, so the differences are in the seventh
significant digit. They come from the 32-bit leaf values in the ONNX file,
the same source as the scores' own differences in week 11. The same check
runs in CI on the test model, against a file of LightGBM's own
contributions.

## What it costs

| | Mean | p50 | p99 |
| --- | --- | --- | --- |
| Scoring a payment | 14.8 µs | 13.5 µs | 39.7 µs |
| Explaining it | 5.3 ms | 5.1 ms | 7.9 ms |

Explaining costs about 350 times as much as scoring, because TreeSHAP tracks
a path for every leaf of every tree. So the consumer explains only the
payments that alert. At about 175 alerts a day, that is under a second of
work a day. On the dashboard it is "Time to explain one alert"
(`detektive_score_explain_seconds`).

## What the explanations show about the model

`models.explain_report` sums up the reasons behind the 525 alerts of the test
period (sparse tail left out), 358 of which are laundering. What it shows
matters more than any single explanation:

**The payment method is the largest reason for every one of the 525
alerts.** Paying by ACH adds a median of 4.7 to the log-odds. On HI-Small:

- ACH has 48 times the laundering rate of the other methods: 0.75% against
  0.016%;
- 87% of all laundering is ACH;
- IBM's listed laundering is ACH in 3,208 of 3,209 payments.

This comes from how IBM's generator made the data. Nothing suggests real
laundering prefers ACH like that, and a model trained on a bank's own data
would not learn this rule. It is the clearest sign yet of how far the HI-Small
numbers can be trusted outside HI-Small.

**The second reason is almost always a first payment to a new payee.** For
472 of the 525 alerts, the second reason is `pair_count`: 87% of alerts are a
sender's first payment to that receiver in the window. The next reasons
differ more. The sender's incoming payments come third most often (151
alerts), then the weekday (87), the receiver's fan-in (58) and the amount
(49).

**The rings and scatter-gathers are rarely what tips a score, but they mark
the better alerts.** A ring or scatter-gather feature is among the top five
reasons for only 111 of the 525 alerts. Those alerts are laundering 86% of the
time, against 64% for the rest.

So what the model mostly learned is "a first ACH payment to a new payee",
which happens to be how HI-Small's laundering looks. The structural features
refine it. That makes the explanations useful twice:

- for an analyst, an alert with a ring among its reasons deserves to be
  looked at first;
- for this project, the graph features need to earn their place on data where
  the payment method gives nothing away.

Two ways to test that are open. One is to train without the payment method
and see what is left. The other is the GNN work in weeks 13–16, which learns
structure directly.

## Running it

```bash
# explanations for every alert in a file, and every 250th other payment
cargo run --release -p detektive-score -- data/raw/HI-Small_Trans.csv --model models \
    --explain explain.jsonl --explain-every 250

cd ml
python -m detektive_ml.models.check_contributions ../data/processed/hi_small.parquet ../graph.csv ../models ../explain.jsonl
python -m detektive_ml.models.explain_report ../data/processed/hi_small.parquet ../graph.csv \
    ../models/scores.csv ../explain.jsonl --patterns ../data/raw/HI-Small_Patterns.txt --raw ../data/raw/HI-Small_Trans.csv
```

In the stack, the consumer explains alerts as soon as `cover.json` is in the
model directory. The reasons appear in the alerts topic and the first one in
the consumer's log.

## Known gaps

- **Reasons are per feature, and some features overlap.** Several degree
  features measure nearly the same thing, so one cause can show up as two
  reasons. Grouping them ("the receiver is busy") would read better.
- **Shares are in log-odds, not in score.** Log-odds add up and probabilities
  do not, so this is the honest unit. An analyst would still prefer a plain
  phrase such as "most of the score".
- **The ring itself is shown as account ids.** It is not yet drawn. That
  belongs to the case view in the console (weeks 17–19).
