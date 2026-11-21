# HI-Small: what the data looks like

Notes from `python -m detektive_ml.explore` on IBM HI-Small (21 Nov 2026).
The data is synthetic; these are facts about the simulator's output.

## Size

| | |
| --- | --- |
| Transactions | 5,078,345 |
| Accounts (bank + account id) | 515,088 |
| Time range | 1 Sep 2022 00:00 to 18 Sep 2022 16:18 |
| Laundering | 5,177 (0.102%, about 1 in 980) |
| Self-transfers (same account both sides) | 591,212 (11.6%) |

## The sparse tail

Daily volume drops from hundreds of thousands to a few hundred after
10 September, but laundering keeps going:

| Days | Transactions | Laundering | Laundering share |
| --- | --- | --- | --- |
| 1–10 Sep | 5,077,237 | 4,522 | 0.09% |
| 11–18 Sep | 1,108 | 655 | 59% |

The simulator appears to stop generating normal traffic while laundering
chains still complete. All of the tail falls in the test window of the
60/20/20 split, where it holds 0.1% of test rows but 36% of test laundering.
It is easy to catch, so it roughly doubles test scores. The protocol now
reports every model with and without it (see `evaluation-protocol.md`).

The first day is also unusually heavy (1.11M transactions against about
480K on a normal weekday).

## Where laundering sits

By payment format, ACH carries 87% of laundering (4,483 of 5,177) at a rate
of 0.75%, ten times the average. Reinvestment and Wire contain no laundering
at all. By currency, Saudi Riyal stands out at 0.42%, four times the average;
the rest sit near 0.1%.

## Shape of the graph

Most accounts are quiet: the median account sends 2 and receives 2
transactions. A few are enormous hubs: one account sends 168,672
transactions. The engine's time window and memory bounds must handle these
hubs without slowing down the common case.

## Laundering attempts by pattern

From `HI-Small_Patterns.txt`, 370 attempts in eight shapes:

| Pattern | Attempts |
| --- | --- |
| Cycle | 54 |
| Gather-scatter | 51 |
| Bipartite | 49 |
| Fan-out | 48 |
| Scatter-gather | 44 |
| Stack | 43 |
| Random | 41 |
| Fan-in | 40 |

These attempts list 3,209 transactions, so about 38% of the 5,177 laundering
transactions belong to no named pattern. A detector built only around these
eight shapes cannot catch everything.

## Split

| Part | From | Rows | Laundering | Rate |
| --- | --- | --- | --- | --- |
| Train | 1 Sep | 3,046,861 | 2,297 | 0.075% |
| Validation | 6 Sep 13:36 | 1,015,602 | 1,082 | 0.106% |
| Test | 8 Sep 16:12 | 1,015,882 | 1,798 | 0.177% |

The laundering rate more than doubles from train to test, partly because of
the tail. Thresholds chosen on validation will drift on test; that is the
honest situation a deployed model is in too.
