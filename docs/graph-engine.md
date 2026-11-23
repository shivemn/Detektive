# Graph engine

Weeks 5 and 6 add the engine's core: a live graph of who paid whom over a
sliding window of event time (`crates/graph`), fed by the consumer, and the
first features read off it for every payment: degree, fan-in, fan-out and
repeats between a pair.

```
Redpanda --> consumer --> watermarks (one clock per partition)
                     \--> window graph --advance--> evict old payments
                                       --insert---> features for this payment
```

## The window graph

Accounts are nodes. Every payment is its own edge, so two accounts can be
joined by many edges: a **directed multigraph**. Laundering patterns are
about time as much as structure ("14 new accounts paid in within a few hours,
then the money left"), so the graph only keeps payments inside a window of
event time, 24 hours by default. When the watermark passes `t`, every payment
older than `t - window` is evicted.

**Layout.** Each account has two adjacency lists, payments out and payments
in, with the time, the other account and the amount stored in the list
itself. Walking a neighbourhood reads one contiguous list instead of
following pointers into a shared edge table. A matrix would need
515,000 × 515,000 cells for a graph where the average account has a handful
of edges; a compressed sparse row layout (CSR) is the fastest to read but has
to be rebuilt to change, and this graph changes on every payment. Adjacency
lists are the usual choice for a sparse graph that is updated constantly.

**Eviction.** An expiry index, ordered by time, says which payments leave the
window next, so advancing the window touches only what is leaving. Each
evicted payment is popped from the front of its two adjacency lists: payments
arrive almost in time order, so the oldest is almost always at the front. A
late payment can sit behind a newer one. It then lingers in the list after
leaving the window until the payments in front of it go too. The live counts
come from the expiry index and are exact either way, and every read filters
by the window, so a lingering payment is never seen.

**Bounded memory.** Memory follows the number of payments inside one window,
not how long the stream has run. An account with nothing left in the window
gives its memory back. On HI-Small (the fullest window is the first day,
which is unusually busy):

| Window | Largest window | Accounts in it | Approx. memory | Speed, whole file |
| --- | --- | --- | --- | --- |
| 1 hour | 344,747 payments | 272,661 | 25 MB | 1.7 M payments/s |
| 6 hours | 511,935 | 330,479 | 36 MB | 1.2 M/s |
| 24 hours | 1,115,882 | 437,135 | 76 MB | 1.2 M/s |
| 72 hours | 2,077,379 | 510,778 | 134 MB | 1.3 M/s |

One core of a cloud VM, release build, after loading the file, with features
computed for every payment but not written out. Writing all features to CSV
as well: 0.72 M payments/s. Run it yourself:

```bash
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --window-hours 24
```

## Watermarks

Payments are evicted on the **watermark**, the event time the whole stream is
known to have reached, not on the newest event seen. Week 4 measured why:
three partitions are read at slightly different speeds, and a consumer
catching up on a backlog reads one partition far ahead of the others, by more
than 48 hours of event time (see
[observability.md](observability.md#a-full-run-how-late-do-events-arrive)).
Evicting on the newest event would throw away payments that still belong in
the window before they arrive.

So each partition keeps its own clock, the newest event it has delivered,
and the watermark is the slowest of them. Each partition arrives in order
(the replayer sends in order and the idempotent producer keeps order within a
partition), so nothing should arrive behind its own partition's clock. The
consumer counts any that do, as
`detektive_consumer_behind_partition_total{partition}`: it is the direct
check of that claim, and it stays 0 in the tests and the smoke run.

Two refinements, each found by a test:

- **Assigned but silent partitions hold the watermark.** When the consumer
  is given partitions 0, 1 and 2 and partition 0 delivers a burst first, the
  watermark must not move on partition 0's clock alone. The consumer passes
  each assignment from librdkafka's rebalance callback to the watermarks,
  and a partition that has delivered nothing yet holds the watermark where it
  is. Without this, the end-to-end test (3,000 payments, 50 hours, three
  partitions) refused 912 payments as too late; with it, none.
- **Idle partitions stop holding it.** A partition silent for 10 s of wall
  clock (`--idle-partition-secs`) is left out until it delivers again, or a
  quiet partition would stop the window forever. The cost: if it comes back
  with older events, they arrive behind the watermark. They still go into the
  graph if they are inside the window; ones older than the window are
  refused and counted (`detektive_graph_too_late_total`), never dropped
  silently.

The watermark never moves backwards.

## Features (week 6)

For a payment from S to R, right after it goes into the window:

| Feature | Meaning |
| --- | --- |
| `src_out_degree`, `src_in_degree` | payments S sent / received in the window |
| `src_fan_out`, `src_fan_in` | distinct accounts S paid / was paid by |
| `dst_in_degree`, `dst_out_degree` | the same for R |
| `dst_fan_in`, `dst_fan_out` | |
| `pair_count` | payments from S to R in the window, this one included |

Degree counts repeats; fan counts distinct counterparties. These are the
simplest graph features and the ones the Graph Feature Preprocessor
(Blanuša et al., 2024) starts from; cycles and scatter-gather follow in
weeks 7 and 8.

**Streaming aggregation.** Recounting a neighbourhood for every payment would
cost as much as the account's degree, and the busiest account in HI-Small
pays 10,912 different accounts in one day. Instead every account keeps a
running count of payments per counterparty, in each direction, updated when a
payment enters the window and when it leaves. Fan-out is the number of
counterparties with a non-zero count; the pair count is one lookup. All nine
features cost nine lookups for any account, hub or not.

**Degree distribution.** In the fullest 24-hour window of HI-Small (437,073
accounts):

| | median | p99 | p99.9 | max |
| --- | --- | --- | --- | --- |
| fan-in | 1 | 4 | 7 | 92 |
| fan-out | 1 | 6 | 10 | 10,912 |

Almost every account deals with one to three others. Fan-in falls off quickly
(a power-law fit to the tail gives an exponent of about 3.2, steep for a real
payment network). Fan-out has the same steep body plus about 15 hubs paying
hundreds to 10,912 accounts a day, the simulator's equivalent of payroll or
large merchants. This is why the features are kept as counts, not sums over
neighbours, and why a single hub must not slow anything down.

**A first look at the signal.** Joining the features to the labels over the
whole file (a description, not a model result; models are trained and judged
under the protocol in week 10):

| Share of payments where... | normal | laundering |
| --- | --- | --- |
| it is S's first payment to R in 24 h (`pair_count` = 1) | 39% | 83% |
| R was paid by 2+ accounts (`dst_fan_in` ≥ 2) | 44% | 58% |
| R has not paid anyone yet (`dst_out_degree` = 0) | 49% | 65% |

Laundering money mostly moves between accounts that have not just been
dealing with each other, into accounts that are collecting from several
places. That is what the single-row floor model in the results table cannot
see.

## How it is tested

- **Against a brute-force reference** (`crates/graph/tests/reference.rs`):
  random streams over three partitions with uneven speeds, late payments,
  parallel edges, payments to oneself, hubs and a partition that goes quiet.
  After every payment the graph must match a plain list of every payment
  ever inserted, filtered by the window: degrees, fan-in, fan-out, pair
  counts, the edges themselves, live payments, accounts and counterparty
  pairs. 30 random streams; the test checks that eviction and refusal both
  actually happen in them.
- **Against an independent recount in Python**
  (`ml/src/detektive_ml/graph_check.py`): numpy recounts every feature for
  every payment from the raw CSV, with none of the engine's bookkeeping, and
  compares. `scripts/check_features.sh` runs it on a synthetic sample with a
  24-hour and a 1-hour window, in CI. On four hours of real HI-Small
  (77,781 payments, including an account paying 356 others in an hour) all
  nine features match exactly.
- **End to end on the mock cluster**: 3,000 payments over 50 hours through
  three partitions. Nothing arrives behind its own partition, nothing is
  refused, and every payment is either in the window or was evicted from it.
  The smoke test checks the same on real Redpanda.

## The dashboard

A new row, "The graph window": payments and accounts in the window, its
memory, the in-order check per partition (should be 0), payments refused as
too late (should be 0), how far the watermark trails the newest event, and
payments in against payments evicted per second. Below it, receiver fan-in
and sender fan-out at p50, p99 and p99.9.

## Known gaps

- **Online features see a slightly different window from offline ones.**
  Offline (the `detektive-graph` tool, and training in week 10) the stream is
  one sorted sequence and a payment at time t sees exactly the payments in
  [t − window, t] up to itself. Online, partitions interleave, so a payment
  can also see a few payments from another partition that are a little newer
  than itself (about 0.2 s of real time at p99, measured in week 4). The
  difference is small but is a training-serving skew; week 11 measures it.
- **The window is lost on restart.** The graph lives in memory. A restarted
  consumer resumes from its committed offsets with an empty window, so
  features are low for one window length. The fix is to rewind each
  partition to the offset one window back on startup and rebuild silently;
  planned with the scorer.
- **Account ids are dense small integers** because the replayer assigns them.
  Data from elsewhere needs the same interning in its adapter (week 23).
- **Amounts are kept but not used yet.** Payments are in many currencies; the
  amount features (and fixed exchange rates) come with the pattern detectors.

## Theory behind it

- **Graph representations**: adjacency matrix, adjacency list, CSR; which
  operations each makes cheap; why a constantly changing sparse graph uses
  lists.
- **Sliding windows**: tumbling vs sliding vs session windows; evicting by an
  expiry index instead of scanning; why memory is bounded by window size.
- **Watermarks**: event time vs processing time; per-partition watermarks and
  idleness, as in Flink's `WatermarkStrategy.withIdleness` and Kafka Streams'
  stream time.
- **Streaming aggregation**: keeping counts incrementally (add on insert,
  subtract on evict) instead of recomputing; which aggregates allow this
  (counts, sums) and which do not (max, distinct counts without per-key state).
- **Degree distributions**: heavy tails, power laws and how to fit one
  (Clauset, Shalizi and Newman 2009, "Power-law distributions in empirical
  data"); why hubs dominate the cost of naive graph algorithms.
