# Graph engine

Weeks 5 to 7 add the engine's core: a live graph of who paid whom over a
sliding window of event time (`crates/graph`), fed by the consumer, and the
first features read off it for every payment: degree, fan-in, fan-out,
repeats between a pair (week 6), and the temporal cycles a payment closes
(week 7).

```
Redpanda --> consumer --> watermarks (one clock per partition)
                     \--> engine: window graph --advance--> evict old payments
                                              --insert---> degree and fan features
                                              --search---> cycles this payment closes
```

## The window graph

Accounts are nodes. Every payment is its own edge, so two accounts can be
joined by many edges: a **directed multigraph**. Laundering patterns are
about time as much as structure ("14 new accounts paid in within a few hours,
then the money left"), so the graph only keeps payments inside a window of
event time, four days by default (the cycles section explains why). When the
watermark passes `t`, every payment older than `t - window` is evicted.

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
gives its memory back. On HI-Small (the fullest windows are the first days,
which are unusually busy):

| Window | Largest window | Accounts in it | Approx. memory | Speed, whole file |
| --- | --- | --- | --- | --- |
| 1 hour | 344,747 payments | 272,661 | 25 MB | 1.46 M payments/s |
| 24 hours | 1,115,882 | 437,135 | 76 MB | 0.72 M/s |
| **4 days (default)** | 2,284,988 | 511,553 | 146 MB | 0.36 M/s |
| 7 days | 3,732,489 | 513,515 | 228 MB | 0.19 M/s |

One core of a cloud VM, release build, after loading the file, with every
feature (degree, fan and cycles) computed for every payment but not written
out. Most of the time goes to the cycle search, whose cost grows with the
window; the degree and fan features alone ran at 1.2 to 1.7 M payments/s for
any of these windows. Run it yourself:

```bash
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv
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
(Blanuša et al., 2024) starts from; cycles follow below, scatter-gather in
week 8.

**Streaming aggregation.** Recounting a neighbourhood for every payment would
cost as much as the account's degree, and the busiest account in HI-Small
pays 10,912 different accounts in one day. Instead every account keeps a
running count of payments per counterparty, in each direction, updated when a
payment enters the window and when it leaves. Fan-out is the number of
counterparties with a non-zero count; the pair count is one lookup. All nine
features cost nine lookups for any account, hub or not.

**Degree distribution.** Measured in week 6, when the window was 24 hours.
In the fullest 24-hour window of HI-Small (437,073 accounts):

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

## Cycles (week 7)

A cycle is money that comes back: A pays B, B pays C, C pays A. In a
**temporal** cycle the payments also happen in that order, each no earlier
than the one before. Laundering cycles have to be temporal, since money must
arrive before it can move on. A ring whose payments run backwards in time is
just accounts that know each other.

| Feature | Meaning |
| --- | --- |
| `cycles` | distinct temporal cycles this payment closes |
| `shortest_cycle` | payments in the shortest of them (0 if none) |
| `src_cycles`, `dst_cycles` | cycles found in the window that S / R is part of |

A cycle is only found when its last payment arrives, after the earlier ones
were scored. `src_cycles` and `dst_cycles` carry the finding to every later
payment of the accounts in the ring, until the ring's oldest payment leaves
the window.

**Why the window is now four days.** The patterns file that comes with
HI-Small lists 54 laundering cycles, of 2 to 12 payments each. They take
days, not hours:

| A cycle's payments all fall within | 6 h | 24 h | 48 h | 72 h | 96 h |
| --- | --- | --- | --- | --- | --- |
| cycles (of 54) | 1 | 5 | 12 | 27 | 54 |

The longest takes 95.6 hours. With the week 6 window of 24 hours the engine
could see at most 5 of them; with four days it can see all. The fan patterns
are just as slow (median 77 to 85 hours), so the degree and fan features
moved to the same window.

**The search.** When a payment from U to V arrives, every new cycle it
completes ends with it. So the search looks for chains V → … → U whose
payments are in time order, no later than this one, inside the window, with
no account twice, and at most 12 payments including this one. It runs
backwards from U over payments *into* each account, going back in time:

- time only runs one way, so every step tightens the bound on the next
  (earlier) payment, and most chains die within a step or two;
- it goes against the grain of fan-out. An account pays up to 14,230 others
  in four days but is paid by at most 545, so walking incoming payments
  branches far less than walking outgoing ones.

**Counting accounts, not payments.** Ten payments each way between B and C
would be a hundred cycles if every combination counted, and it is one ring.
A cycle here is a sequence of accounts with at least one run of payments in
time order. Between each pair the search takes the latest suitable payment:
the later that one is, the more room earlier payments have, so if any run
exists, this one does. Each search also returns the shortest cycle it found
as actual payments, oldest first: the start of an explanation for an alert.

**A budget.** Enumerating cycles is exponential in the worst case. Each
search may look at 100,000 payments (`--cycle-budget`); past that it stops
and says so, and the consumer counts it. On HI-Small no search reached it.

**Results on HI-Small** (4-day window, cycles of up to 12 payments, the
whole file in time order):

- 3,357 payments closed 3,602 cycles; no search ran out of budget.
- **49 of the 54 laundering cycles were caught as they closed**: every one
  of the 49 whose payments are in time order. The other 5 (rings of 4, 7, 7,
  7 and 10 payments) are rings whose payments are not in time order: in one,
  the second payment happens a day after the third. By the definition above
  they are not cycles money could flow round, so the search does not see
  them. With the old 24-hour window it caught 11.
- Among all payments, 0.10% are laundering. Among the 3,357 that close a
  cycle, 9.7% are; among the 91 whose shortest cycle has 3 or more payments,
  61.5% are. Rings of two (A pays B, B pays A) are mostly normal (8.3%
  laundering) but still 80 times the base rate. Payments by or to an account
  already in a cycle: 1.6% of 26,446. As before, this is a description over
  the whole file, not a model result.

Reproduce it (needs the patterns file next to the data):

```bash
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --features features.csv
python -m detektive_ml.cycle_report data/raw/HI-Small_Trans.csv data/raw/HI-Small_Patterns.txt features.csv
```

## How it is tested

- **Against a brute-force reference** (`crates/graph/tests/reference.rs`):
  random streams over three partitions with uneven speeds, late payments,
  parallel edges, payments to oneself, hubs and a partition that goes quiet.
  After every payment the graph must match a plain list of every payment
  ever inserted, filtered by the window: degrees, fan-in, fan-out, pair
  counts, the edges themselves, live payments, accounts and counterparty
  pairs. 30 random streams; the test checks that eviction and refusal both
  actually happen in them.
- **Cycles against brute force** (`crates/graph/tests/cycles_reference.rs`):
  for every payment of 60 random streams (small, dense, with parallel
  payments, payments to oneself and late payments), list every chain of
  individual payments from the receiver back to the sender in time order,
  with no shortcuts, and group them by accounts. The engine must find exactly
  the same cycles, the same shortest length, the same oldest-payment time
  for each, a valid shortest example made of real payments in the window,
  and the same per-account counts as cycles expire. Over 32,000 cycles,
  including ones of 6 payments; with the length limit raised to 12 too.
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
and sender fan-out at p50, p99 and p99.9, then payments closing a cycle per
minute (rings of two apart from longer ones), cycles in the window, cycles
closed, and cycle searches cut short (should be 0). The consumer also logs
every payment that closes a cycle, with the ring's accounts.

## Known gaps

- **Rings out of time order are not found.** 5 of HI-Small's 54 laundering
  cycles have payments out of order. A plain (non-temporal) cycle search
  inside the window would find them, at a much higher cost since time no
  longer prunes the search; the Graph Feature Preprocessor offers both kinds.
  A candidate for week 9.
- **A ring is flagged when it closes, not before.** Its earlier payments were
  already scored by then. `src_cycles` and `dst_cycles` flag the accounts'
  later payments; raising an alert on the earlier ones after the fact is a
  job for the alert layer (week 12).

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
- **Cycle enumeration**: Johnson's algorithm (1975) for all simple cycles of
  a directed graph, and why it is not used directly here: the engine only
  needs cycles through one new payment, bounded in length and time. Temporal
  cycles and time-window pruning (Kumar and Calders' 2SCENT, 2018; Blanuša
  et al.'s work on temporal and hop-constrained cycles); why bounded
  enumeration still needs a budget.
- **Degree distributions**: heavy tails, power laws and how to fit one
  (Clauset, Shalizi and Newman 2009, "Power-law distributions in empirical
  data"); why hubs dominate the cost of naive graph algorithms.
