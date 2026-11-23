# Observability

Week 4 makes the stream measurable. The consumer exports Prometheus metrics,
Prometheus scrapes them every 5 seconds, and a provisioned Grafana dashboard
shows whether the stream is flowing, whether the consumer keeps up, how fast
it is, and how disordered event time is.

```
replay --> Redpanda --> consumer --/metrics--> Prometheus --> Grafana
```

## Running it

```bash
docker compose up -d --build              # first build compiles librdkafka: a few minutes
docker compose run --rm replay --speed 3600   # one hour of data per second
```

Then open http://localhost:3000. The "Detektive: payment stream" dashboard
is the home page; no login is needed (anonymous access is on, for local use
only).

On Windows, keep the dataset inside WSL for speed and point the replayer at
it: `DETEKTIVE_DATA_DIR=$HOME/detektive-data docker compose run --rm replay`.

If another project already uses one of the ports, pick others:
`GRAFANA_PORT=3001 PROMETHEUS_PORT=9091 docker compose up -d` (also
`CONSOLE_PORT` and `METRICS_PORT`; a `.env` file next to `compose.yaml` works
too). Grafana reaches Prometheus inside Docker's network, so it does not care.

To run the consumer with `cargo run` instead of in Docker, stop the container
first (`docker compose stop consumer`). Run that way, the consumer serves its
metrics on port 9899, and Prometheus also scrapes `host.docker.internal:9899`,
so the dashboard keeps working. The port differs from the container's 9898 on
purpose: the container's port is published on your machine too, so scraping
`host.docker.internal:9898` would read the same consumer twice and double
every total on the dashboard (an early version did exactly that).

## Three kinds of time

The metrics keep three different times apart, because they answer different
questions:

| Metric | Measures | Answers |
| --- | --- | --- |
| `detektive_consumer_handle_seconds` | CPU time to decode and count one message | Is the code fast? |
| `detektive_consumer_end_to_end_latency_seconds` | Wall-clock time from the producer writing a message to the consumer handling it | Is the system keeping up? |
| `detektive_consumer_event_lateness_seconds` | How far an event's own timestamp is behind the newest event already seen | How disordered is the data? |

The first two are **processing time**: they are about machines. The third is
about **event time**: when the payment happened, according to the data.
A stream can be fast and still badly out of order, and the graph engine cares
about both. Windows like "payments into this account in the last hour" must be
defined on event time, or a burst of replayed history would look like one busy
second.

## Back-pressure and lag

`detektive_consumer_lag_messages` is the number of messages in Redpanda that
the consumer has not read yet, per partition. librdkafka reports it in its
statistics every 5 seconds. Lag is how back-pressure shows up in a log-based
system: the broker does not slow the producer down, it stores the excess, and
the backlog grows. Flat, near-zero lag means the consumer keeps up. Lag that
keeps growing means it does not, and end-to-end latency will grow with it,
because every message waits behind the backlog.

## Why percentiles

Averages hide the slow cases. If 99 messages take 1 ms and one takes 1 s, the
mean is about 11 ms and describes none of them. The dashboard shows p50 (the
typical message), p90 and p99 (the slow tail). The tail is what decides whether
a deadline is met.

Latency is recorded as Prometheus histograms: counts of observations per
bucket (under 1 ms, under 2.5 ms, …). Grafana estimates percentiles from the
buckets with `histogram_quantile`, so a percentile is only as precise as the
bucket it falls in. Buckets were chosen for each metric's range: microseconds
for handling, milliseconds to minutes for latency, minutes to days of event
time for lateness.

## The metrics

| Name | Type | Meaning |
| --- | --- | --- |
| `detektive_consumer_messages_total{outcome}` | counter | Messages handled: `processed`, `duplicate`, `bad` |
| `detektive_consumer_dead_lettered_total` | counter | Messages sent to `payments-dead-letter` |
| `detektive_consumer_out_of_order_total` | counter | Events older than the newest already seen |
| `detektive_consumer_accounts` | gauge | Distinct accounts seen |
| `detektive_consumer_event_time_seconds` | gauge | Newest event time seen: the stream's clock |
| `detektive_consumer_lag_messages{partition}` | gauge | Unread messages per partition |
| `detektive_consumer_end_to_end_latency_seconds` | histogram | Producer to consumer, wall clock |
| `detektive_consumer_event_lateness_seconds` | histogram | Event-time lateness (0 when in order) |
| `detektive_consumer_handle_seconds` | histogram | Time to handle one message |
| `detektive_consumer_watermark_seconds` | gauge | The watermark: the slowest active partition's newest event |
| `detektive_consumer_behind_partition_total{partition}` | counter | Events older than something their own partition already delivered (expected 0) |
| `detektive_graph_edges` | gauge | Payments inside the graph window |
| `detektive_graph_accounts` | gauge | Accounts with a payment inside the window |
| `detektive_graph_memory_bytes` | gauge | Approximate memory held by the window |
| `detektive_graph_evicted_total` | counter | Payments evicted after leaving the window |
| `detektive_graph_too_late_total` | counter | Payments older than the window on arrival, not added |
| `detektive_graph_receiver_fan_in` | histogram | Receiver's fan-in at each payment |
| `detektive_graph_sender_fan_out` | histogram | Sender's fan-out at each payment |

What the graph metrics mean: [graph-engine.md](graph-engine.md).

The consumer also serves `/health`, which answers `ok`.

## How it is tested

- An end-to-end test runs the real metrics server, consumes from librdkafka's
  mock cluster (500 good messages and 1 bad one), then scrapes `/metrics` over
  HTTP, as Prometheus does. Every value must match what the consumer did,
  including a lag of 0 on each of the 3 partitions.
- A unit test reads the Grafana dashboard and fails if any query uses a metric
  name the consumer does not export, so the two cannot drift apart.

- `scripts/smoke.sh` runs the whole stack for real: it builds the image,
  starts Redpanda, the consumer, Prometheus and Grafana, replays 20,000
  synthetic transactions, and checks the count at every hop: the consumer's
  `/metrics`, a Prometheus query, the dashboard loaded in Grafana, and
  Grafana's connection to Prometheus. CI runs it on every relevant push
  (`.github/workflows/stack.yml`); it also runs locally.

Not covered: how the dashboard looks. Open it and judge.

## A full run: how late do events arrive?

The whole HI-Small file (5,078,345 transactions over 18 days) replayed at
speed 3600 (one hour of data per second) into the 3-partition topic, with the
consumer already running. Everything in Docker on a Windows laptop (WSL2),
fresh volumes.

| | |
| --- | --- |
| Replay | 424.4 s, about 12,000 events/s |
| Processed / duplicates / bad | 5,078,345 / 0 / 0 |
| Lag at the end | 0 on every partition |
| Arrived after a later event (`out_of_order`) | 233,971 (4.6%) |

Event-time lateness, read straight from the histogram buckets:

| Late by at most | Share of events |
| --- | --- |
| 0 s (in order) | 95.39% |
| 1 minute | 96.13% |
| 5 minutes | 98.72% |
| 15 minutes | 99.23% |
| 30 minutes | 99.37% |
| 1 hour | 99.59% |
| 2 hours | 100% |

So p99 is between 5 and 15 minutes (about 10.5 by interpolation, as
`histogram_quantile` would estimate it), p99.9 is between 1 and 2 hours, and
nothing was later than 2 hours.

**Where the lateness comes from.** Not from the data. The replayer sends in
timestamp order and the idempotent producer keeps order within a partition,
so each partition should arrive in order. The disorder comes from interleaving
three partitions, and that is wall-clock skew: at speed 3600, one second of
skew between partitions is one hour of event time. The 2-hour maximum is about
2 seconds of skew; p99 is about 0.2 seconds. At real speed the same skew would
be a second or two of event time.

**Catching up is much worse.** In an earlier run the consumer started only
after the replay had finished, so it read 5 million events as a backlog, in
large batches from one partition at a time. Then 68% of events arrived out of
order, the mean lateness was about 13 hours, and p99 was past the histogram's
top bucket: at least 48 hours. The 66% in [streaming.md](streaming.md), from
an early `--speed max` run, is the same effect.

**What this means for the graph engine.** One stream-wide clock (the newest
event seen) is the wrong watermark: a consumer catching up after a restart
would push it days ahead and close windows on events still to come. The engine
needs a watermark per partition and takes the slowest one as the stream's
watermark, as Flink and Kafka Streams do. If each partition is in order, that
leaves almost nothing late; a small allowance covers the rest, and anything
later still is counted rather than silently dropped. The graph engine now
works this way, and counts per-partition order directly to confirm it: see
[graph-engine.md](graph-engine.md#watermarks).
