# Streaming through Redpanda

Week 3 puts a real message broker between the replayer and everything after
it, the way a bank's payment feed would arrive.

```
replayer --(payments topic, 3 partitions)--> consumer --(bad messages)--> payments-dead-letter
```

## Running it

In WSL (librdkafka, the Kafka client underneath, does not build well on plain
Windows):

```bash
docker compose up -d                       # Redpanda, its console, the topics
cargo run --release -p detektive-replay --features kafka -- \
    data/raw/HI-Small_Trans.csv --out kafka --speed max --limit 100000
cargo run --release -p detektive-consumer -- --idle-secs 10
```

Redpanda Console at http://localhost:8080 shows the topics, partitions and
the consumer group's lag. `docker compose down -v` removes everything.

## Delivery guarantees

The stream is **at-least-once**, and the consumer turns that into
**effectively-once counting**:

| Where | Rule | Why |
| --- | --- | --- |
| Producer | Idempotent, `acks=all` | A retry after a network blip cannot write an event twice; nothing counts as sent until every replica has it |
| Producer | Key = sender account | One sender's payments stay on one partition, in order |
| Consumer | Store an offset only after handling its message; librdkafka commits stored offsets every second | A crash repeats a few messages but never skips one |
| Consumer | Undecodable message → dead-letter topic, with the reason and source offset in headers, confirmed before its offset is stored | One bad message never stops the stream and is never silently lost |
| Consumer | Skip any `tx_id` already seen (a 0.6 MB bitset for 5M ids) | Repeats from at-least-once delivery are not counted twice |
| Consumer | Leave the group on shutdown; 6 s session timeout | Partitions move to another member at once on a clean stop, within 6 s on a crash |

What this does not give yet: the seen-set lives in memory, so after a crash
it is rebuilt from scratch and a handful of repeated messages are counted
again. True exactly-once needs the engine's state and the offsets saved
together, which comes with the graph engine's checkpoints.

## Event time and arrival order

With three partitions, events arrive interleaved: a later payment from one
partition can be read before an earlier one from another. The consumer counts
these (`out_of_order`) and never moves its clock backwards.

On real Redpanda this is not a corner case. Replaying the first 100,000
HI-Small transactions through the 3-partition topic, **65,940 (66%) arrived
after an event with a later timestamp**. The consumer reads each partition
in bursts, so whole batches from one partition land before the next. The
graph engine must therefore window on event time with a lateness allowance
and a watermark, never on arrival order. How late events actually arrive, on
a full replay and while catching up, is measured in
[observability.md](observability.md#a-full-run-how-late-do-events-arrive).

## Testing without a broker

The integration tests (`crates/consumer/tests/end_to_end.rs`) run the real
producer and consumer against librdkafka's in-process mock cluster, which
speaks the Kafka protocol. They check that every replayed event arrives once,
that a restarted consumer resumes from its committed offsets, that bad
messages reach the dead-letter topic with their reasons, that duplicates are
skipped, and that several partitions do deliver events out of time order.
They run in CI with no Docker.

Two things the mock cluster taught us:

- It does not act on a consumer's leave-group request, so a restarted member
  waits out the old session. That is why the session timeout is 6 s, which is
  also the better production setting.
- It keeps only a limited amount of data per partition and deletes the oldest
  beyond that (about 100,000 HI-Small events with LZ4). Real Redpanda keeps
  data for its configured retention. The throughput test checks the consumer
  against what the broker actually retained.

## Numbers so far

| Where | What | Result |
| --- | --- | --- |
| Real Redpanda, local Docker (WSL) | 100,000 HI-Small events, replay then consume | All 100,000 processed, 0 duplicates, 0 bad, 105,585 distinct accounts |
| Real Redpanda, same laptop | 200,000 messages (the same 100,000 sent twice), new consumer group | 100,000 processed, 100,000 duplicates skipped, 1.33 s, about 150,000 messages/s |
| Mock cluster, 2-core cloud VM, release build | Replayer publishing, including the final flush | about 360,000 events/s |
| Mock cluster, same machine | Consumer, first to last message | about 600,000 events/s |

Throughput, lag and latency are now exported as Prometheus metrics; see
[observability.md](observability.md).

On Windows, read the dataset from inside WSL (for example `~/detektive-data`),
not from `/mnt/c/...`: loading HI-Small took 103.6 s from the C: drive
against about 4 s from a Linux filesystem.

The idle stop (`--idle-secs`) counts only while the consumer owns
partitions, so a slow group join does not end a run before it starts, and
progress lines report the rate since the previous line rather than a
running average.
