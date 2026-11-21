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
these (`out_of_order`) and never moves its clock backwards. The graph engine
will use event time with a lateness allowance, not arrival order, for exactly
this reason.

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

On the mock cluster (2-core cloud VM, release build): the replayer publishes
about 360,000 events per second including the final flush. Consumer
throughput against real Redpanda gets measured in week 4, with metrics.
