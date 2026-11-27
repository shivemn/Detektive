# The replayer

`crates/replay` turns an IBM `*_Trans.csv` file into a stream of payment
events in time order, at any speed. Everything downstream (the graph engine,
the scorer) reads from this stream, so it never touches the CSV itself.

```
cargo run --release -p detektive-replay -- data/raw/HI-Small_Trans.csv --speed 100 --limit 1000
```

| Flag | Meaning |
| --- | --- |
| `--speed max` | As fast as possible (default; for benchmarks) |
| `--speed 1` | Real time: a minute in the data is a minute of waiting |
| `--speed 100` | 100× faster than real time |
| `--limit N` | Stop after N events |
| `--out PATH` | Write to a file; `-` is stdout (default), `none` discards, `kafka` publishes to Redpanda |
| `--brokers`, `--topic` | Where `--out kafka` publishes (default `localhost:19092`, `payments`) |
| `--india` | Add UPI-style ids, INR amounts and IST times |
| `--with-labels` | Include `is_laundering` (off by default) |

## Design decisions

**Sorted once, in memory.** The file is not in time order (its third row is
earlier than its first), so all rows are loaded and sorted by
`(timestamp, tx_id)`. `tx_id` is the row number in the file and breaks ties,
so the order is identical on every run and identical to the Python loader.
That was checked on all 5,078,345 HI-Small rows.

**Accounts become integers.** An account is only unique together with its
bank, so `"03208:8000F4580"` is the key. Each key gets a dense `u32` id in
order of first appearance. The graph engine needs exactly this: array
indexes instead of string hashing on every edge.

**Banks travel with the payment.** Each event also carries the sender's and
receiver's bank as small ids (`src_bank`, `dst_bank`, week 11), because the
model uses whether a payment stays inside one bank. In India that would be the
IFSC prefix, which every payment message carries anyway.

**Labels stay off the stream.** A real payment stream carries no answer key.
If the engine could see `is_laundering`, a bug could quietly let it cheat.
Evaluation joins labels back on by `tx_id` instead. `--with-labels` exists
for debugging and checks like the one above.

**Output behind a trait.** The replay loop only knows the `Sink` trait. The
Redpanda sink (`kafka.rs`, week 3) was added without touching the loop. It
sits behind the `kafka` cargo feature, so the replayer still builds on plain
Windows, where librdkafka is hard to compile. See
[streaming.md](streaming.md).

**Pacing is a pure function.** `Pacer::delay` computes how long to wait from
the event offset and the time already elapsed, so it is tested without
sleeping. If the sink falls behind, the replayer never waits; it catches up.

**The Indian view is cosmetic.** UPI-style ids (`acct0000042@dkbank`, a
made-up handle), INR amounts and an IST label change how events look, not
what they contain. The exchange rates are rough, fixed values near
September 2022, labelled as illustrative in the code; no model sees them.

## Numbers

HI-Small, release build, on a 2-core cloud VM (21 Nov 2026):

| Step | Result |
| --- | --- |
| Load and sort 5,078,345 rows, 515,088 accounts | 3.9 s |
| Replay to nothing (loop cost only) | 0.34 s, about 15 million events/s |
| Replay to a JSON-lines file (944 MB) | 1.9 s, about 2.7 million events/s |
| Peak memory | about 310 MB |

The replayer will not be the bottleneck: the IBM simulator's busiest day has
about 1.1 million transactions, roughly 13 per second on average.

## Reading the code

In the order data flows, with the Rust ideas each file leans on:

| File | Does | Rust ideas |
| --- | --- | --- |
| `model.rs` | Transaction record, currency and payment-format enums | enums, `match`, `Option`, `Copy` types |
| `accounts.rs` | "bank:account" → `u32` | `HashMap`, borrowing a reused buffer |
| `load.rs` | Read, validate and sort the CSV | `Result`, `?`, `anyhow` context, iterators, generics over `Read` |
| `pace.rs` | When each event is due | `FromStr`, `Duration`, pure functions for testing |
| `event.rs` | The JSON shape of an event | `serde` derive, `Option` with `then`, `&'static str` for fixed strings |
| `sink.rs` | Where events go | traits, generics (`JsonLines<W: Write>`), trait objects (`Box<dyn Sink>`) |
| `lib.rs` | The replay loop | slices, `&mut dyn Trait` |
| `kafka.rs` | Publishes events to Redpanda | `#[cfg(feature)]`, implementing a library's trait (`ProducerContext`), atomics, `Arc` |
| `main.rs` | Command-line flags | `clap` derive |

Every file ends with its tests (`#[cfg(test)] mod tests`). Run them with
`cargo test`.
