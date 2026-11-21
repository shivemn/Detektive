//! Reads the `payments` topic, decodes each event and keeps running counts.
//! The graph engine will live inside this service from week 5.
//!
//! Delivery is at-least-once, built from three rules:
//!
//! 1. An offset is stored only after its message has been handled. librdkafka
//!    then commits stored offsets every second. A crash can repeat a few
//!    messages but never skip one.
//! 2. A message that cannot be decoded goes to a dead-letter topic with the
//!    reason attached, and only then is its offset stored. One bad message
//!    never stops the stream, and it is never silently dropped.
//! 3. Repeats are recognised by `tx_id` and skipped (see `state.rs`).

pub mod bitset;
pub mod event;
pub mod state;

use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Header, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{info, warn};

use crate::state::{Outcome, State};

#[derive(Debug, Clone)]
pub struct Config {
    pub brokers: String,
    pub topic: String,
    pub group: String,
    /// Where undecodable messages go. `None` logs and skips them instead.
    pub dead_letter_topic: Option<String>,
    /// Stop after handling this many messages (benchmarks and tests).
    pub stop_after: Option<u64>,
    /// Stop if no message arrives for this long (benchmarks and tests).
    pub idle_timeout: Option<Duration>,
    pub report_every: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub processed: u64,
    pub duplicates: u64,
    pub bad: u64,
    pub dead_lettered: u64,
    pub out_of_order: u64,
    pub accounts: usize,
    pub elapsed: Duration,
}

impl Summary {
    pub fn per_second(&self) -> f64 {
        let handled = self.processed + self.duplicates + self.bad;
        handled as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }
}

/// Run until `shutdown` resolves, `stop_after` messages are handled, or the
/// stream is idle for `idle_timeout`.
pub async fn run(cfg: Config, shutdown: impl Future<Output = ()>) -> Result<Summary> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false") // rule 1: we store offsets ourselves
        .set("auto.commit.interval.ms", "1000")
        // a member that dies without leaving loses its partitions after 6 s
        // (default 45 s); heartbeats must come at least 3 times per session
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "2000")
        .create()
        .with_context(|| format!("connecting consumer to {}", cfg.brokers))?;
    consumer.subscribe(&[&cfg.topic]).context("subscribing")?;

    let dead_letters: Option<FutureProducer> = match &cfg.dead_letter_topic {
        Some(_) => Some(
            ClientConfig::new()
                .set("bootstrap.servers", &cfg.brokers)
                .set("enable.idempotence", "true")
                .create()
                .context("creating dead-letter producer")?,
        ),
        None => None,
    };

    info!(topic = %cfg.topic, group = %cfg.group, "consuming");
    let mut state = State::new();
    let mut dead_lettered = 0u64;
    let started = Instant::now();
    let mut report = tokio::time::interval(cfg.report_every);
    report.tick().await; // the first tick fires at once; skip it
    tokio::pin!(shutdown);

    loop {
        if cfg
            .stop_after
            .is_some_and(|n| state.processed + state.duplicates + state.bad >= n)
        {
            break;
        }
        let next = async {
            match cfg.idle_timeout {
                Some(idle) => tokio::time::timeout(idle, consumer.recv()).await.ok(),
                None => Some(consumer.recv().await),
            }
        };
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown requested");
                break;
            }
            _ = report.tick() => log_progress(&state, started),
            received = next => {
                let Some(received) = received else {
                    info!("no messages for {:?}, stopping", cfg.idle_timeout.unwrap_or_default());
                    break;
                };
                let message = received.context("receiving")?;
                if let Outcome::Bad(reason) = state.handle(message.payload()) {
                    let reason = reason.to_string();
                    match (&dead_letters, &cfg.dead_letter_topic) {
                        (Some(producer), Some(topic)) => {
                            dead_letter(producer, topic, &message, &reason).await?;
                            dead_lettered += 1;
                        }
                        _ => warn!(partition = message.partition(), offset = message.offset(), %reason, "skipping bad message"),
                    }
                }
                consumer.store_offset_from_message(&message).context("storing offset")?;
            }
        }
    }

    // commit whatever was handled since the last automatic commit
    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
        Err(e) => return Err(e).context("final commit"),
    }
    // leave the group now, so its partitions move to another member at once
    // instead of after this one's session times out
    consumer.unsubscribe();
    log_progress(&state, started);

    Ok(Summary {
        processed: state.processed,
        duplicates: state.duplicates,
        bad: state.bad,
        dead_lettered,
        out_of_order: state.out_of_order,
        accounts: state.accounts(),
        elapsed: started.elapsed(),
    })
}

/// Copy a bad message to the dead-letter topic, with where it came from and
/// why it failed in its headers. Waits for the broker to confirm, so the
/// offset is only stored once the message is safe.
async fn dead_letter(
    producer: &FutureProducer,
    topic: &str,
    msg: &BorrowedMessage<'_>,
    reason: &str,
) -> Result<()> {
    let partition = msg.partition().to_string();
    let offset = msg.offset().to_string();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "error",
            value: Some(reason),
        })
        .insert(Header {
            key: "source.topic",
            value: Some(msg.topic()),
        })
        .insert(Header {
            key: "source.partition",
            value: Some(&partition),
        })
        .insert(Header {
            key: "source.offset",
            value: Some(&offset),
        });
    let mut record = FutureRecord::<[u8], [u8]>::to(topic).headers(headers);
    if let Some(key) = msg.key() {
        record = record.key(key);
    }
    if let Some(payload) = msg.payload() {
        record = record.payload(payload);
    }
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)
        .context("writing to dead-letter topic")?;
    warn!(partition = msg.partition(), offset = msg.offset(), %reason, "sent bad message to dead-letter topic");
    Ok(())
}

fn log_progress(state: &State, started: Instant) {
    let handled = state.processed + state.duplicates + state.bad;
    info!(
        processed = state.processed,
        duplicates = state.duplicates,
        bad = state.bad,
        out_of_order = state.out_of_order,
        accounts = state.accounts(),
        rate = format!(
            "{:.0}/s",
            handled as f64 / started.elapsed().as_secs_f64().max(f64::EPSILON)
        ),
        "progress"
    );
}
