//! A `Sink` that publishes events to a Redpanda (or any Kafka) topic.
//!
//! Each event is keyed by its sender account, so all payments from one
//! account land on the same partition and stay in order. The producer is
//! idempotent with `acks=all`: a retry after a network blip cannot write the
//! same event twice, and nothing counts as sent until every replica has it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rdkafka::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};

use crate::event::Event;
use crate::sink::Sink;

/// Counts messages the broker finally rejected, after all retries.
#[derive(Default, Clone)]
pub struct DeliveryCounter {
    delivered: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
}

impl DeliveryCounter {
    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}

impl ClientContext for DeliveryCounter {}

impl ProducerContext for DeliveryCounter {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        match result {
            Ok(_) => self.delivered.fetch_add(1, Ordering::Relaxed),
            Err(_) => self.failed.fetch_add(1, Ordering::Relaxed),
        };
    }
}

pub struct KafkaSink {
    producer: BaseProducer<DeliveryCounter>,
    topic: String,
    counter: DeliveryCounter,
    // reused for every event, so serialising allocates nothing per message
    payload: Vec<u8>,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let counter = DeliveryCounter::default();
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true") // implies acks=all and safe retries
            .set("acks", "all")
            .set("linger.ms", "5") // wait up to 5 ms to fill a batch
            .set("compression.type", "lz4")
            .set("queue.buffering.max.messages", "1000000")
            .create_with_context(counter.clone())
            .with_context(|| format!("connecting producer to {brokers}"))?;
        Ok(Self {
            producer,
            topic: topic.to_owned(),
            counter,
            payload: Vec::with_capacity(512),
        })
    }

    pub fn counter(&self) -> &DeliveryCounter {
        &self.counter
    }
}

impl Sink for KafkaSink {
    fn send(&mut self, event: &Event) -> Result<()> {
        self.payload.clear();
        serde_json::to_writer(&mut self.payload, event)?;
        let key = event.src.to_be_bytes();

        loop {
            let record = BaseRecord::to(&self.topic)
                .key(&key[..])
                .payload(&self.payload[..]);
            match self.producer.send(record) {
                Ok(()) => break,
                // the local queue is full: let the client deliver some, then retry
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    self.producer.poll(Duration::from_millis(50));
                }
                Err((e, _)) => return Err(e).context("queueing event"),
            }
        }
        // serve delivery callbacks without blocking
        self.producer.poll(Duration::ZERO);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(30))
            .context("flushing producer")?;
        let failed = self.counter.failed();
        if failed > 0 {
            bail!("{failed} events were not delivered");
        }
        Ok(())
    }
}
