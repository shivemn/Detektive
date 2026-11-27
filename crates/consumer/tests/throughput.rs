//! Rough throughput on the mock cluster, using real HI-Small transactions.
//! Ignored by default; run with an absolute path:
//! `DETEKTIVE_DATA=$PWD/data/raw/HI-Small_Trans.csv cargo test --release -p detektive-consumer --test throughput -- --ignored --nocapture`
//! The mock broker is not Redpanda and keeps only a limited amount of data
//! per partition, so this uses 250,000 events. Treat the numbers as a
//! client-side sanity check, not a broker benchmark.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::mocking::MockCluster;

use detektive_consumer::metrics::Metrics;
use detektive_consumer::{Config, run};
use detektive_replay::event::EventOptions;
use detektive_replay::kafka::KafkaSink;
use detektive_replay::pace::Speed;
use detektive_replay::{ReplayOptions, load, replay};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn events_through_the_mock_cluster() {
    let path = std::env::var("DETEKTIVE_DATA").expect("set DETEKTIVE_DATA to an IBM *_Trans.csv");
    // the mock broker keeps only about 100k of these per partition and deletes
    // the oldest beyond that, so the consumer is checked against what it kept
    let n = 250_000;
    let data = load::load_csv(path.as_ref()).unwrap();

    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("payments", 1, 1).unwrap();
    let brokers = cluster.bootstrap_servers();

    let mut sink = KafkaSink::new(&brokers, "payments").unwrap();
    let opts = ReplayOptions {
        speed: Speed::Max,
        limit: Some(n),
        event: EventOptions::default(),
    };
    let started = Instant::now();
    replay(&data.transactions, opts, &mut sink).unwrap();
    let produce = started.elapsed();
    eprintln!(
        "produced {n} in {:.2}s ({:.0}/s)",
        produce.as_secs_f64(),
        n as f64 / produce.as_secs_f64()
    );

    let probe: rdkafka::consumer::BaseConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let (low, high) = rdkafka::consumer::Consumer::fetch_watermarks(
        &probe,
        "payments",
        0,
        Duration::from_secs(5),
    )
    .unwrap();
    let retained = (high - low) as u64;
    eprintln!("mock broker retained offsets {low}..{high} ({retained} events)");

    let cfg = Config {
        brokers,
        topic: "payments".into(),
        group: "bench".into(),
        dead_letter_topic: None,
        stop_after: Some(retained),
        idle_timeout: Some(Duration::from_secs(15)),
        report_every: Duration::from_secs(600),
        graph: Default::default(),
        model: None,
        alerts_topic: None,
    };
    let summary = run(
        cfg,
        Arc::new(Metrics::new().unwrap()),
        std::future::pending(),
    )
    .await
    .unwrap();
    // everything the broker still holds must arrive, once
    assert_eq!(summary.processed, retained);
    eprintln!(
        "consumed {} in {:.2}s ({:.0}/s, first to last message)",
        summary.processed,
        summary.busy.as_secs_f64(),
        summary.per_second()
    );
}
