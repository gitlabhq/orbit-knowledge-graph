//! Metrics emitted by the ETL engine (NATS dispatch, handler execution,
//! worker pool, ClickHouse writes).

use crate::buckets::LATENCY;
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const TOPIC: &str = "topic";
    pub const OUTCOME: &str = "outcome";
    pub const HANDLER: &str = "handler";
    pub const ERROR_KIND: &str = "error_kind";
    pub const TABLE: &str = "table";
}

const DOMAIN: &str = "indexer.etl";

pub const MESSAGES_PROCESSED: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.messages.processed",
    description: "ETL messages processed by the indexer, labelled by topic and outcome.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TOPIC, labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const MESSAGE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.message.duration",
    description: "End-to-end time per message through dispatch.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::TOPIC],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const HANDLER_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.handler.duration",
    description: "Time inside each handler's handle() call.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::HANDLER],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const PERMIT_WAIT_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.permit.wait.duration",
    description: "Time waiting for a worker pool permit.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ACTIVE_PERMITS: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.permits.active",
    description: "Number of worker permits currently held.",
    kind: MetricKind::UpDownCounter,
    unit: None,
    labels: &[],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const NATS_FETCH_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.nats.fetch.duration",
    description: "Time to fetch a batch of messages from NATS.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::OUTCOME],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const DESTINATION_WRITE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.destination.write.duration",
    description: "Time to write a batch to ClickHouse.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::TABLE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const DESTINATION_ROWS_WRITTEN: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.destination.rows.written",
    description: "Total rows written to ClickHouse per table.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TABLE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Drop the `.bytes` token from the OTel name: the `By` unit already maps to
// the `_bytes` Prometheus suffix, and `bytes_bytes_total` was a double suffix
// in the current Prometheus exposure.
pub const DESTINATION_BYTES_WRITTEN: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.destination.written",
    description: "Total bytes written to ClickHouse per table.",
    kind: MetricKind::Counter,
    unit: Some("By"),
    labels: &[labels::TABLE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const DESTINATION_WRITE_ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.destination.write.errors",
    description: "Total failed writes to ClickHouse per table.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TABLE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const HANDLER_ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.etl.handler.errors",
    description: "Total handler errors at the engine dispatch level.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::HANDLER, labels::ERROR_KIND],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &MESSAGES_PROCESSED,
    &MESSAGE_DURATION,
    &HANDLER_DURATION,
    &PERMIT_WAIT_DURATION,
    &ACTIVE_PERMITS,
    &NATS_FETCH_DURATION,
    &DESTINATION_WRITE_DURATION,
    &DESTINATION_ROWS_WRITTEN,
    &DESTINATION_BYTES_WRITTEN,
    &DESTINATION_WRITE_ERRORS,
    &HANDLER_ERRORS,
];
