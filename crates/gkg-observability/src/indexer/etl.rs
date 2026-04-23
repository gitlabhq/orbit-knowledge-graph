//! Metrics emitted by the ETL engine (NATS dispatch, handler execution,
//! worker pool, ClickHouse writes).

use crate::MetricSpec;
use crate::buckets::LATENCY;

pub mod labels {
    pub const TOPIC: &str = "topic";
    pub const OUTCOME: &str = "outcome";
    pub const HANDLER: &str = "handler";
    pub const ERROR_KIND: &str = "error_kind";
    pub const TABLE: &str = "table";
}

const DOMAIN: &str = "indexer.etl";

pub const MESSAGES_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.etl.messages.processed",
    "ETL messages processed by the indexer, labelled by topic and outcome.",
    None,
    &[labels::TOPIC, labels::OUTCOME],
    DOMAIN,
);

pub const MESSAGE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.etl.message.duration",
    "End-to-end time per message through dispatch.",
    Some("s"),
    &[labels::TOPIC],
    LATENCY,
    DOMAIN,
);

pub const HANDLER_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.etl.handler.duration",
    "Time inside each handler's handle() call.",
    Some("s"),
    &[labels::HANDLER],
    LATENCY,
    DOMAIN,
);

pub const PERMIT_WAIT_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.etl.permit.wait.duration",
    "Time waiting for a worker pool permit.",
    Some("s"),
    &[],
    LATENCY,
    DOMAIN,
);

pub const ACTIVE_PERMITS: MetricSpec = MetricSpec::up_down_counter(
    "gkg.etl.permits.active",
    "Number of worker permits currently held.",
    None,
    &[],
    DOMAIN,
);

pub const NATS_FETCH_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.etl.nats.fetch.duration",
    "Time to fetch a batch of messages from NATS.",
    Some("s"),
    &[labels::OUTCOME],
    LATENCY,
    DOMAIN,
);

pub const DESTINATION_WRITE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.etl.destination.write.duration",
    "Time to write a batch to ClickHouse.",
    Some("s"),
    &[labels::TABLE],
    LATENCY,
    DOMAIN,
);

pub const DESTINATION_ROWS_WRITTEN: MetricSpec = MetricSpec::counter(
    "gkg.etl.destination.rows.written",
    "Total rows written to ClickHouse per table.",
    None,
    &[labels::TABLE],
    DOMAIN,
);

// Drop the `.bytes` token from the OTel name: the `By` unit already maps to
// the `_bytes` Prometheus suffix, and `bytes_bytes_total` was a double suffix
// in the current Prometheus exposure.
pub const DESTINATION_BYTES_WRITTEN: MetricSpec = MetricSpec::counter(
    "gkg.etl.destination.written",
    "Total bytes written to ClickHouse per table.",
    Some("By"),
    &[labels::TABLE],
    DOMAIN,
);

pub const DESTINATION_WRITE_ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.etl.destination.write.errors",
    "Total failed writes to ClickHouse per table.",
    None,
    &[labels::TABLE],
    DOMAIN,
);

pub const HANDLER_ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.etl.handler.errors",
    "Total handler errors at the engine dispatch level.",
    None,
    &[labels::HANDLER, labels::ERROR_KIND],
    DOMAIN,
);

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
