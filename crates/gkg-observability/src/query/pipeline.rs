//! Query pipeline metrics: per-stage latency, ClickHouse resource consumption,
//! and error counters.
//!
//! The three `batch_count`, `redacted_count`, and `result_set_size` histograms
//! in the pre-rename code had Prometheus names ending in `_count` or ambiguous
//! `_size`, which clashed with the auto-generated histogram child series.
//! Renamed here to `batches`, `redactions`, and `result_set.rows`.

use crate::buckets::{LATENCY, MEMORY_BYTES, ROW_COUNT};
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const QUERY_TYPE: &str = "query_type";
    pub const STATUS: &str = "status";
    pub const LABEL: &str = "label";
    pub const REASON: &str = "reason";
}

const DOMAIN: &str = "query.pipeline";

pub const QUERIES: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.queries",
    description: "Total queries processed through the pipeline.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::QUERY_TYPE, labels::STATUS],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const COMPILE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.compile.duration",
    description: "Time spent compiling a query from JSON to parameterised SQL.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY_TYPE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const PIPELINE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.duration",
    description: "End-to-end pipeline duration from security check to formatted output.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY_TYPE, labels::STATUS],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const EXECUTE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.execute.duration",
    description: "Time spent executing the compiled query against ClickHouse.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY_TYPE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const AUTHORIZATION_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.authorization.duration",
    description: "Time spent on authorization exchange with Rails.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY_TYPE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const HYDRATION_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.hydration.duration",
    description: "Time spent hydrating neighbour properties from ClickHouse.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY_TYPE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Renamed from `gkg.query.pipeline.result_set.size` to avoid the unitless
// "size" naming. Now a bucketed row count with an explicit `rows` token.
pub const RESULT_SET_ROWS: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.result_set.rows",
    description: "Number of rows returned after formatting.",
    kind: MetricKind::HistogramU64,
    unit: None,
    labels: &[labels::QUERY_TYPE],
    buckets: Some(ROW_COUNT),
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Renamed from `gkg.query.pipeline.batch.count` to avoid colliding with the
// auto-generated `_count` child series of Prometheus histograms.
pub const BATCHES: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.batches",
    description: "Number of Arrow record batches returned from ClickHouse per query.",
    kind: MetricKind::HistogramU64,
    unit: None,
    labels: &[labels::QUERY_TYPE],
    buckets: Some(ROW_COUNT),
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Renamed from `gkg.query.pipeline.redacted.count` for the same reason.
pub const REDACTIONS: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.redactions",
    description: "Number of rows redacted per query.",
    kind: MetricKind::HistogramU64,
    unit: None,
    labels: &[labels::QUERY_TYPE],
    buckets: Some(ROW_COUNT),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CH_READ_ROWS: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.ch.read_rows",
    description: "ClickHouse rows read across all queries in the pipeline.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::QUERY_TYPE, labels::LABEL],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Dropped `_bytes` from the OTel name; the `By` unit already maps to the
// `_bytes` Prometheus suffix, previously producing `_bytes_bytes_total`.
pub const CH_READ_BYTES: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.ch.read",
    description: "ClickHouse bytes read across all queries in the pipeline.",
    kind: MetricKind::Counter,
    unit: Some("By"),
    labels: &[labels::QUERY_TYPE, labels::LABEL],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CH_MEMORY_USAGE: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.ch.memory_usage",
    description: "ClickHouse peak memory usage per query execution.",
    kind: MetricKind::HistogramU64,
    unit: Some("By"),
    labels: &[labels::QUERY_TYPE, labels::LABEL],
    buckets: Some(MEMORY_BYTES),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERROR_SECURITY_REJECTED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.error.security_rejected",
    description: "Pipeline rejected due to invalid or missing security context.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERROR_EXECUTION_FAILED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.error.execution_failed",
    description: "ClickHouse query execution failed.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERROR_AUTHORIZATION_FAILED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.error.authorization_failed",
    description: "Authorization exchange with Rails failed.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERROR_CONTENT_RESOLUTION_FAILED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.error.content_resolution_failed",
    description: "Virtual column resolution from remote service failed.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERROR_STREAMING_FAILED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.pipeline.error.streaming_failed",
    description: "Streaming channel unavailable during authorization.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &QUERIES,
    &COMPILE_DURATION,
    &PIPELINE_DURATION,
    &EXECUTE_DURATION,
    &AUTHORIZATION_DURATION,
    &HYDRATION_DURATION,
    &RESULT_SET_ROWS,
    &BATCHES,
    &REDACTIONS,
    &CH_READ_ROWS,
    &CH_READ_BYTES,
    &CH_MEMORY_USAGE,
    &ERROR_SECURITY_REJECTED,
    &ERROR_EXECUTION_FAILED,
    &ERROR_AUTHORIZATION_FAILED,
    &ERROR_CONTENT_RESOLUTION_FAILED,
    &ERROR_STREAMING_FAILED,
];
