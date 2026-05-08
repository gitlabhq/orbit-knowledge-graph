//! Query pipeline metrics: per-stage latency, ClickHouse resource consumption,
//! and error counters.
//!
//! The three `batch_count`, `redacted_count`, and `result_set_size` histograms
//! in the pre-rename code had Prometheus names ending in `_count` or ambiguous
//! `_size`, which clashed with the auto-generated histogram child series.
//! Renamed here to `batches`, `redactions`, and `result_set.rows`.

use crate::MetricSpec;
use crate::buckets::{LATENCY, MEMORY_BYTES, ROW_COUNT};

pub mod labels {
    pub const QUERY_TYPE: &str = "query_type";
    pub const STATUS: &str = "status";
    pub const LABEL: &str = "label";
    pub const FAILURE_REASON: &str = "failure_reason";
}

const DOMAIN: &str = "query.pipeline";

pub const QUERIES: MetricSpec = MetricSpec::counter(
    "gkg.query.pipeline.queries",
    "Total queries processed through the pipeline.",
    None,
    &[labels::QUERY_TYPE, labels::STATUS],
    DOMAIN,
);

pub const COMPILE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.compile.duration",
    "Time spent compiling a query from JSON to parameterised SQL.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY,
    DOMAIN,
);

pub const PIPELINE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.duration",
    "End-to-end pipeline duration from security check to formatted output.",
    Some("s"),
    &[labels::QUERY_TYPE, labels::STATUS],
    LATENCY,
    DOMAIN,
);

pub const EXECUTE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.execute.duration",
    "Time spent executing the compiled query against ClickHouse.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY,
    DOMAIN,
);

pub const AUTHORIZATION_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.authorization.duration",
    "Time spent on authorization exchange with Rails.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY,
    DOMAIN,
);

pub const HYDRATION_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.hydration.duration",
    "Time spent hydrating neighbour properties from ClickHouse.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY,
    DOMAIN,
);

// Renamed from `gkg.query.pipeline.result_set.size` to avoid the unitless
// "size" naming. Now a bucketed row count with an explicit `rows` token.
pub const RESULT_SET_ROWS: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.result_set.rows",
    "Number of rows returned after formatting.",
    None,
    &[labels::QUERY_TYPE],
    ROW_COUNT,
    DOMAIN,
);

// Renamed from `gkg.query.pipeline.batch.count` to avoid colliding with the
// auto-generated `_count` child series of Prometheus histograms.
pub const BATCHES: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.batches",
    "Number of Arrow record batches returned from ClickHouse per query.",
    None,
    &[labels::QUERY_TYPE],
    ROW_COUNT,
    DOMAIN,
);

// Renamed from `gkg.query.pipeline.redacted.count` for the same reason.
pub const REDACTIONS: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.redactions",
    "Number of rows redacted per query.",
    None,
    &[labels::QUERY_TYPE],
    ROW_COUNT,
    DOMAIN,
);

pub const CH_READ_ROWS: MetricSpec = MetricSpec::counter(
    "gkg.query.pipeline.ch.read_rows",
    "ClickHouse rows read across all queries in the pipeline.",
    None,
    &[labels::QUERY_TYPE, labels::LABEL],
    DOMAIN,
);

// Dropped `_bytes` from the OTel name; the `By` unit already maps to the
// `_bytes` Prometheus suffix, previously producing `_bytes_bytes_total`.
pub const CH_READ_BYTES: MetricSpec = MetricSpec::counter(
    "gkg.query.pipeline.ch.read",
    "ClickHouse bytes read across all queries in the pipeline.",
    Some("By"),
    &[labels::QUERY_TYPE, labels::LABEL],
    DOMAIN,
);

pub const CH_MEMORY_USAGE: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.ch.memory_usage",
    "ClickHouse peak memory usage per query execution.",
    Some("By"),
    &[labels::QUERY_TYPE, labels::LABEL],
    MEMORY_BYTES,
    DOMAIN,
);

/// Counter for every query that failed during pipeline execution (post-compile).
/// `failure_reason` is a closed enum derived from `PipelineError::code()` with
/// the `_error` suffix stripped: `security`, `execution`, `authorization`,
/// `content_resolution`, `streaming`, `custom`. Compile-time rejections are
/// counted separately under `gkg.query.engine.compiler.rejected`.
pub const FAILED: MetricSpec = MetricSpec::counter(
    "gkg.query.pipeline.failed",
    "Total queries that failed in the pipeline post-compile, labelled by failure reason.",
    None,
    &[labels::FAILURE_REASON],
    DOMAIN,
);

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
    &FAILED,
];
