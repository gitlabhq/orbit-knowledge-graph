use crate::MetricSpec;
use crate::buckets::{LATENCY_FAST_FINE, MEMORY_BYTES, QUERY_LATENCY, ROW_COUNT};

pub mod labels {
    pub const QUERY_TYPE: &str = "query_type";
    pub const STATUS: &str = "status";
    pub const OUTCOME: &str = "outcome";
    pub const LABEL: &str = "label";
    pub const FAILURE_REASON: &str = "failure_reason";
}

const DOMAIN: &str = "query.pipeline";

pub const QUERIES: MetricSpec = MetricSpec::counter(
    "gkg.query.pipeline.queries",
    "Total queries processed through the pipeline.",
    None,
    &[labels::QUERY_TYPE, labels::STATUS, labels::OUTCOME],
    DOMAIN,
);

pub const COMPILE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.compile.duration",
    "Time spent compiling a query from JSON to parameterised SQL.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY_FAST_FINE,
    DOMAIN,
);

pub const PIPELINE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.duration",
    "End-to-end pipeline duration from security check to formatted output.",
    Some("s"),
    &[labels::QUERY_TYPE, labels::STATUS, labels::OUTCOME],
    QUERY_LATENCY,
    DOMAIN,
);

pub const EXECUTE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.execute.duration",
    "Time spent executing the compiled query against ClickHouse.",
    Some("s"),
    &[labels::QUERY_TYPE],
    QUERY_LATENCY,
    DOMAIN,
);

pub const AUTHORIZATION_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.authorization.duration",
    "Time spent on authorization exchange with Rails.",
    Some("s"),
    &[labels::QUERY_TYPE],
    QUERY_LATENCY,
    DOMAIN,
);

pub const HYDRATION_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.query.pipeline.hydration.duration",
    "Time spent hydrating neighbour properties from ClickHouse.",
    Some("s"),
    &[labels::QUERY_TYPE],
    LATENCY_FAST_FINE,
    DOMAIN,
);

pub const RESULT_SET_ROWS: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.result_set.rows",
    "Number of rows returned after formatting.",
    None,
    &[labels::QUERY_TYPE],
    ROW_COUNT,
    DOMAIN,
);

pub const BATCHES: MetricSpec = MetricSpec::histogram_u64(
    "gkg.query.pipeline.batches",
    "Number of Arrow record batches returned from ClickHouse per query.",
    None,
    &[labels::QUERY_TYPE],
    ROW_COUNT,
    DOMAIN,
);

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
