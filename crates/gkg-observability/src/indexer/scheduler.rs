//! Scheduler metrics: per-task cadence, duration, publish/skip counts, errors,
//! and dirty-namespace change-detection observability.

use crate::MetricSpec;
use crate::buckets::{LATENCY, LATENCY_FAST_FINE, ROW_COUNT};

pub mod labels {
    pub const TASK: &str = "task";
    pub const OUTCOME: &str = "outcome";
    pub const QUERY: &str = "query";
    pub const STAGE: &str = "stage";
    pub const TABLE: &str = "table";
}

const DOMAIN: &str = "indexer.scheduler";

pub const RUNS: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.task.runs",
    "Total scheduled task runs, labelled by task and outcome.",
    None,
    &[labels::TASK, labels::OUTCOME],
    DOMAIN,
);

pub const DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.scheduler.task.duration",
    "End-to-end duration of a scheduled task run.",
    Some("s"),
    &[labels::TASK],
    LATENCY,
    DOMAIN,
);

pub const REQUESTS_PUBLISHED: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.task.requests.published",
    "Requests successfully published to NATS per task.",
    None,
    &[labels::TASK],
    DOMAIN,
);

pub const REQUESTS_SKIPPED: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.task.requests.skipped",
    "Requests skipped because an equivalent request was already in flight.",
    None,
    &[labels::TASK],
    DOMAIN,
);

pub const QUERY_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.scheduler.task.query.duration",
    "Duration of a scheduled task's ClickHouse query.",
    Some("s"),
    &[labels::QUERY],
    LATENCY_FAST_FINE,
    DOMAIN,
);

pub const ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.task.errors",
    "Scheduled task errors, labelled by task and pipeline stage.",
    None,
    &[labels::TASK, labels::STAGE],
    DOMAIN,
);

// ── Dirty-namespace change-detection metrics ─────────────────────────

pub const DIRTY_NAMESPACES: MetricSpec = MetricSpec::histogram_f64(
    "gkg.scheduler.dirty_detection.namespaces",
    "Number of distinct dirty namespaces found per dispatch cycle.",
    None,
    &[],
    ROW_COUNT,
    DOMAIN,
);

pub const DIRTY_DETECTION_QUERY_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.scheduler.dirty_detection.query.duration",
    "Per-table change-detection query duration.",
    Some("s"),
    &[labels::TABLE],
    LATENCY_FAST_FINE,
    DOMAIN,
);

pub const DIRTY_DETECTION_READ_ROWS: MetricSpec = MetricSpec::histogram_f64(
    "gkg.scheduler.dirty_detection.query.read_rows",
    "Rows read by the per-table change-detection query.",
    None,
    &[labels::TABLE],
    ROW_COUNT,
    DOMAIN,
);

pub const SWEEP_ONLY_DISPATCHED: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.dirty_detection.sweep_only_dispatched",
    "Namespaces dispatched only because of the full sweep, not seen by dirty-detection. A sustained non-zero value signals dirty-detection is under-reporting.",
    None,
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &RUNS,
    &DURATION,
    &REQUESTS_PUBLISHED,
    &REQUESTS_SKIPPED,
    &QUERY_DURATION,
    &ERRORS,
    &DIRTY_NAMESPACES,
    &DIRTY_DETECTION_QUERY_DURATION,
    &DIRTY_DETECTION_READ_ROWS,
    &SWEEP_ONLY_DISPATCHED,
];
