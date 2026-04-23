//! Scheduler metrics: per-task cadence, duration, publish/skip counts, errors.

use crate::MetricSpec;
use crate::buckets::LATENCY;

pub mod labels {
    pub const TASK: &str = "task";
    pub const OUTCOME: &str = "outcome";
    pub const QUERY: &str = "query";
    pub const STAGE: &str = "stage";
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
    LATENCY,
    DOMAIN,
);

pub const ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.scheduler.task.errors",
    "Scheduled task errors, labelled by task and pipeline stage.",
    None,
    &[labels::TASK, labels::STAGE],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &RUNS,
    &DURATION,
    &REQUESTS_PUBLISHED,
    &REQUESTS_SKIPPED,
    &QUERY_DURATION,
    &ERRORS,
];
