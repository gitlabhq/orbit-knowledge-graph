//! Scheduler metrics: per-task cadence, duration, publish/skip counts, errors.

use crate::buckets::LATENCY;
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const TASK: &str = "task";
    pub const OUTCOME: &str = "outcome";
    pub const QUERY: &str = "query";
    pub const STAGE: &str = "stage";
}

const DOMAIN: &str = "indexer.scheduler";

pub const RUNS: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.runs",
    description: "Total scheduled task runs, labelled by task and outcome.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TASK, labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.duration",
    description: "End-to-end duration of a scheduled task run.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::TASK],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REQUESTS_PUBLISHED: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.requests.published",
    description: "Requests successfully published to NATS per task.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TASK],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REQUESTS_SKIPPED: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.requests.skipped",
    description: "Requests skipped because an equivalent request was already in flight.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TASK],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const QUERY_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.query.duration",
    description: "Duration of a scheduled task's ClickHouse query.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::QUERY],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.scheduler.task.errors",
    description: "Scheduled task errors, labelled by task and pipeline stage.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TASK, labels::STAGE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &RUNS,
    &DURATION,
    &REQUESTS_PUBLISHED,
    &REQUESTS_SKIPPED,
    &QUERY_DURATION,
    &ERRORS,
];
