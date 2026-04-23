//! Schema migration metrics.
//!
//! The pre-rename Prometheus names had a `_total_total` double suffix because
//! the OTel names were hand-shaped in Prometheus style. The OTel names here
//! drop the `_total` suffix so the exporter can append it exactly once.

use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const PHASE: &str = "phase";
    pub const RESULT: &str = "result";
    pub const VERSION_BAND: &str = "version_band";
}

const DOMAIN: &str = "indexer.migration";

pub const PHASE: MetricSpec = MetricSpec {
    otel_name: "gkg.schema.migration.phase",
    description: "Total schema migration phase executions, labelled by phase and result.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::PHASE, labels::RESULT],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const COMPLETED: MetricSpec = MetricSpec {
    otel_name: "gkg.schema.migration.completed",
    description: "Total successful schema migration completions.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CLEANUP: MetricSpec = MetricSpec {
    otel_name: "gkg.schema.cleanup",
    description: "Schema table cleanup operations, labelled by version band and result.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::VERSION_BAND, labels::RESULT],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[&PHASE, &COMPLETED, &CLEANUP];
