//! Schema migration metrics.
//!
//! The pre-rename Prometheus names had a `_total_total` double suffix because
//! the OTel names were hand-shaped in Prometheus style. The OTel names here
//! drop the `_total` suffix so the exporter can append it exactly once.

use crate::MetricSpec;

pub mod labels {
    pub const PHASE: &str = "phase";
    pub const RESULT: &str = "result";
    pub const VERSION_BAND: &str = "version_band";
}

const DOMAIN: &str = "indexer.migration";

pub const PHASE: MetricSpec = MetricSpec::counter(
    "gkg.schema.migration.phase",
    "Total schema migration phase executions, labelled by phase and result.",
    None,
    &[labels::PHASE, labels::RESULT],
    DOMAIN,
);

pub const COMPLETED: MetricSpec = MetricSpec::counter(
    "gkg.schema.migration.completed",
    "Total successful schema migration completions.",
    None,
    &[],
    DOMAIN,
);

pub const CLEANUP: MetricSpec = MetricSpec::counter(
    "gkg.schema.cleanup",
    "Schema table cleanup operations, labelled by version band and result.",
    None,
    &[labels::VERSION_BAND, labels::RESULT],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&PHASE, &COMPLETED, &CLEANUP];
