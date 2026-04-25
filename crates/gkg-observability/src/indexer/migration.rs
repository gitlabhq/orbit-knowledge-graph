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
    /// Used on the `indexed_units` / `eligible_units` gauges to distinguish
    /// the SDLC promotion gate from the code backfill telemetry.
    pub const SCOPE: &str = "scope";
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

/// Numerator of the migration-readiness ratio. Distinct units
/// (namespaces for `scope=sdlc`, projects for `scope=code`) that have a
/// checkpoint row on the migrating version.
pub const INDEXED_UNITS: MetricSpec = MetricSpec::gauge(
    "gkg.schema.indexed_units",
    "Distinct units with a checkpoint on the migrating schema version. \
     Numerator of the migration-readiness ratio.",
    None,
    &[labels::SCOPE, labels::VERSION_BAND],
    DOMAIN,
);

/// Denominator of the migration-readiness ratio. Distinct units under
/// currently-enabled namespaces (namespaces for `scope=sdlc`, projects for
/// `scope=code`).
pub const ELIGIBLE_UNITS: MetricSpec = MetricSpec::gauge(
    "gkg.schema.eligible_units",
    "Distinct units under enabled namespaces. \
     Denominator of the migration-readiness ratio.",
    None,
    &[labels::SCOPE, labels::VERSION_BAND],
    DOMAIN,
);

/// Wall-clock seconds since the current migrating version's row was created.
/// Zero when no version is migrating. Direct alert target for stuck migrations.
pub const MIGRATING_AGE: MetricSpec = MetricSpec::gauge(
    "gkg.schema.migrating_age_seconds",
    "Seconds since the current migrating version's row was created. \
     Zero when no version is migrating.",
    Some("s"),
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &PHASE,
    &COMPLETED,
    &CLEANUP,
    &INDEXED_UNITS,
    &ELIGIBLE_UNITS,
    &MIGRATING_AGE,
];
