//! Schema migration metrics.
//!
//! The pre-rename Prometheus names had a `_total_total` double suffix because
//! the OTel names were hand-shaped in Prometheus style. The OTel names here
//! drop the `_total` suffix so the exporter can append it exactly once.

use crate::{MetricSpec, buckets};

pub mod labels {
    pub const PHASE: &str = "phase";
    pub const RESULT: &str = "result";
    pub const VERSION_BAND: &str = "version_band";
    /// Used on the `indexed_units` / `eligible_units` gauges to distinguish
    /// the SDLC promotion gate from the code backfill telemetry.
    pub const SCOPE: &str = "scope";
    /// Used on the deferred-projection telemetry to identify which
    /// migrating-version table a statement targeted.
    pub const TABLE: &str = "table";
    /// Used on the deferred-projection telemetry to distinguish the
    /// `modify_setting`, `add_projection`, and `materialize_projection`
    /// kinds — see [`compiler::PostBackfillKind`].
    pub const KIND: &str = "kind";
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
///
/// Otel name is suffix-free so the Prometheus exporter appends `_seconds`
/// itself; see [`no_banned_suffix_in_otel_name`] in the crate's tests.
pub const MIGRATING_AGE: MetricSpec = MetricSpec::gauge(
    "gkg.schema.migrating_age",
    "Seconds since the current migrating version's row was created. \
     Zero when no version is migrating.",
    Some("s"),
    &[],
    DOMAIN,
);

/// Per-statement outcome counter for the deferred-projection phase that runs
/// after a migrating version's backfill completes. The `kind` label
/// distinguishes the `modify_setting` / `add_projection` /
/// `materialize_projection` statements emitted by
/// [`compiler::generate_post_backfill_statements`].
pub const PROJECTION_APPLY: MetricSpec = MetricSpec::counter(
    "gkg.schema.projection_apply",
    "Deferred-projection statement outcomes (modify/add/materialize) \
     during the post-backfill phase, labelled by table, kind, and result.",
    None,
    &[labels::TABLE, labels::KIND, labels::RESULT],
    DOMAIN,
);

/// Wall-clock duration of each deferred-projection statement. The
/// `materialize_projection` kind is the load-bearing one for diagnosing
/// post-backfill latency; `modify_setting` and `add_projection` should be
/// near-instant and serve as a control to detect ClickHouse-side issues.
pub const PROJECTION_APPLY_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.schema.projection_apply.duration",
    "Wall-clock duration of each deferred-projection statement \
     (modify_setting, add_projection, materialize_projection).",
    Some("s"),
    &[labels::TABLE, labels::KIND],
    buckets::LATENCY_VERY_SLOW,
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &PHASE,
    &COMPLETED,
    &CLEANUP,
    &INDEXED_UNITS,
    &ELIGIBLE_UNITS,
    &MIGRATING_AGE,
    &PROJECTION_APPLY,
    &PROJECTION_APPLY_DURATION,
];
