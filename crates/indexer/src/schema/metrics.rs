//! OTel instrument holders for schema migration, backed by the central
//! `gkg-observability` catalog.
//!
//! Names, descriptions, units, labels, and histogram buckets live in
//! `crates/gkg-observability/src/indexer/migration.rs`. This module only
//! builds instruments against the running `MeterProvider`.

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge};

use gkg_observability::indexer::migration;

/// Schema-migration counter with labels `phase` and `result`.
///
/// Prometheus exposes this as `gkg_schema_migration_phase_total` after the
/// rename from the former `gkg_schema_migration_total_total` double suffix.
#[derive(Clone)]
pub struct MigrationMetrics {
    pub(crate) phase: Counter<u64>,
}

impl MigrationMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            phase: migration::PHASE.build_counter_u64(&meter),
        }
    }

    pub(crate) fn record(&self, phase: &'static str, result: &'static str) {
        self.phase.add(
            1,
            &[
                KeyValue::new(migration::labels::PHASE, phase),
                KeyValue::new(migration::labels::RESULT, result),
            ],
        );
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration completion metrics: successful finishes and per-version cleanup.
///
/// The `version_band` label buckets the historical version integer into
/// `current`, `previous`, or `ancient` to cap cardinality as schema history
/// grows. Prometheus exposes these as `gkg_schema_migration_completed_total`
/// and `gkg_schema_cleanup_total` after the rename that dropped the former
/// `_total_total` double suffix.
#[derive(Clone)]
pub struct CompletionMetrics {
    pub(crate) migration_completed: Counter<u64>,
    pub(crate) cleanup: Counter<u64>,
    pub(crate) indexed_units: Gauge<f64>,
    pub(crate) eligible_units: Gauge<f64>,
    pub(crate) migrating_age: Gauge<f64>,
}

impl CompletionMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            migration_completed: migration::COMPLETED.build_counter_u64(&meter),
            cleanup: migration::CLEANUP.build_counter_u64(&meter),
            indexed_units: migration::INDEXED_UNITS.build_gauge_f64(&meter),
            eligible_units: migration::ELIGIBLE_UNITS.build_gauge_f64(&meter),
            migrating_age: migration::MIGRATING_AGE.build_gauge_f64(&meter),
        }
    }

    pub(crate) fn record_migration_completed(&self) {
        self.migration_completed.add(1, &[]);
    }

    pub(crate) fn record_cleanup(&self, version: u32, current: u32, result: &'static str) {
        let band = version_band(version, current);
        self.cleanup.add(
            1,
            &[
                KeyValue::new(migration::labels::VERSION_BAND, band),
                KeyValue::new(migration::labels::RESULT, result),
            ],
        );
    }

    /// Records both gauges for a single (scope, version) at once. `version`
    /// is the migrating version we just measured against, used to derive the
    /// `version_band` label so dashboards don't have to know version numbers.
    pub(crate) fn record_units(
        &self,
        scope: &'static str,
        version: u32,
        current: u32,
        indexed: u64,
        eligible: u64,
    ) {
        let band = version_band(version, current);
        let attrs = [
            KeyValue::new(migration::labels::SCOPE, scope),
            KeyValue::new(migration::labels::VERSION_BAND, band),
        ];
        self.indexed_units.record(indexed as f64, &attrs);
        self.eligible_units.record(eligible as f64, &attrs);
    }

    pub(crate) fn record_migrating_age(&self, age_seconds: u64) {
        self.migrating_age.record(age_seconds as f64, &[]);
    }
}

impl Default for CompletionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn version_band(version: u32, current: u32) -> &'static str {
    match current.saturating_sub(version) {
        0 => "current",
        1 => "previous",
        _ => "ancient",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_band_buckets() {
        assert_eq!(version_band(10, 10), "current");
        assert_eq!(version_band(9, 10), "previous");
        assert_eq!(version_band(1, 10), "ancient");
        assert_eq!(version_band(11, 10), "current");
    }
}
