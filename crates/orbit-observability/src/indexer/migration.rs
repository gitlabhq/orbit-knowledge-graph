use crate::MetricSpec;

pub mod labels {
    pub const PHASE: &str = "phase";
    pub const RESULT: &str = "result";
    pub const VERSION_BAND: &str = "version_band";
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

pub const INDEXED_UNITS: MetricSpec = MetricSpec::gauge(
    "gkg.schema.indexed_units",
    "Distinct units with a checkpoint on the migrating schema version. \
     Numerator of the migration-readiness ratio.",
    None,
    &[labels::SCOPE, labels::VERSION_BAND],
    DOMAIN,
);

pub const ELIGIBLE_UNITS: MetricSpec = MetricSpec::gauge(
    "gkg.schema.eligible_units",
    "Distinct units under enabled namespaces. \
     Denominator of the migration-readiness ratio.",
    None,
    &[labels::SCOPE, labels::VERSION_BAND],
    DOMAIN,
);

pub const MIGRATING_AGE: MetricSpec = MetricSpec::gauge(
    "gkg.schema.migrating_age",
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
