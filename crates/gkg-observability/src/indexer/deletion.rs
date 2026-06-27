use crate::MetricSpec;
use crate::buckets::LATENCY;

const DOMAIN: &str = "indexer.namespace_deletion";

pub const TABLE_DELETION_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.namespace_deletion.table.duration",
    "Duration of a single table's soft-delete INSERT-SELECT.",
    Some("s"),
    &[],
    LATENCY,
    DOMAIN,
);

pub const TABLE_DELETION_ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.indexer.namespace_deletion.table.errors",
    "Total table deletion failures during namespace deletion.",
    None,
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&TABLE_DELETION_DURATION, &TABLE_DELETION_ERRORS];
