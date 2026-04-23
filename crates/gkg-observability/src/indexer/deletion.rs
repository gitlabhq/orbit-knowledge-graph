//! Namespace-deletion pipeline metrics.

use crate::buckets::LATENCY;
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const TABLE: &str = "table";
}

const DOMAIN: &str = "indexer.namespace_deletion";

pub const TABLE_DELETION_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.namespace_deletion.table.duration",
    description: "Duration of a single table's soft-delete INSERT-SELECT.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::TABLE],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const TABLE_DELETION_ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.namespace_deletion.table.errors",
    description: "Total per-table deletion failures.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::TABLE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[&TABLE_DELETION_DURATION, &TABLE_DELETION_ERRORS];
