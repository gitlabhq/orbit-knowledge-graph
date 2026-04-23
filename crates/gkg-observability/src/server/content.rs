//! Content resolution metrics: Gitaly latency, batch sizing, blob sizes.

use crate::buckets::{BATCH_SIZE, BLOB_BYTES, LATENCY_FAST};
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const OUTCOME: &str = "outcome";
}

const DOMAIN: &str = "server.content";

pub const RESOLVE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.content.resolve.duration",
    description: "Time spent resolving content from Gitaly.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::OUTCOME],
    buckets: Some(LATENCY_FAST),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const RESOLVE_TOTAL: MetricSpec = MetricSpec {
    otel_name: "gkg.content.resolve",
    description: "Total content resolution attempts.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const BATCH_SIZE_METRIC: MetricSpec = MetricSpec {
    otel_name: "gkg.content.resolve.batch_size",
    description: "Number of rows per content resolution batch.",
    kind: MetricKind::HistogramU64,
    unit: None,
    labels: &[],
    buckets: Some(BATCH_SIZE),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const BLOB_BYTES_METRIC: MetricSpec = MetricSpec {
    otel_name: "gkg.content.blob.size",
    description: "Size of resolved blob content in bytes.",
    kind: MetricKind::HistogramU64,
    unit: Some("By"),
    labels: &[],
    buckets: Some(BLOB_BYTES),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const GITALY_CALLS: MetricSpec = MetricSpec {
    otel_name: "gkg.content.gitaly.calls",
    description: "Total list_blobs RPCs issued to Gitaly.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &RESOLVE_DURATION,
    &RESOLVE_TOTAL,
    &BATCH_SIZE_METRIC,
    &BLOB_BYTES_METRIC,
    &GITALY_CALLS,
];
