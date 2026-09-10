use crate::MetricSpec;
use crate::buckets::{BATCH_SIZE, BLOB_BYTES, LATENCY_FAST};

pub mod labels {
    pub const OUTCOME: &str = "outcome";
    pub const ENDPOINT: &str = "endpoint";
    pub const TRANSPORT: &str = "transport";
}

const DOMAIN: &str = "server.content";

pub const RESOLVE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.content.resolve.duration",
    "Time spent resolving content from Gitaly.",
    Some("s"),
    &[labels::OUTCOME],
    LATENCY_FAST,
    DOMAIN,
);

pub const RESOLVE_TOTAL: MetricSpec = MetricSpec::counter(
    "gkg.content.resolve",
    "Total content resolution attempts.",
    None,
    &[labels::OUTCOME],
    DOMAIN,
);

pub const BATCH_SIZE_METRIC: MetricSpec = MetricSpec::histogram_u64(
    "gkg.content.resolve.batch_size",
    "Number of rows per content resolution batch.",
    None,
    &[],
    BATCH_SIZE,
    DOMAIN,
);

pub const BLOB_BYTES_METRIC: MetricSpec = MetricSpec::histogram_u64(
    "gkg.content.blob.size",
    "Size of resolved blob content in bytes.",
    Some("By"),
    &[],
    BLOB_BYTES,
    DOMAIN,
);

pub const GITALY_CALLS: MetricSpec = MetricSpec::counter(
    "gkg.content.gitaly.calls",
    "Total list_blobs RPCs issued to Gitaly.",
    None,
    &[],
    DOMAIN,
);

pub const GITALY_TRANSPORT_CALLS: MetricSpec = MetricSpec::counter(
    "gkg.content.gitaly.transport.calls",
    "Logical blob calls by selected transport and final outcome; fallback records the proxy attempt.",
    None,
    &[labels::TRANSPORT, labels::OUTCOME],
    DOMAIN,
);

pub const MR_DIFF_CALLS: MetricSpec = MetricSpec::counter(
    "gkg.content.mr_diff.calls",
    "Total HTTP calls issued to the merge_request_diffs internal API.",
    None,
    &[labels::ENDPOINT],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &RESOLVE_DURATION,
    &RESOLVE_TOTAL,
    &BATCH_SIZE_METRIC,
    &BLOB_BYTES_METRIC,
    &GITALY_CALLS,
    &GITALY_TRANSPORT_CALLS,
    &MR_DIFF_CALLS,
];
