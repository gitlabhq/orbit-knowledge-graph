use crate::MetricSpec;
use crate::buckets::LATENCY_FAST;

pub mod labels {
    pub const DECISION: &str = "decision";
    pub const CACHE: &str = "cache";
    pub const OUTCOME: &str = "outcome";
    pub const SOURCE_TYPE: &str = "source_type";
}

pub mod values {
    pub const ALLOW: &str = "allow";
    pub const DENY: &str = "deny";
    pub const FAIL_CLOSED: &str = "fail_closed";
    pub const FAIL_OPEN: &str = "fail_open";
    pub const ERROR: &str = "error";
    pub const SKIPPED: &str = "skipped";
    pub const HIT: &str = "hit";
    pub const MISS: &str = "miss";
}

const DOMAIN: &str = "billing.quota";

pub const QUOTA_DECISIONS: MetricSpec = MetricSpec::counter(
    "gkg.billing.quota.decisions",
    "Quota gate decisions, labelled by outcome (allow/deny/fail_open/fail_closed/skipped), cache result \
     (hit/miss), and source_type (mcp/rest). \
     cache=miss on fail_closed does not imply a 1:1 CDot call ratio — see \
     gkg.billing.quota.cdot.duration for actual upstream call counts.",
    None,
    &[labels::DECISION, labels::CACHE, labels::SOURCE_TYPE],
    DOMAIN,
);

pub const QUOTA_CDOT_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.billing.quota.cdot.duration",
    "Latency of upstream CustomersDot quota checks, labelled by outcome \
     (allow/deny/error). Recorded once per check that reaches CustomersDot, \
     including its retry; concurrent cache-miss coalescing means this count is \
     lower than the decisions{cache=miss} counter under load.",
    Some("s"),
    &[labels::OUTCOME],
    LATENCY_FAST,
    DOMAIN,
);

pub const QUOTA_BYPASSED: MetricSpec = MetricSpec::counter(
    "gkg.billing.quota.bypassed",
    "Requests that bypassed the quota gate because their source_type is not in the \
     metered set (mcp/rest), labelled by source_type.",
    None,
    &[labels::SOURCE_TYPE],
    DOMAIN,
);

pub const QUOTA_CACHE_ENTRIES: MetricSpec = MetricSpec::observable_gauge(
    "gkg.billing.quota.cache.entries",
    "Current number of entries in the per-pod quota decision cache. \
     Approaches the QUOTA_MAX_CACHE_ENTRIES ceiling under sustained load from many \
     distinct namespaces.",
    None,
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &QUOTA_DECISIONS,
    &QUOTA_CDOT_DURATION,
    &QUOTA_BYPASSED,
    &QUOTA_CACHE_ENTRIES,
];
