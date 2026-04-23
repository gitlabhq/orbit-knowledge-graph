//! Query engine threat and invariant counters, fired during compilation.

use crate::MetricSpec;

pub mod labels {
    pub const REASON: &str = "reason";
}

const DOMAIN: &str = "query.engine";

pub const THREAT_VALIDATION_FAILED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.validation_failed",
    "Query rejected by structural validation (schema, references, pagination).",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_ALLOWLIST_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.allowlist_rejected",
    "Query referenced an entity, column, or relationship not in the ontology allowlist.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_AUTH_FILTER_MISSING: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.auth_filter_missing",
    "Security context was invalid or absent when required.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_TIMEOUT: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.timeout",
    "Query compilation or execution exceeded the deadline.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_RATE_LIMITED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.rate_limited",
    "Caller was throttled before query compilation.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_DEPTH_EXCEEDED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.depth_exceeded",
    "Traversal depth or hop count exceeded the hard cap.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const THREAT_LIMIT_EXCEEDED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.threat.limit_exceeded",
    "Array cardinality cap exceeded (node_ids count or IN filter value count).",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const INTERNAL_PIPELINE_INVARIANT_VIOLATED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.internal.pipeline_invariant_violated",
    "Lowering or codegen hit a state that upstream validation should have prevented.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &THREAT_VALIDATION_FAILED,
    &THREAT_ALLOWLIST_REJECTED,
    &THREAT_AUTH_FILTER_MISSING,
    &THREAT_TIMEOUT,
    &THREAT_RATE_LIMITED,
    &THREAT_DEPTH_EXCEEDED,
    &THREAT_LIMIT_EXCEEDED,
    &INTERNAL_PIPELINE_INVARIANT_VIOLATED,
];
