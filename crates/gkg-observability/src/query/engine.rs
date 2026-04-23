//! Query engine threat and invariant counters, fired during compilation.

use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const REASON: &str = "reason";
}

const DOMAIN: &str = "query.engine";

pub const THREAT_VALIDATION_FAILED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.validation_failed",
    description: "Query rejected by structural validation (schema, references, pagination).",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_ALLOWLIST_REJECTED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.allowlist_rejected",
    description: "Query referenced an entity, column, or relationship not in the ontology allowlist.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_AUTH_FILTER_MISSING: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.auth_filter_missing",
    description: "Security context was invalid or absent when required.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_TIMEOUT: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.timeout",
    description: "Query compilation or execution exceeded the deadline.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_RATE_LIMITED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.rate_limited",
    description: "Caller was throttled before query compilation.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_DEPTH_EXCEEDED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.depth_exceeded",
    description: "Traversal depth or hop count exceeded the hard cap.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const THREAT_LIMIT_EXCEEDED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.threat.limit_exceeded",
    description: "Array cardinality cap exceeded (node_ids count or IN filter value count).",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const INTERNAL_PIPELINE_INVARIANT_VIOLATED: MetricSpec = MetricSpec {
    otel_name: "gkg.query.engine.internal.pipeline_invariant_violated",
    description: "Lowering or codegen hit a state that upstream validation should have prevented.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

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
