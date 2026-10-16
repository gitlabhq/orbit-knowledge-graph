use crate::MetricSpec;

pub mod labels {
    pub const PHASE: &str = "phase";
    pub const MAX_AGE: &str = "max_age";
    pub const PLAN: &str = "plan";
}

const DOMAIN: &str = "indexer.freshness";

pub const NAMESPACES: MetricSpec = MetricSpec::observable_gauge(
    "gkg.indexer.freshness.namespaces",
    "Enabled namespaces by SDLC indexing phase on the served schema version.",
    None,
    &[labels::PHASE],
    DOMAIN,
);

pub const READY_NAMESPACES: MetricSpec = MetricSpec::observable_gauge(
    "gkg.indexer.freshness.ready.namespaces",
    "Ready namespaces whose least recent SDLC plan completed within max_age.",
    None,
    &[labels::MAX_AGE],
    DOMAIN,
);

pub const PLAN_OLDEST_AGE: MetricSpec = MetricSpec::observable_gauge(
    "gkg.indexer.freshness.plan.oldest.age",
    "Seconds since the least recent completion of each namespaced SDLC plan across enabled namespaces.",
    Some("s"),
    &[labels::PLAN],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&NAMESPACES, &READY_NAMESPACES, &PLAN_OLDEST_AGE];
