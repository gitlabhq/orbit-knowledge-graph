use crate::MetricSpec;

pub mod labels {
    pub const FAILURE_REASON: &str = "failure_reason";
}

const DOMAIN: &str = "query.engine";

pub const COMPILER_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.compiler.rejected",
    "Total queries the compiler rejected before execution, labelled by failure reason.",
    None,
    &[labels::FAILURE_REASON],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&COMPILER_REJECTED];
