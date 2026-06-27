use crate::MetricSpec;

pub mod labels {
    pub const FAILURE_REASON: &str = "failure_reason";
}

const DOMAIN: &str = "query.engine";

/// Counter for every query the compiler rejects before execution.
/// `failure_reason` is a closed enum mapped from the [`QueryError`] discriminant:
/// `parse`, `schema`, `reference`, `pagination`, `ontology`, `ontology_internal`,
/// `authorization`, `depth`, `limit`, `security`, `lowering`, `enforcement`,
/// `codegen`, `pipeline`.
pub const COMPILER_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.query.engine.compiler.rejected",
    "Total queries the compiler rejected before execution, labelled by failure reason.",
    None,
    &[labels::FAILURE_REASON],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&COMPILER_REJECTED];
