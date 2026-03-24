//! Query engine metrics.
//!
//! Eight counters track security-relevant events during query compilation.
//! When no `MeterProvider` is configured, all instruments are no-ops.
//!
//! Counters fired internally by [`compile`](crate::compile):
//! - `validation_failed` — malformed query structure (schema, reference, pagination)
//! - `allowlist_rejected` — entity/column/relationship not in ontology (validation + normalization)
//! - `depth_exceeded` — max_hops or max_depth exceeds the hard cap
//! - `limit_exceeded` — node_ids count or IN filter value count exceeds cardinality cap
//! - `pipeline_invariant_violated` — lowering/codegen hit a state the upstream stages should prevent
//!
//! Counters exported for the server layer to increment:
//! - `auth_filter_missing` — security context invalid or absent
//! - `timeout` — query compilation or execution exceeded deadline
//! - `rate_limited` — caller throttled before compilation

use std::sync::LazyLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;

use crate::error::QueryError;

pub static METRICS: LazyLock<QueryEngineMetrics> = LazyLock::new(QueryEngineMetrics::new);

#[derive(Clone)]
pub struct QueryEngineMetrics {
    pub validation_failed: Counter<u64>,
    pub allowlist_rejected: Counter<u64>,
    pub auth_filter_missing: Counter<u64>,
    pub timeout: Counter<u64>,
    pub rate_limited: Counter<u64>,
    pub depth_exceeded: Counter<u64>,
    pub limit_exceeded: Counter<u64>,
    pub pipeline_invariant_violated: Counter<u64>,
}

impl QueryEngineMetrics {
    pub fn new() -> Self {
        let meter = global::meter("query_engine");

        let validation_failed = meter
            .u64_counter("qe.threat.validation_failed")
            .with_description(
                "Query rejected by structural validation (schema, references, pagination)",
            )
            .build();

        let allowlist_rejected = meter
            .u64_counter("qe.threat.allowlist_rejected")
            .with_description(
                "Query referenced an entity, column, or relationship not in the ontology allowlist",
            )
            .build();

        let auth_filter_missing = meter
            .u64_counter("qe.threat.auth_filter_missing")
            .with_description("Security context was invalid or absent when required")
            .build();

        let timeout = meter
            .u64_counter("qe.threat.timeout")
            .with_description("Query compilation or execution exceeded the deadline")
            .build();

        let rate_limited = meter
            .u64_counter("qe.threat.rate_limited")
            .with_description("Caller was throttled before query compilation")
            .build();

        let depth_exceeded = meter
            .u64_counter("qe.threat.depth_exceeded")
            .with_description("Traversal depth or hop count exceeded the hard cap")
            .build();

        let limit_exceeded = meter
            .u64_counter("qe.threat.limit_exceeded")
            .with_description(
                "Array cardinality cap exceeded (node_ids count or IN filter value count)",
            )
            .build();

        let pipeline_invariant_violated = meter
            .u64_counter("qe.internal.pipeline_invariant_violated")
            .with_description(
                "Lowering or codegen hit a state that upstream validation should have prevented",
            )
            .build();

        Self {
            validation_failed,
            allowlist_rejected,
            auth_filter_missing,
            timeout,
            rate_limited,
            depth_exceeded,
            limit_exceeded,
            pipeline_invariant_violated,
        }
    }
}

impl Default for QueryEngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Maps a [`QueryError`] variant to its counter and a low-cardinality reason label.
pub(crate) fn counter_info(err: &QueryError) -> (&Counter<u64>, &'static str) {
    match err {
        QueryError::Parse(_) => (&METRICS.validation_failed, "parse"),
        QueryError::Validation(_) => (&METRICS.validation_failed, "schema"),
        QueryError::ReferenceError(_) => (&METRICS.validation_failed, "reference"),
        QueryError::PaginationError(_) => (&METRICS.validation_failed, "pagination"),
        QueryError::AllowlistRejected(_) => (&METRICS.allowlist_rejected, "ontology"),
        QueryError::Ontology(_) => (&METRICS.allowlist_rejected, "ontology_internal"),
        QueryError::DepthExceeded(_) => (&METRICS.depth_exceeded, "depth"),
        QueryError::LimitExceeded(_) => (&METRICS.limit_exceeded, "limit"),
        QueryError::Security(_) => (&METRICS.auth_filter_missing, "security"),
        QueryError::Lowering(_) => (&METRICS.pipeline_invariant_violated, "lowering"),
        QueryError::Enforcement(_) => (&METRICS.pipeline_invariant_violated, "enforcement"),
        QueryError::Codegen(_) => (&METRICS.pipeline_invariant_violated, "codegen"),
        QueryError::PipelineInvariant(_) => (&METRICS.pipeline_invariant_violated, "pipeline"),
    }
}

/// Extension trait that converts any compatible error into [`QueryError`],
/// increments the matching counter with a `reason` label, and returns `Result<T>`.
pub(crate) trait CountErr<T, E> {
    fn count_err(self) -> crate::error::Result<T>;
}

impl<T, E: Into<QueryError>> CountErr<T, E> for std::result::Result<T, E> {
    fn count_err(self) -> crate::error::Result<T> {
        self.map_err(|e| {
            let qe: QueryError = e.into();
            let (counter, reason) = counter_info(&qe);
            counter.add(1, &[KeyValue::new("reason", reason)]);
            qe
        })
    }
}
