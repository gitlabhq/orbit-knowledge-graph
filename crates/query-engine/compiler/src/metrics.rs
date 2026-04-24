//! Query engine metrics, backed by the central `gkg-observability` catalog.
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
use opentelemetry::metrics::Counter;

use gkg_observability::query::engine as spec;

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
        let meter = gkg_observability::meter();
        Self {
            validation_failed: spec::THREAT_VALIDATION_FAILED.build_counter_u64(&meter),
            allowlist_rejected: spec::THREAT_ALLOWLIST_REJECTED.build_counter_u64(&meter),
            auth_filter_missing: spec::THREAT_AUTH_FILTER_MISSING.build_counter_u64(&meter),
            timeout: spec::THREAT_TIMEOUT.build_counter_u64(&meter),
            rate_limited: spec::THREAT_RATE_LIMITED.build_counter_u64(&meter),
            depth_exceeded: spec::THREAT_DEPTH_EXCEEDED.build_counter_u64(&meter),
            limit_exceeded: spec::THREAT_LIMIT_EXCEEDED.build_counter_u64(&meter),
            pipeline_invariant_violated: spec::INTERNAL_PIPELINE_INVARIANT_VIOLATED
                .build_counter_u64(&meter),
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
            counter.add(1, &[KeyValue::new(spec::labels::REASON, reason)]);
            qe
        })
    }
}
