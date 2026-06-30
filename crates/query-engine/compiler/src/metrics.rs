//! Every [`QueryError`] increments [`spec::COMPILER_REJECTED`] with a
//! closed-enum `failure_reason` label.

use std::sync::LazyLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

use gkg_observability::query::engine as spec;

use crate::error::QueryError;

/// Test-only side-channel that lets unit tests verify `count_err` was
/// actually invoked. The OpenTelemetry counter has no readable handle, so
/// this is the cheapest way to keep the regression tests honest.
#[cfg(test)]
pub(crate) static COUNT_ERR_HITS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub static METRICS: LazyLock<QueryEngineMetrics> = LazyLock::new(QueryEngineMetrics::new);

#[derive(Clone)]
pub struct QueryEngineMetrics {
    pub compiler_rejected: Counter<u64>,
}

impl QueryEngineMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            compiler_rejected: spec::COMPILER_REJECTED.build_counter_u64(&meter),
        }
    }
}

impl Default for QueryEngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Maps to a low-cardinality `failure_reason` label.
pub(crate) fn failure_reason(err: &QueryError) -> &'static str {
    match err {
        QueryError::Parse(_) => "parse",
        QueryError::Validation(_) => "schema",
        QueryError::ReferenceError(_) => "reference",
        QueryError::PaginationError(_) => "pagination",
        QueryError::AllowlistRejected(_) => "ontology",
        QueryError::Authorization(_) => "authorization",
        QueryError::Restrict(_) => "restrict",
        QueryError::Ontology(_) => "ontology_internal",
        QueryError::DepthExceeded(_) => "depth",
        QueryError::LimitExceeded(_) => "limit",
        QueryError::Security(_) => "security",
        QueryError::Lowering(_) => "lowering",
        QueryError::Enforcement(_) => "enforcement",
        QueryError::Codegen(_) => "codegen",
        QueryError::PipelineInvariant(_) => "pipeline",
    }
}

pub(crate) trait CountErr<T, E> {
    fn count_err(self) -> crate::error::Result<T>;
}

impl<T, E: Into<QueryError>> CountErr<T, E> for std::result::Result<T, E> {
    fn count_err(self) -> crate::error::Result<T> {
        self.map_err(|e| {
            let qe: QueryError = e.into();
            METRICS.compiler_rejected.add(
                1,
                &[KeyValue::new(
                    spec::labels::FAILURE_REASON,
                    failure_reason(&qe),
                )],
            );
            #[cfg(test)]
            COUNT_ERR_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            qe
        })
    }
}
