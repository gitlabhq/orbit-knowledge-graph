use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use gkg_observability::indexer::code;

use crate::handler::HandlerError;

#[derive(Clone)]
pub struct CodeMetrics {
    pub(super) events_processed: Counter<u64>,
    pub(super) handler_duration: Histogram<f64>,
    pub(super) repository_fetch_duration: Histogram<f64>,
    pub(super) repository_resolution_strategy: Counter<u64>,
    pub(super) repository_cleanup: Counter<u64>,
    pub(super) repository_empty: Counter<u64>,
    pub(super) indexing_duration: Histogram<f64>,
    pub(super) files_processed: Counter<u64>,
    pub(super) errors: Counter<u64>,
}

impl CodeMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            events_processed: code::EVENTS_PROCESSED.build_counter_u64(meter),
            handler_duration: code::HANDLER_DURATION.build_histogram_f64(meter),
            repository_fetch_duration: code::REPOSITORY_FETCH_DURATION.build_histogram_f64(meter),
            repository_resolution_strategy: code::REPOSITORY_RESOLUTION_STRATEGY
                .build_counter_u64(meter),
            repository_cleanup: code::REPOSITORY_CLEANUP.build_counter_u64(meter),
            repository_empty: code::REPOSITORY_EMPTY.build_counter_u64(meter),
            indexing_duration: code::INDEXING_DURATION.build_histogram_f64(meter),
            files_processed: code::FILES_PROCESSED.build_counter_u64(meter),
            nodes_indexed: code::NODES_INDEXED.build_counter_u64(meter),
            errors: code::ERRORS.build_counter_u64(meter),
        }
    }
}

impl CodeMetrics {
    pub(super) fn record_resolution_strategy(&self, strategy: &'static str) {
        self.repository_resolution_strategy
            .add(1, &[KeyValue::new(code::labels::STRATEGY, strategy)]);
    }

    pub(super) fn record_cleanup(&self, outcome: &'static str) {
        self.repository_cleanup
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(super) fn record_outcome(&self, outcome: &'static str) {
        self.events_processed
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(super) fn record_handler_duration(&self, started_at: DateTime<Utc>) {
        let elapsed = (Utc::now() - started_at).to_std().unwrap_or_default();
        self.handler_duration.record(elapsed.as_secs_f64(), &[]);
    }

    pub(super) fn record_empty_repository(&self, reason: &'static str) {
        self.repository_empty
            .add(1, &[KeyValue::new(code::labels::REASON, reason)]);
    }

    pub(super) fn record_files_processed(&self, count: u64, outcome: &'static str) {
        self.files_processed
            .add(count, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(super) fn record_graph_counts(&self, graph: &code_graph::v2::linker::CodeGraph) {
        self.nodes_indexed.add(
            graph.directories().count() as u64,
            &[KeyValue::new(code::labels::KIND, "directory")],
        );
        self.nodes_indexed.add(
            graph.files().count() as u64,
            &[KeyValue::new(code::labels::KIND, "file")],
        );
        self.nodes_indexed.add(
            graph.defs.len() as u64,
            &[KeyValue::new(code::labels::KIND, "definition")],
        );
        self.nodes_indexed.add(
            graph.imports.len() as u64,
            &[KeyValue::new(code::labels::KIND, "imported_symbol")],
        );
        self.nodes_indexed.add(
            graph.graph.edge_count() as u64,
            &[KeyValue::new(code::labels::KIND, "edge")],
        );
    }
}

impl Default for CodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) trait RecordStageError<T> {
    fn record_error_stage(
        self,
        metrics: &CodeMetrics,
        stage: &'static str,
    ) -> Result<T, HandlerError>;
}

impl<T> RecordStageError<T> for Result<T, HandlerError> {
    fn record_error_stage(
        self,
        metrics: &CodeMetrics,
        stage: &'static str,
    ) -> Result<T, HandlerError> {
        if self.is_err() {
            metrics
                .errors
                .add(1, &[KeyValue::new(code::labels::STAGE, stage)]);
        }
        self
    }
}
