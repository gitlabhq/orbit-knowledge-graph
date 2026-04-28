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
    pub(super) repository_indexing_completed: Counter<u64>,
    pub(super) repository_source_size: Histogram<u64>,
    pub(super) indexing_duration: Histogram<f64>,
    pub(super) files_processed: Counter<u64>,
    pub(super) nodes_indexed: Counter<u64>,
    pub(super) errors: Counter<u64>,
    pub(super) files_skipped: Counter<u64>,
    pub(super) file_faults: Counter<u64>,
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
            repository_indexing_completed: code::REPOSITORY_INDEXING_COMPLETED
                .build_counter_u64(meter),
            repository_source_size: code::REPOSITORY_SOURCE_SIZE.build_histogram_u64(meter),
            indexing_duration: code::INDEXING_DURATION.build_histogram_f64(meter),
            files_processed: code::FILES_PROCESSED.build_counter_u64(meter),
            nodes_indexed: code::NODES_INDEXED.build_counter_u64(meter),
            errors: code::ERRORS.build_counter_u64(meter),
            files_skipped: code::FILES_SKIPPED.build_counter_u64(meter),
            file_faults: code::FILE_FAULTS.build_counter_u64(meter),
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

    pub(super) fn record_repository_indexed(&self, outcome: &'static str) {
        self.repository_indexing_completed
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(super) fn record_repository_source_size(&self, bytes: u64) {
        self.repository_source_size.record(bytes, &[]);
    }

    pub(super) fn record_files_processed(&self, count: u64, outcome: &'static str) {
        self.files_processed
            .add(count, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(super) fn record_nodes_indexed(&self, count: u64, kind: &'static str) {
        self.nodes_indexed
            .add(count, &[KeyValue::new(code::labels::KIND, kind)]);
    }

    pub(super) fn record_file_skipped(&self, reason: &'static str) {
        self.files_skipped
            .add(1, &[KeyValue::new(code::labels::REASON, reason)]);
    }

    pub(super) fn record_file_fault(&self, kind: &'static str) {
        self.file_faults
            .add(1, &[KeyValue::new(code::labels::KIND, kind)]);
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
