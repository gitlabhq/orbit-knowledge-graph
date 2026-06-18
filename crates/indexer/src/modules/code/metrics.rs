use std::time::Duration;

use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use gkg_observability::indexer::code;

use crate::handler::HandlerError;

#[derive(Clone)]
pub struct CodeMetrics {
    pub(in crate::modules::code) events_processed: Counter<u64>,
    pub(in crate::modules::code) handler_duration: Histogram<f64>,
    pub(in crate::modules::code) repository_fetch_duration: Histogram<f64>,
    pub(in crate::modules::code) repository_resolution_strategy: Counter<u64>,
    pub(in crate::modules::code) repository_cleanup: Counter<u64>,
    pub(in crate::modules::code) repository_empty: Counter<u64>,
    pub(in crate::modules::code) repository_indexing_completed: Counter<u64>,
    pub(in crate::modules::code) repository_source_size: Histogram<u64>,
    pub(in crate::modules::code) indexing_duration: Histogram<f64>,
    pub(in crate::modules::code) files_processed: Counter<u64>,
    pub(in crate::modules::code) nodes_indexed: Counter<u64>,
    pub(in crate::modules::code) errors: Counter<u64>,
    pub(in crate::modules::code) files_skipped: Counter<u64>,
    pub(in crate::modules::code) file_faults: Counter<u64>,
    pub(in crate::modules::code) archive_entries_skipped: Counter<u64>,
    pub(in crate::modules::code) archive_bytes_skipped: Counter<u64>,
    pub(in crate::modules::code) language_phase_duration: Histogram<f64>,
    pub(in crate::modules::code) file_phase_cpu_duration: Histogram<f64>,
    pub(in crate::modules::code) language_files: Counter<u64>,
    pub(in crate::modules::code) language_bytes: Counter<u64>,
    pub(in crate::modules::code) pipeline_phase_duration: Histogram<f64>,
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
            archive_entries_skipped: code::ARCHIVE_ENTRIES_SKIPPED.build_counter_u64(meter),
            archive_bytes_skipped: code::ARCHIVE_BYTES_SKIPPED.build_counter_u64(meter),
            language_phase_duration: code::LANGUAGE_PHASE_DURATION.build_histogram_f64(meter),
            file_phase_cpu_duration: code::FILE_PHASE_CPU_DURATION.build_histogram_f64(meter),
            language_files: code::LANGUAGE_FILES.build_counter_u64(meter),
            language_bytes: code::LANGUAGE_BYTES.build_counter_u64(meter),
            pipeline_phase_duration: code::PIPELINE_PHASE_DURATION.build_histogram_f64(meter),
        }
    }
}

impl CodeMetrics {
    pub(in crate::modules::code) fn record_resolution_strategy(&self, strategy: &'static str) {
        self.repository_resolution_strategy
            .add(1, &[KeyValue::new(code::labels::STRATEGY, strategy)]);
    }

    pub(in crate::modules::code) fn record_cleanup(&self, outcome: &'static str) {
        self.repository_cleanup
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(in crate::modules::code) fn record_outcome(&self, outcome: &'static str) {
        self.events_processed
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(in crate::modules::code) fn record_handler_duration(&self, started_at: DateTime<Utc>) {
        let elapsed = (Utc::now() - started_at).to_std().unwrap_or_default();
        self.handler_duration.record(elapsed.as_secs_f64(), &[]);
    }

    pub(in crate::modules::code) fn record_empty_repository(&self, reason: &'static str) {
        self.repository_empty
            .add(1, &[KeyValue::new(code::labels::REASON, reason)]);
    }

    pub(in crate::modules::code) fn record_repository_indexed(&self, outcome: &'static str) {
        self.repository_indexing_completed
            .add(1, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(in crate::modules::code) fn record_repository_source_size(&self, bytes: u64) {
        self.repository_source_size.record(bytes, &[]);
    }

    pub(in crate::modules::code) fn record_files_processed(
        &self,
        count: u64,
        outcome: &'static str,
    ) {
        self.files_processed
            .add(count, &[KeyValue::new(code::labels::OUTCOME, outcome)]);
    }

    pub(in crate::modules::code) fn record_nodes_indexed(&self, count: u64, kind: &str) {
        self.nodes_indexed
            .add(count, &[KeyValue::new(code::labels::KIND, kind.to_owned())]);
    }

    pub(in crate::modules::code) fn record_file_skipped(&self, reason: &'static str) {
        self.files_skipped
            .add(1, &[KeyValue::new(code::labels::REASON, reason)]);
    }

    pub(in crate::modules::code) fn record_file_fault(&self, kind: &'static str) {
        self.file_faults
            .add(1, &[KeyValue::new(code::labels::KIND, kind)]);
    }

    pub(in crate::modules::code) fn record_archive_entry_skipped(
        &self,
        reason: &'static str,
        bytes: u64,
    ) {
        let labels = [KeyValue::new(code::labels::REASON, reason)];
        self.archive_entries_skipped.add(1, &labels);
        self.archive_bytes_skipped.add(bytes, &labels);
    }

    pub(in crate::modules::code) fn record_language_timing(
        &self,
        timing: &code_graph::v2::LanguageTimings,
    ) {
        let lang = timing.language.as_str();
        self.language_files.add(
            timing.file_count as u64,
            &[KeyValue::new(code::labels::LANGUAGE, lang.to_owned())],
        );
        self.language_bytes.add(
            timing.total_bytes,
            &[KeyValue::new(code::labels::LANGUAGE, lang.to_owned())],
        );
        for (phase, duration_ms) in [
            ("parse", timing.parse_ms),
            ("graph_build", timing.graph_build_ms),
            ("resolve", timing.resolve_ms),
        ] {
            self.language_phase_duration.record(
                duration_ms / 1000.0,
                &[
                    KeyValue::new(code::labels::LANGUAGE, lang.to_owned()),
                    KeyValue::new(code::labels::PHASE, phase),
                ],
            );
        }
    }

    pub(in crate::modules::code) fn record_file_phase_cpu(
        &self,
        language: code_graph::v2::config::Language,
        cpu: code_graph::v2::PhaseCpu,
    ) {
        let lang = language.as_ref();
        for (phase, dur) in [("parse", cpu.parse), ("walk", cpu.walk), ("ssa", cpu.ssa)] {
            self.file_phase_cpu_duration.record(
                dur.as_secs_f64(),
                &[
                    KeyValue::new(code::labels::LANGUAGE, lang.to_owned()),
                    KeyValue::new(code::labels::PHASE, phase),
                ],
            );
        }
    }

    pub(in crate::modules::code) fn record_phase_timing(
        &self,
        timing: &code_graph::v2::PhaseTimings,
    ) {
        for (phase, duration_ms) in [
            ("file_discovery", timing.file_discovery_ms),
            ("structural_graph", timing.structural_graph_ms),
            ("language_processing", timing.language_processing_ms),
        ] {
            self.pipeline_phase_duration.record(
                duration_ms / 1000.0,
                &[KeyValue::new(code::labels::PHASE, phase)],
            );
        }
    }

    pub(in crate::modules::code) fn record_fetch_duration(&self, elapsed: Duration) {
        self.repository_fetch_duration
            .record(elapsed.as_secs_f64(), &[]);
    }

    pub(in crate::modules::code) fn record_indexing_duration(&self, elapsed: Duration) {
        self.indexing_duration.record(elapsed.as_secs_f64(), &[]);
    }

    pub(in crate::modules::code) fn record_stage_error(&self, stage: &'static str) {
        self.errors
            .add(1, &[KeyValue::new(code::labels::STAGE, stage)]);
    }
}

impl Default for CodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub(in crate::modules::code) trait RecordStageError<T> {
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
            metrics.record_stage_error(stage);
        }
        self
    }
}
