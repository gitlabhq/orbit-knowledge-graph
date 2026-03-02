use code_graph::analysis::types::GraphData;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use crate::handler::HandlerError;

const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

#[derive(Clone)]
pub struct CodeMetrics {
    pub(super) events_processed: Counter<u64>,
    pub(super) handler_duration: Histogram<f64>,
    pub(super) repository_fetch_duration: Histogram<f64>,
    pub(super) indexing_duration: Histogram<f64>,
    pub(super) write_duration: Histogram<f64>,
    pub(super) files_processed: Counter<u64>,
    pub(super) nodes_indexed: Counter<u64>,
    pub(super) errors: Counter<u64>,
}

impl CodeMetrics {
    pub fn new() -> Self {
        let meter = global::meter("indexer_code");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let events_processed = meter
            .u64_counter("indexer.code.events.processed")
            .with_description("Total push events processed by the code indexing handler")
            .build();

        let handler_duration = meter
            .f64_histogram("indexer.code.handler.duration")
            .with_unit("s")
            .with_description("End-to-end duration of processing a single push event")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let repository_fetch_duration = meter
            .f64_histogram("indexer.code.repository.fetch.duration")
            .with_unit("s")
            .with_description("Duration of fetching and extracting a repository from Gitaly")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let indexing_duration = meter
            .f64_histogram("indexer.code.indexing.duration")
            .with_unit("s")
            .with_description("Duration of code-graph parsing and analysis")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let write_duration = meter
            .f64_histogram("indexer.code.write.duration")
            .with_unit("s")
            .with_description("Duration of writing all graph tables to ClickHouse")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let files_processed = meter
            .u64_counter("indexer.code.files.processed")
            .with_description("Total files seen by the code-graph indexer")
            .build();

        let nodes_indexed = meter
            .u64_counter("indexer.code.nodes.indexed")
            .with_description("Total graph nodes and edges indexed by the code handler")
            .build();

        let errors = meter
            .u64_counter("indexer.code.errors")
            .with_description("Total code indexing errors by pipeline stage")
            .build();

        Self {
            events_processed,
            handler_duration,
            repository_fetch_duration,
            indexing_duration,
            write_duration,
            files_processed,
            nodes_indexed,
            errors,
        }
    }
}

impl CodeMetrics {
    pub(super) fn record_outcome(&self, outcome: &'static str) {
        self.events_processed
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }

    pub(super) fn record_files_processed(&self, count: u64, outcome: &'static str) {
        self.files_processed
            .add(count, &[KeyValue::new("outcome", outcome)]);
    }

    pub(super) fn record_node_counts(&self, graph_data: &GraphData) {
        self.nodes_indexed.add(
            graph_data.directory_nodes.len() as u64,
            &[KeyValue::new("kind", "directory")],
        );
        self.nodes_indexed.add(
            graph_data.file_nodes.len() as u64,
            &[KeyValue::new("kind", "file")],
        );
        self.nodes_indexed.add(
            graph_data.definition_nodes.len() as u64,
            &[KeyValue::new("kind", "definition")],
        );
        self.nodes_indexed.add(
            graph_data.imported_symbol_nodes.len() as u64,
            &[KeyValue::new("kind", "imported_symbol")],
        );
        self.nodes_indexed.add(
            graph_data.relationships.len() as u64,
            &[KeyValue::new("kind", "edge")],
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
            metrics.errors.add(1, &[KeyValue::new("stage", stage)]);
        }
        self
    }
}
