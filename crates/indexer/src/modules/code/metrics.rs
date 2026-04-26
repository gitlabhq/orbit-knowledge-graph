use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use gkg_observability::indexer::code;
pub(crate) use gkg_observability::indexer::code::NAMESPACE_ID_UNKNOWN;

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

    pub(super) fn record_outcome(&self, outcome: &'static str, namespace_id: &str) {
        self.events_processed.add(
            1,
            &[
                KeyValue::new(code::labels::OUTCOME, outcome),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_handler_duration(&self, started_at: DateTime<Utc>) {
        let elapsed = (Utc::now() - started_at).to_std().unwrap_or_default();
        self.handler_duration.record(elapsed.as_secs_f64(), &[]);
    }

    pub(super) fn record_empty_repository(&self, reason: &'static str, namespace_id: &str) {
        self.repository_empty.add(
            1,
            &[
                KeyValue::new(code::labels::REASON, reason),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_repository_indexed(&self, outcome: &'static str, namespace_id: &str) {
        self.repository_indexing_completed.add(
            1,
            &[
                KeyValue::new(code::labels::OUTCOME, outcome),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_repository_source_size(&self, bytes: u64) {
        self.repository_source_size.record(bytes, &[]);
    }

    pub(super) fn record_files_processed(
        &self,
        count: u64,
        outcome: &'static str,
        namespace_id: &str,
    ) {
        self.files_processed.add(
            count,
            &[
                KeyValue::new(code::labels::OUTCOME, outcome),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_nodes_indexed(&self, count: u64, kind: &'static str, namespace_id: &str) {
        self.nodes_indexed.add(
            count,
            &[
                KeyValue::new(code::labels::KIND, kind),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_stage_error(&self, stage: &'static str, namespace_id: &str) {
        self.errors.add(
            1,
            &[
                KeyValue::new(code::labels::STAGE, stage),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_file_skipped(&self, reason: &'static str, namespace_id: &str) {
        self.files_skipped.add(
            1,
            &[
                KeyValue::new(code::labels::REASON, reason),
                KeyValue::new(code::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
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
        namespace_id: &str,
    ) -> Result<T, HandlerError>;
}

impl<T> RecordStageError<T> for Result<T, HandlerError> {
    fn record_error_stage(
        self,
        metrics: &CodeMetrics,
        stage: &'static str,
        namespace_id: &str,
    ) -> Result<T, HandlerError> {
        if self.is_err() {
            metrics.record_stage_error(stage, namespace_id);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::time::Duration;

    fn provider_and_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_millis(50))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        (provider, exporter)
    }

    fn collect(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<ResourceMetrics> {
        provider.force_flush().unwrap();
        exporter.get_finished_metrics().unwrap()
    }

    fn find_counter_u64<'a>(
        metrics: &'a [ResourceMetrics],
        name: &str,
    ) -> Vec<&'a opentelemetry_sdk::metrics::data::SumDataPoint<u64>> {
        metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics().flat_map(|sm| sm.metrics()))
            .filter(|m| m.name() == name)
            .filter_map(|m| match m.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    Some(sum.data_points().collect::<Vec<_>>())
                }
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn attr_value(
        dp: &opentelemetry_sdk::metrics::data::SumDataPoint<u64>,
        key: &str,
    ) -> Option<String> {
        dp.attributes()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
    }

    #[test]
    fn record_outcome_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_outcome("indexed", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.events.processed");
        let dp = dps.first().expect("events_processed point should exist");
        assert_eq!(attr_value(dp, "outcome").as_deref(), Some("indexed"));
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn record_repository_indexed_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_repository_indexed("indexed", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.repository.indexing.completed");
        let dp = dps.first().expect("indexing_completed point should exist");
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn record_empty_repository_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_empty_repository("not_found", "_unknown");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.repository.empty");
        let dp = dps.first().expect("repository_empty point should exist");
        assert_eq!(attr_value(dp, "reason").as_deref(), Some("not_found"));
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("_unknown")
        );
    }

    #[test]
    fn record_files_processed_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_files_processed(42, "parsed", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.files.processed");
        let dp = dps.first().expect("files_processed point should exist");
        assert_eq!(dp.value(), 42);
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn record_nodes_indexed_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_nodes_indexed(7, "definition", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.nodes.indexed");
        let dp = dps.first().expect("nodes_indexed point should exist");
        assert_eq!(dp.value(), 7);
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn record_stage_error_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_stage_error("checkpoint", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.errors");
        let dp = dps.first().expect("errors point should exist");
        assert_eq!(attr_value(dp, "stage").as_deref(), Some("checkpoint"));
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn record_file_skipped_writes_namespace_label() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = CodeMetrics::with_meter(&provider.meter("test"));

        metrics.record_file_skipped("oversize", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.code.files.skipped");
        let dp = dps.first().expect("files_skipped point should exist");
        assert_eq!(attr_value(dp, "reason").as_deref(), Some("oversize"));
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }
}
