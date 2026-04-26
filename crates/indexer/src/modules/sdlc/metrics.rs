use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use gkg_observability::indexer::sdlc;

pub(crate) use gkg_observability::indexer::sdlc::NAMESPACE_ID_GLOBAL;

#[derive(Clone)]
pub struct SdlcMetrics {
    pub(super) pipeline_duration: Histogram<f64>,
    pub(super) pipeline_rows_processed: Counter<u64>,
    pub(super) pipeline_errors: Counter<u64>,
    pub(super) handler_duration: Histogram<f64>,
    pub(super) datalake_query_duration: Histogram<f64>,
    pub(super) datalake_query_bytes: Counter<u64>,
    pub(super) transform_duration: Histogram<f64>,
    pub(super) watermark_lag: Gauge<f64>,
}

impl SdlcMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            pipeline_duration: sdlc::PIPELINE_DURATION.build_histogram_f64(meter),
            pipeline_rows_processed: sdlc::PIPELINE_ROWS_PROCESSED.build_counter_u64(meter),
            pipeline_errors: sdlc::PIPELINE_ERRORS.build_counter_u64(meter),
            handler_duration: sdlc::HANDLER_DURATION.build_histogram_f64(meter),
            datalake_query_duration: sdlc::DATALAKE_QUERY_DURATION.build_histogram_f64(meter),
            datalake_query_bytes: sdlc::DATALAKE_QUERY_BYTES.build_counter_u64(meter),
            transform_duration: sdlc::TRANSFORM_DURATION.build_histogram_f64(meter),
            watermark_lag: sdlc::WATERMARK_LAG.build_gauge_f64(meter),
        }
    }
}

impl SdlcMetrics {
    pub(super) fn record_pipeline_error(&self, entity: &str, error_kind: &str, namespace_id: &str) {
        self.pipeline_errors.add(
            1,
            &[
                KeyValue::new(sdlc::labels::ENTITY, entity.to_owned()),
                KeyValue::new(sdlc::labels::ERROR_KIND, error_kind.to_owned()),
                KeyValue::new(sdlc::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_pipeline_completion(
        &self,
        entity: &str,
        duration: f64,
        rows: u64,
        namespace_id: &str,
    ) {
        // Histograms intentionally drop the namespace label: per-namespace
        // bucket cardinality would push past Mimir's per-tenant comfort.
        self.pipeline_duration.record(
            duration,
            &[KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())],
        );
        self.pipeline_rows_processed.add(
            rows,
            &[
                KeyValue::new(sdlc::labels::ENTITY, entity.to_owned()),
                KeyValue::new(sdlc::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_datalake_query(
        &self,
        entity: &str,
        duration: f64,
        bytes: u64,
        namespace_id: &str,
    ) {
        self.datalake_query_duration.record(
            duration,
            &[KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())],
        );
        self.datalake_query_bytes.add(
            bytes,
            &[
                KeyValue::new(sdlc::labels::ENTITY, entity.to_owned()),
                KeyValue::new(sdlc::labels::NAMESPACE_ID, namespace_id.to_owned()),
            ],
        );
    }

    pub(super) fn record_transform_duration(&self, entity: &str, duration: f64) {
        self.transform_duration.record(
            duration,
            &[KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())],
        );
    }

    pub(super) fn record_handler_duration(&self, handler: &'static str, duration: f64) {
        self.handler_duration
            .record(duration, &[KeyValue::new(sdlc::labels::HANDLER, handler)]);
    }

    pub(super) fn record_watermark_lag(&self, entity: &str, watermark: &DateTime<Utc>) {
        let lag = Utc::now()
            .signed_duration_since(*watermark)
            .num_milliseconds()
            .max(0) as f64
            / 1000.0;
        self.watermark_lag.record(
            lag,
            &[KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())],
        );
    }
}

impl Default for SdlcMetrics {
    fn default() -> Self {
        Self::new()
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
    fn pipeline_completion_writes_top_level_namespace_id() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = SdlcMetrics::with_meter(&provider.meter("test"));

        metrics.record_pipeline_completion("Project", 0.05, 7, "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.sdlc.pipeline.rows.processed");
        let dp = dps
            .iter()
            .find(|dp| attr_value(dp, "entity").as_deref() == Some("Project"))
            .expect("rows counter point should exist");
        assert_eq!(dp.value(), 7);
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }

    #[test]
    fn pipeline_completion_uses_global_sentinel() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = SdlcMetrics::with_meter(&provider.meter("test"));

        metrics.record_pipeline_completion("User", 0.01, 1, NAMESPACE_ID_GLOBAL);

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.sdlc.pipeline.rows.processed");
        let dp = dps
            .iter()
            .find(|dp| attr_value(dp, "entity").as_deref() == Some("User"))
            .expect("rows counter point should exist");
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("_global")
        );
    }

    #[test]
    fn pipeline_error_writes_top_level_namespace_id() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = SdlcMetrics::with_meter(&provider.meter("test"));

        metrics.record_pipeline_error("Project", "datalake_query", "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.sdlc.pipeline.errors");
        let dp = dps.first().expect("error point should exist");
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
        assert_eq!(
            attr_value(dp, "error_kind").as_deref(),
            Some("datalake_query")
        );
    }

    #[test]
    fn datalake_query_counter_writes_top_level_namespace_id() {
        let (provider, exporter) = provider_and_exporter();
        let metrics = SdlcMetrics::with_meter(&provider.meter("test"));

        metrics.record_datalake_query("Project", 0.02, 1024, "9970");

        let collected = collect(&provider, &exporter);
        let dps = find_counter_u64(&collected, "gkg.indexer.sdlc.datalake.query");
        let dp = dps.first().expect("bytes counter point should exist");
        assert_eq!(dp.value(), 1024);
        assert_eq!(
            attr_value(dp, "top_level_namespace_id").as_deref(),
            Some("9970")
        );
    }
}
