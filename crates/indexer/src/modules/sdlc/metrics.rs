use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use crate::metrics::DURATION_BUCKETS;

#[derive(Clone)]
pub struct SdlcMetrics {
    pub(super) pipeline_duration: Histogram<f64>,
    pub(super) pipeline_rows_processed: Counter<u64>,
    pub(super) pipeline_edges_processed: Counter<u64>,
    pub(super) pipeline_batches_processed: Counter<u64>,
    pub(super) pipeline_errors: Counter<u64>,
    pub(super) handler_duration: Histogram<f64>,
    pub(super) datalake_query_duration: Histogram<f64>,
    pub(super) transform_duration: Histogram<f64>,
    pub(super) watermark_lag: Gauge<f64>,
}

impl SdlcMetrics {
    pub fn new() -> Self {
        let meter = global::meter("indexer_sdlc");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let pipeline_duration = meter
            .f64_histogram("indexer.sdlc.pipeline.duration")
            .with_unit("s")
            .with_description("End-to-end duration of a single entity or edge pipeline run")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let pipeline_rows_processed = meter
            .u64_counter("indexer.sdlc.pipeline.rows.processed")
            .with_description("Total rows extracted and written by SDLC pipelines")
            .build();

        let pipeline_edges_processed = meter
            .u64_counter("indexer.sdlc.pipeline.edges.processed")
            .with_description("Total edges written by SDLC pipelines")
            .build();

        let pipeline_batches_processed = meter
            .u64_counter("indexer.sdlc.pipeline.batches.processed")
            .with_description("Total Arrow batches processed by SDLC pipelines")
            .build();

        let pipeline_errors = meter
            .u64_counter("indexer.sdlc.pipeline.errors")
            .with_description("Total SDLC pipeline failures")
            .build();

        let handler_duration = meter
            .f64_histogram("indexer.sdlc.handler.duration")
            .with_unit("s")
            .with_description("Duration of a full handler invocation across all its pipelines")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let datalake_query_duration = meter
            .f64_histogram("indexer.sdlc.datalake.query.duration")
            .with_unit("s")
            .with_description("Duration of ClickHouse datalake extraction queries")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let transform_duration = meter
            .f64_histogram("indexer.sdlc.transform.duration")
            .with_unit("s")
            .with_description("Duration of DataFusion SQL transform per batch")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let watermark_lag = meter
            .f64_gauge("indexer.sdlc.watermark.lag")
            .with_unit("s")
            .with_description(
                "Seconds between the current watermark and wall clock time (data freshness)",
            )
            .build();

        Self {
            pipeline_duration,
            pipeline_rows_processed,
            pipeline_edges_processed,
            pipeline_batches_processed,
            pipeline_errors,
            handler_duration,
            datalake_query_duration,
            transform_duration,
            watermark_lag,
        }
    }
}

impl SdlcMetrics {
    pub(super) fn record_pipeline_error(&self, entity: &str, error_kind: &str) {
        self.pipeline_errors.add(
            1,
            &[
                KeyValue::new("entity", entity.to_owned()),
                KeyValue::new("error_kind", error_kind.to_owned()),
            ],
        );
    }

    pub(super) fn record_pipeline_completion(
        &self,
        entity: &str,
        duration: f64,
        rows: u64,
        edges: u64,
        batches: u64,
    ) {
        let labels = [KeyValue::new("entity", entity.to_owned())];
        self.pipeline_duration.record(duration, &labels);
        self.pipeline_rows_processed.add(rows, &labels);
        self.pipeline_edges_processed.add(edges, &labels);
        self.pipeline_batches_processed.add(batches, &labels);
    }

    pub(super) fn record_datalake_query_duration(&self, entity: &str, duration: f64) {
        self.datalake_query_duration
            .record(duration, &[KeyValue::new("entity", entity.to_owned())]);
    }

    pub(super) fn record_transform_duration(&self, entity: &str, duration: f64) {
        self.transform_duration
            .record(duration, &[KeyValue::new("entity", entity.to_owned())]);
    }

    pub(super) fn record_handler_duration(&self, handler: &'static str, duration: f64) {
        self.handler_duration
            .record(duration, &[KeyValue::new("handler", handler)]);
    }

    pub(super) fn record_watermark_lag(&self, entity: &str, watermark: &DateTime<Utc>) {
        let lag = Utc::now()
            .signed_duration_since(*watermark)
            .num_milliseconds()
            .max(0) as f64
            / 1000.0;
        self.watermark_lag
            .record(lag, &[KeyValue::new("entity", entity.to_owned())]);
    }
}

impl Default for SdlcMetrics {
    fn default() -> Self {
        Self::new()
    }
}
