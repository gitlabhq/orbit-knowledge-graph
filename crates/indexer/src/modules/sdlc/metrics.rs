use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use crate::metrics::DURATION_BUCKETS;

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
        let meter = global::meter("gkg_indexer_sdlc");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let pipeline_duration = meter
            .f64_histogram("gkg.indexer.sdlc.pipeline.duration")
            .with_unit("s")
            .with_description("End-to-end duration of a single entity or edge pipeline run")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let pipeline_rows_processed = meter
            .u64_counter("gkg.indexer.sdlc.pipeline.rows.processed")
            .with_description("Total rows extracted and written by SDLC pipelines")
            .build();

        let pipeline_errors = meter
            .u64_counter("gkg.indexer.sdlc.pipeline.errors")
            .with_description("Total SDLC pipeline failures")
            .build();

        let handler_duration = meter
            .f64_histogram("gkg.indexer.sdlc.handler.duration")
            .with_unit("s")
            .with_description("Duration of a full handler invocation across all its pipelines")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let datalake_query_duration = meter
            .f64_histogram("gkg.indexer.sdlc.datalake.query.duration")
            .with_unit("s")
            .with_description("Duration of ClickHouse datalake extraction queries")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let datalake_query_bytes = meter
            .u64_counter("gkg.indexer.sdlc.datalake.query.bytes")
            .with_unit("By")
            .with_description("Total bytes returned by ClickHouse datalake extraction queries")
            .build();

        let transform_duration = meter
            .f64_histogram("gkg.indexer.sdlc.transform.duration")
            .with_unit("s")
            .with_description("Duration of DataFusion SQL transform per batch")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let watermark_lag = meter
            .f64_gauge("gkg.indexer.sdlc.watermark.lag")
            .with_unit("s")
            .with_description(
                "Seconds between the current watermark and wall clock time (data freshness)",
            )
            .build();

        Self {
            pipeline_duration,
            pipeline_rows_processed,
            pipeline_errors,
            handler_duration,
            datalake_query_duration,
            datalake_query_bytes,
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

    pub(super) fn record_pipeline_completion(&self, entity: &str, duration: f64, rows: u64) {
        let labels = [KeyValue::new("entity", entity.to_owned())];
        self.pipeline_duration.record(duration, &labels);
        self.pipeline_rows_processed.add(rows, &labels);
    }

    pub(super) fn record_datalake_query(&self, entity: &str, duration: f64, bytes: u64) {
        let labels = [KeyValue::new("entity", entity.to_owned())];
        self.datalake_query_duration.record(duration, &labels);
        self.datalake_query_bytes.add(bytes, &labels);
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
