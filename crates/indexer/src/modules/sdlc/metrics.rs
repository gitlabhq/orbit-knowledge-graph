use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use gkg_observability::indexer::sdlc;

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
    pub(super) fn record_pipeline_error(&self, entity: &str, error_kind: &str) {
        self.pipeline_errors.add(
            1,
            &[
                KeyValue::new(sdlc::labels::ENTITY, entity.to_owned()),
                KeyValue::new(sdlc::labels::ERROR_KIND, error_kind.to_owned()),
            ],
        );
    }

    pub(super) fn record_pipeline_completion(&self, entity: &str, duration: f64) {
        let labels = [KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())];
        self.pipeline_duration.record(duration, &labels);
    }

    pub(super) fn record_batch_rows(&self, entity: &str, rows: u64) {
        self.pipeline_rows_processed.add(
            rows,
            &[KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())],
        );
    }

    pub(super) fn record_datalake_query(&self, entity: &str, duration: f64, bytes: u64) {
        let labels = [KeyValue::new(sdlc::labels::ENTITY, entity.to_owned())];
        self.datalake_query_duration.record(duration, &labels);
        self.datalake_query_bytes.add(bytes, &labels);
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
