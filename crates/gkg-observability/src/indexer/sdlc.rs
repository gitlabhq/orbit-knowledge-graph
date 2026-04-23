//! SDLC indexing pipeline metrics: per-entity throughput, watermark freshness,
//! datalake query and transform latency.

use crate::buckets::LATENCY;
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const ENTITY: &str = "entity";
    pub const ERROR_KIND: &str = "error_kind";
    pub const HANDLER: &str = "handler";
}

const DOMAIN: &str = "indexer.sdlc";

pub const PIPELINE_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.pipeline.duration",
    description: "End-to-end duration of a single entity or edge pipeline run.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::ENTITY],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const PIPELINE_ROWS_PROCESSED: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.pipeline.rows.processed",
    description: "Total rows extracted and written by SDLC pipelines.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::ENTITY],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const PIPELINE_ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.pipeline.errors",
    description: "Total SDLC pipeline failures.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::ENTITY, labels::ERROR_KIND],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const HANDLER_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.handler.duration",
    description: "Duration of a full handler invocation across all its pipelines.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::HANDLER],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const DATALAKE_QUERY_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.datalake.query.duration",
    description: "Duration of ClickHouse datalake extraction queries.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::ENTITY],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

// Drop `.bytes` from the name so the Prometheus `By` unit suffix doesn't
// produce `bytes_bytes_total`.
pub const DATALAKE_QUERY_BYTES: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.datalake.query",
    description: "Total bytes returned by ClickHouse datalake extraction queries.",
    kind: MetricKind::Counter,
    unit: Some("By"),
    labels: &[labels::ENTITY],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const TRANSFORM_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.transform.duration",
    description: "Duration of DataFusion SQL transform per batch.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[labels::ENTITY],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const WATERMARK_LAG: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.sdlc.watermark.lag",
    description: "Seconds between the current watermark and wall-clock time (data freshness).",
    kind: MetricKind::Gauge,
    unit: Some("s"),
    labels: &[labels::ENTITY],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &PIPELINE_DURATION,
    &PIPELINE_ROWS_PROCESSED,
    &PIPELINE_ERRORS,
    &HANDLER_DURATION,
    &DATALAKE_QUERY_DURATION,
    &DATALAKE_QUERY_BYTES,
    &TRANSFORM_DURATION,
    &WATERMARK_LAG,
];
