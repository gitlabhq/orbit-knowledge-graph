//! SDLC indexing pipeline metrics: per-entity throughput, watermark freshness,
//! datalake query and transform latency.

use crate::MetricSpec;
use crate::buckets::{LATENCY, LATENCY_FAST_FINE};

pub mod labels {
    pub const ENTITY: &str = "entity";
    pub const ERROR_KIND: &str = "error_kind";
    pub const HANDLER: &str = "handler";
    pub const ACTION: &str = "action";
}

const DOMAIN: &str = "indexer.sdlc";

pub const PIPELINE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.sdlc.pipeline.duration",
    "End-to-end duration of a single entity or edge pipeline run.",
    Some("s"),
    &[labels::ENTITY],
    LATENCY,
    DOMAIN,
);

pub const PIPELINE_ROWS_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.pipeline.rows.processed",
    "Total rows extracted and written by SDLC pipelines.",
    None,
    &[labels::ENTITY],
    DOMAIN,
);

pub const PIPELINE_ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.pipeline.errors",
    "Total SDLC pipeline failures.",
    None,
    &[labels::ENTITY, labels::ERROR_KIND],
    DOMAIN,
);

pub const HANDLER_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.sdlc.handler.duration",
    "Duration of a full handler invocation across all its pipelines.",
    Some("s"),
    &[labels::HANDLER],
    LATENCY,
    DOMAIN,
);

pub const DATALAKE_QUERY_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.sdlc.datalake.query.duration",
    "Duration of ClickHouse datalake extraction queries.",
    Some("s"),
    &[],
    LATENCY_FAST_FINE,
    DOMAIN,
);

// Drop `.bytes` from the name so the Prometheus `By` unit suffix doesn't
// produce `bytes_bytes_total`.
pub const DATALAKE_QUERY_BYTES: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.datalake.query",
    "Total bytes returned by ClickHouse datalake extraction queries.",
    Some("By"),
    &[labels::ENTITY],
    DOMAIN,
);

pub const TRANSFORM_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.sdlc.transform.duration",
    "Duration of DataFusion SQL transform per batch.",
    Some("s"),
    &[],
    LATENCY_FAST_FINE,
    DOMAIN,
);

pub const WATERMARK_LAG: MetricSpec = MetricSpec::gauge(
    "gkg.indexer.sdlc.watermark.lag",
    "Seconds between the current watermark and wall-clock time (data freshness).",
    Some("s"),
    &[],
    DOMAIN,
);

// Cardinality is bounded by the vendored Rails `ICON_TYPES` list (~60–100
// values), so the `action` label is safe. A non-zero count means the
// system-notes handler saw a Rails action it does not yet handle and
// dropped it — the signal that the vendored `ICON_TYPES` copy has drifted.
pub const SYSTEM_NOTES_UNKNOWN_ACTION: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.system_notes.unknown_action",
    "Total system notes dropped because their action is not handled by the parser.",
    None,
    &[labels::ACTION],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &PIPELINE_DURATION,
    &PIPELINE_ROWS_PROCESSED,
    &PIPELINE_ERRORS,
    &HANDLER_DURATION,
    &DATALAKE_QUERY_DURATION,
    &DATALAKE_QUERY_BYTES,
    &TRANSFORM_DURATION,
    &WATERMARK_LAG,
    &SYSTEM_NOTES_UNKNOWN_ACTION,
];
