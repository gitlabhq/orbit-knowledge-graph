//! SDLC indexing pipeline metrics: per-entity throughput, watermark freshness,
//! datalake query and transform latency.

use crate::MetricSpec;
use crate::buckets::{LATENCY, LATENCY_FAST_FINE};

pub mod labels {
    pub const ENTITY: &str = "entity";
    pub const ERROR_KIND: &str = "error_kind";
    pub const HANDLER: &str = "handler";
    pub const ACTION: &str = "action";
    pub const NOTEABLE_TYPE: &str = "noteable_type";
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

// Defense-in-depth guard, not the primary drift detector. The extract query
// pre-filters `action IN (handled set)`, which is the exact set the parser
// accepts, so under normal operation this never increments — a drifted/new
// Rails action isn't in the IN-list and is never SELECTed. The real drift
// detector is the CI check `scripts/check-system-note-actions.sh`. This
// counter only fires if the IN-list and `Action::parse` themselves drift
// apart (e.g. an action added to `handled_actions()` but not to the parser),
// which is a code bug rather than upstream drift. Cardinality is bounded by
// the vendored `ICON_TYPES` list (~60–100 values), so the `action` label is
// safe.
pub const SYSTEM_NOTES_UNKNOWN_ACTION: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.system_notes.unknown_action",
    "Guard counter for system notes whose action the parser rejects; \
     normally zero (see scripts/check-system-note-actions.sh for drift).",
    None,
    &[labels::ACTION],
    DOMAIN,
);

// A system note arrived with a `noteable_type` the edge mapper does not
// handle (anything outside MergeRequest / the Issue family / Commit). These
// rows are dropped before edge emission; a sustained non-zero count means
// Rails added a noteable kind we should map. Cardinality is bounded by the
// finite set of Rails STI noteable types, so the label is safe.
pub const SYSTEM_NOTES_UNSUPPORTED_NOTEABLE: MetricSpec = MetricSpec::counter(
    "gkg.indexer.sdlc.system_notes.unsupported_noteable_type",
    "System notes dropped because their noteable_type has no edge mapping.",
    None,
    &[labels::NOTEABLE_TYPE],
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
    &SYSTEM_NOTES_UNSUPPORTED_NOTEABLE,
];
