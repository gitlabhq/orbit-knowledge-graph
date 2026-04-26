//! Code-indexing pipeline metrics (push events, repo fetch, parsing, nodes,
//! per-stage errors).

use crate::MetricSpec;
use crate::buckets::{LATENCY, MEMORY_BYTES};

pub mod labels {
    pub const OUTCOME: &str = "outcome";
    pub const STRATEGY: &str = "strategy";
    pub const REASON: &str = "reason";
    pub const KIND: &str = "kind";
    pub const STAGE: &str = "stage";
    pub const NAMESPACE_ID: &str = "top_level_namespace_id";
}

/// Sentinel emitted when the top-level namespace id cannot be derived from
/// the task's `traversal_path` (parse failure or unexpected format). Keeps
/// the label set homogeneous across series; a non-zero rate of this value
/// means the upstream contract changed.
pub const NAMESPACE_ID_UNKNOWN: &str = "_unknown";

const DOMAIN: &str = "indexer.code";

pub const EVENTS_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.events.processed",
    "Total push events processed by the code indexing handler.",
    None,
    &[labels::OUTCOME, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const HANDLER_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.handler.duration",
    "End-to-end duration of processing a single push event.",
    Some("s"),
    &[],
    LATENCY,
    DOMAIN,
);

pub const REPOSITORY_FETCH_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.repository.fetch.duration",
    "Duration of downloading a repository archive from Gitaly.",
    Some("s"),
    &[],
    LATENCY,
    DOMAIN,
);

pub const REPOSITORY_RESOLUTION_STRATEGY: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.repository.resolution",
    "Repository resolution strategy used (full_download, empty_repository).",
    None,
    &[labels::STRATEGY],
    DOMAIN,
);

pub const REPOSITORY_CLEANUP: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.repository.cleanup",
    "Repository disk cleanup outcomes (success, failure).",
    None,
    &[labels::OUTCOME],
    DOMAIN,
);

pub const REPOSITORY_EMPTY: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.repository.empty",
    "Projects short-circuited as terminal-empty at fetch time.",
    None,
    &[labels::REASON, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const REPOSITORY_INDEXING_COMPLETED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.repository.indexing.completed",
    "Repository indexing runs completed by the code indexing handler.",
    None,
    &[labels::OUTCOME, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const REPOSITORY_SOURCE_SIZE: MetricSpec = MetricSpec::histogram_u64(
    "gkg.indexer.code.repository.source.size",
    "Total bytes of language-supported source files discovered during one code indexing run.",
    Some("By"),
    &[],
    MEMORY_BYTES,
    DOMAIN,
);

pub const INDEXING_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.indexing.duration",
    "Duration of code-graph parsing and analysis.",
    Some("s"),
    &[],
    LATENCY,
    DOMAIN,
);

pub const FILES_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.files.processed",
    "Total files seen by the code-graph indexer.",
    None,
    &[labels::OUTCOME, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const NODES_INDEXED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.nodes.indexed",
    "Total graph nodes and edges indexed by the code handler.",
    None,
    &[labels::KIND, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.errors",
    "Total code indexing errors by pipeline stage.",
    None,
    &[labels::STAGE, labels::NAMESPACE_ID],
    DOMAIN,
);

pub const FILES_SKIPPED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.files.skipped",
    "Source files skipped by the code-graph indexer for policy or watchdog reasons. \
     Not an error. Reasons: `oversize` (file exceeds the per-language byte ceiling), \
     `line_too_long` (a single line exceeds the per-language character ceiling), \
     `timeout_sentinel` (per-file watchdog killed the file before resolution finished).",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &EVENTS_PROCESSED,
    &HANDLER_DURATION,
    &REPOSITORY_FETCH_DURATION,
    &REPOSITORY_RESOLUTION_STRATEGY,
    &REPOSITORY_CLEANUP,
    &REPOSITORY_EMPTY,
    &REPOSITORY_INDEXING_COMPLETED,
    &REPOSITORY_SOURCE_SIZE,
    &INDEXING_DURATION,
    &FILES_PROCESSED,
    &NODES_INDEXED,
    &ERRORS,
    &FILES_SKIPPED,
];
