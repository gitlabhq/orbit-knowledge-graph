//! Code-indexing pipeline metrics (push events, repo fetch, parsing, nodes,
//! per-stage errors).

use crate::buckets::LATENCY;
use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const OUTCOME: &str = "outcome";
    pub const STRATEGY: &str = "strategy";
    pub const REASON: &str = "reason";
    pub const KIND: &str = "kind";
    pub const STAGE: &str = "stage";
}

const DOMAIN: &str = "indexer.code";

pub const EVENTS_PROCESSED: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.events.processed",
    description: "Total push events processed by the code indexing handler.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const HANDLER_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.handler.duration",
    description: "End-to-end duration of processing a single push event.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REPOSITORY_FETCH_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.repository.fetch.duration",
    description: "Duration of downloading a repository archive from Gitaly.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REPOSITORY_RESOLUTION_STRATEGY: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.repository.resolution",
    description: "Repository resolution strategy used (full_download, empty_repository).",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::STRATEGY],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REPOSITORY_CLEANUP: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.repository.cleanup",
    description: "Repository disk cleanup outcomes (success, failure).",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const REPOSITORY_EMPTY: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.repository.empty",
    description: "Projects short-circuited as terminal-empty at fetch time.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::REASON],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const INDEXING_DURATION: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.indexing.duration",
    description: "Duration of code-graph parsing and analysis.",
    kind: MetricKind::HistogramF64,
    unit: Some("s"),
    labels: &[],
    buckets: Some(LATENCY),
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const FILES_PROCESSED: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.files.processed",
    description: "Total files seen by the code-graph indexer.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::OUTCOME],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const NODES_INDEXED: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.nodes.indexed",
    description: "Total graph nodes and edges indexed by the code handler.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::KIND],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const ERRORS: MetricSpec = MetricSpec {
    otel_name: "gkg.indexer.code.errors",
    description: "Total code indexing errors by pipeline stage.",
    kind: MetricKind::Counter,
    unit: None,
    labels: &[labels::STAGE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[
    &EVENTS_PROCESSED,
    &HANDLER_DURATION,
    &REPOSITORY_FETCH_DURATION,
    &REPOSITORY_RESOLUTION_STRATEGY,
    &REPOSITORY_CLEANUP,
    &REPOSITORY_EMPTY,
    &INDEXING_DURATION,
    &FILES_PROCESSED,
    &NODES_INDEXED,
    &ERRORS,
];
