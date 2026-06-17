//! Code-indexing pipeline metrics (push events, repo fetch, parsing, nodes,
//! per-stage errors).

use crate::MetricSpec;
use crate::buckets::{LATENCY_SLOW, MEMORY_BYTES};

pub mod labels {
    pub const OUTCOME: &str = "outcome";
    pub const STRATEGY: &str = "strategy";
    pub const REASON: &str = "reason";
    pub const KIND: &str = "kind";
    pub const STAGE: &str = "stage";
    pub const LANGUAGE: &str = "language";
    pub const PHASE: &str = "phase";
}

const DOMAIN: &str = "indexer.code";

pub const EVENTS_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.events.processed",
    "Total push events processed by the code indexing handler.",
    None,
    &[labels::OUTCOME],
    DOMAIN,
);

pub const HANDLER_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.handler.duration",
    "End-to-end duration of processing a single push event.",
    Some("s"),
    &[],
    LATENCY_SLOW,
    DOMAIN,
);

pub const REPOSITORY_FETCH_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.repository.fetch.duration",
    "Duration of downloading a repository archive from Gitaly.",
    Some("s"),
    &[],
    LATENCY_SLOW,
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
    &[labels::REASON],
    DOMAIN,
);

pub const REPOSITORY_INDEXING_COMPLETED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.repository.indexing.completed",
    "Repository indexing runs completed by the code indexing handler.",
    None,
    &[labels::OUTCOME],
    DOMAIN,
);

pub const REPOSITORY_SOURCE_SIZE: MetricSpec = MetricSpec::histogram_u64(
    "gkg.indexer.code.repository.source.size",
    "Total bytes of repository files discovered during one code indexing run.",
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
    LATENCY_SLOW,
    DOMAIN,
);

pub const FILES_PROCESSED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.files.processed",
    "Total files seen by the code-graph indexer.",
    None,
    &[labels::OUTCOME],
    DOMAIN,
);

pub const NODES_INDEXED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.nodes.indexed",
    "Total graph nodes and edges indexed by the code handler.",
    None,
    &[labels::KIND],
    DOMAIN,
);

pub const ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.errors",
    "Task-level code indexing failures by pipeline stage. Increments only \
     when a code indexing task ends with a fatal pipeline error (sink write, \
     thread pool, sentinel spawn, internal panic). Per-file failures land in \
     `gkg.indexer.code.file_faults` instead, so this rate is suitable for alerting.",
    None,
    &[labels::STAGE],
    DOMAIN,
);

pub const FILES_SKIPPED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.files.skipped",
    "Source files skipped by the code-graph indexer for policy or watchdog reasons. \
     Not an error. Reasons: `oversize`, `oversize_combined`, `line_too_long`, \
     `minified`, `not_utf8`, `non_regular_file`, `unsafe_path`, and per-phase \
     CPU-budget timeouts `timeout_parse`, `timeout_walk`, `timeout_ssa`, \
     `timeout_sentinel`.",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const FILE_FAULTS: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.file_faults",
    "Per-file failures during code indexing, by kind. The task itself \
     completes successfully; individual files were excluded from the graph. \
     Kinds: `file_read`, `invalid_utf8`, `syntax_error`, `oxc_panic`, \
     `oxc_semantic`, `analyzer_panic`, `unknown_source_type`, \
     `embedded_script_parse`, `rust_workspace_missing`.",
    None,
    &[labels::KIND],
    DOMAIN,
);

pub const ARCHIVE_ENTRIES_SKIPPED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.archive.entries.skipped",
    "Tar archive entries dropped before extraction so they never touch disk. \
     Reasons: `excluded_extension` (basename matches the binary-asset / \
     media / archive denylist in `code-graph::v2::config::filter::\
     EXCLUDED_INDEXING_GLOBS`), `oversize` (declared size exceeds the \
     configured per-file ceiling).",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const ARCHIVE_BYTES_SKIPPED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.archive.bytes.skipped",
    "Total bytes that the archive extractor refused to write to disk, \
     summed from the tar header sizes of skipped entries. Same `reason` \
     labels as `gkg.indexer.code.archive.entries.skipped`.",
    Some("By"),
    &[labels::REASON],
    DOMAIN,
);

pub const LANGUAGE_PHASE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.language.phase_duration",
    "Per-language pipeline phase duration. Labels: language (ruby, java_script, \
     type_script, rust, ...), phase (parse, graph_build, resolve).",
    Some("s"),
    &[labels::LANGUAGE, labels::PHASE],
    LATENCY_SLOW,
    DOMAIN,
);

pub const LANGUAGE_FILES: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.language.files",
    "Files processed per language family.",
    None,
    &[labels::LANGUAGE],
    DOMAIN,
);

pub const LANGUAGE_BYTES: MetricSpec = MetricSpec::counter(
    "gkg.indexer.code.language.bytes",
    "Source bytes processed per language family.",
    Some("By"),
    &[labels::LANGUAGE],
    DOMAIN,
);

pub const PIPELINE_PHASE_DURATION: MetricSpec = MetricSpec::histogram_f64(
    "gkg.indexer.code.pipeline.phase_duration",
    "Top-level pipeline phase duration. Labels: phase (file_discovery, \
     structural_graph, language_processing).",
    Some("s"),
    &[labels::PHASE],
    LATENCY_SLOW,
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
    &FILE_FAULTS,
    &ARCHIVE_ENTRIES_SKIPPED,
    &ARCHIVE_BYTES_SKIPPED,
    &LANGUAGE_PHASE_DURATION,
    &LANGUAGE_FILES,
    &LANGUAGE_BYTES,
    &PIPELINE_PHASE_DURATION,
];
