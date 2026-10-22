use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::super::pipeline::IndexingReport;
use code_graph::v2::pipeline::{FileTimingEntry, LanguageTimings};
use code_graph::v2::{OnBatch, SinkError};
use code_graph_incremental::pipeline::FileStats;
use code_graph_incremental::{Envelope, Error, Scalar, export::ExportConfig, templates};

use super::super::{
    arrow_converter::{IndexerEnvelope, route_graph},
    config::CodeTableNames,
};
use super::{Repository, output::Output};

pub(in crate::modules::code) fn run(
    repository: Repository,
    options: templates::Options,
    envelope: IndexerEnvelope,
    ontology: Arc<ontology::Ontology>,
    tables: Arc<CodeTableNames>,
    on_batch: Arc<OnBatch>,
) -> Result<IndexingReport, SinkError> {
    use code_graph::v2::error::{CodeGraphError, FaultedFile, FileFault};
    use code_graph::v2::pipeline::{PhaseTimings, PipelineStats};
    let started = std::time::Instant::now();
    let dropped = repository.usage().dropped_listed;
    if dropped > 0 {
        orbit_log_fields::warn!(
            safe(SKIPPED_COUNT) = dropped,
            safe(REASON) = "file_limit",
            "listed-only files past the repository file limit have no File row"
        );
    }
    let stats = PipelineStats {
        files_discovered: repository.usage().files + dropped,
        bytes_discovered: repository.usage().bytes,
        files_parsed: code_graph_incremental::pipeline::candidates(
            &repository,
            options.limits.max_files,
        )
        .count(),
        files_skipped: repository
            .files()
            .filter(|file| matches!(file.decision(), orbit_utils::vfs::Decision::List(_)))
            .count()
            + dropped,
        files_indexed: 0,
        directories_indexed: 0,
        definitions_count: 0,
        imports_count: 0,
        edges_count: 0,
        references_count: 0,
        slowest_files: Vec::new(),
        language_timings: Vec::new(),
        phase_timings: PhaseTimings::default(),
    };
    let mut output = Output::new(&envelope, &ontology);
    output.batch_bytes = options.batch_bytes;
    let cancelled = options.cancelled.clone();
    let mut export_config = ExportConfig::embedded().clone();
    for name in ontology.local_entity_names() {
        let table = ontology
            .table_name(name)
            .map_err(|error| SinkError(error.to_string()))?;
        export_config.schemas.insert(
            table.to_owned(),
            super::super::arrow_converter::entity_specs(&ontology, name),
        );
    }
    let indexed = std::sync::Mutex::new(stats);
    let sink_failed = std::sync::atomic::AtomicBool::new(false);
    let report = templates::repository(
        repository,
        options,
        &Envelope::new([
            ("project_id", Scalar::Int(envelope.project_id)),
            ("branch", Scalar::Str(&envelope.branch)),
            ("commit_sha", Scalar::Str(&envelope.commit_sha)),
            (
                "traversal_path",
                Scalar::Str(envelope.traversal_path.as_str()),
            ),
            ("_version", Scalar::Int(envelope.version_micros)),
            ("_deleted", Scalar::Bool(false)),
        ]),
        &ontology,
        &export_config,
        |batches| -> Result<(), SinkError> {
            for batches in output.convert(batches)? {
                {
                    #[expect(
                        clippy::expect_used,
                        reason = "mutex poisoning means another thread already panicked; propagating is intended"
                    )]
                    let mut stats = indexed.lock().expect("stats lock");
                    stats.files_indexed += batches.files.num_rows();
                    stats.directories_indexed += batches.directories.num_rows();
                    stats.definitions_count += batches.definitions.num_rows();
                    stats.imports_count += batches.imported_symbols.num_rows();
                    stats.edges_count += batches.edges.num_rows();
                }
                for (table, batch) in route_graph(batches, &tables)? {
                    if cancelled() {
                        return Err(SinkError("indexing cancelled".into()));
                    }
                    on_batch(&table, batch).inspect_err(|_| {
                        sink_failed.store(true, std::sync::atomic::Ordering::Relaxed)
                    })?;
                }
            }
            Ok(())
        },
    );
    #[expect(
        clippy::expect_used,
        reason = "mutex poisoning means another thread already panicked; propagating is intended"
    )]
    let mut stats = indexed.into_inner().expect("stats lock");
    let report = match report {
        Ok(report) => report,
        Err(error)
            if !cancelled()
                && (sink_failed.load(std::sync::atomic::Ordering::Relaxed)
                    || matches!(error, Error::Killed(_))) =>
        {
            return Err(SinkError(error.to_string()));
        }
        Err(error) => {
            let internal = |context: &str, message: String| {
                vec![
                    CodeGraphError::Internal {
                        context: context.into(),
                        message,
                    }
                    .into(),
                ]
            };
            let errors = match error {
                _ if cancelled() => Vec::new(),
                Error::Load(error) => internal("load", error.to_string()),
                error @ Error::Panic { .. } => internal("panic", error.to_string()),
                error => vec![
                    CodeGraphError::ArrowConversion {
                        message: error.to_string(),
                    }
                    .into(),
                ],
            };
            return Ok(IndexingReport {
                stats,
                errors,
                skipped: Vec::new(),
                limited: Vec::new(),
                faults: Vec::new(),
            });
        }
    };
    stats.slowest_files = file_timings(report.files);
    stats.language_timings = language_timings(&stats.slowest_files);
    stats.slowest_files.truncate(SLOWEST_FILES);
    stats.phase_timings.total_ms = started.elapsed().as_secs_f64() * 1000.0;
    stats.phase_timings.language_processing_ms = stats.phase_timings.total_ms;
    Ok(IndexingReport {
        stats,
        errors: Vec::new(),
        skipped: report.skipped.into_iter().map(skipped_file).collect(),
        limited: report.limited.into_iter().map(skipped_file).collect(),
        faults: report
            .unread_files
            .into_iter()
            .map(|path| FaultedFile {
                path,
                kind: FileFault::FileRead,
                detail: "source or manifest could not be read".into(),
            })
            .collect(),
    })
}

fn skipped_file(killed: code_graph_incremental::Killed) -> code_graph::v2::error::SkippedFile {
    use code_graph::v2::error::{AbortPhase, FileSkip};
    let kind = match killed.label.as_ref() {
        "resolve" if killed.path.is_empty() => FileSkip::CrossFileTimeout,
        "resolve" => FileSkip::ResolveTimeout,
        "names" => FileSkip::TooManyNames,
        "tree-sitter" => FileSkip::Timeout(AbortPhase::Parse),
        "depth" => FileSkip::DeeplyNested,
        "stack" => FileSkip::StackLimit,
        "rewrite" => FileSkip::Timeout(AbortPhase::Walk),
        "link" => FileSkip::Timeout(AbortPhase::Ssa),
        "edges" => FileSkip::TooManyEdges,
        "manifests" => FileSkip::TooManyManifestBytes,
        "fqn" => FileSkip::FqnTruncated,
        _ => FileSkip::Timeout(AbortPhase::Sentinel),
    };
    code_graph::v2::error::SkippedFile {
        detail: killed.to_string(),
        family: killed.family.map(|lang| lang.family().to_string()),
        path: killed.path,
        kind,
    }
}

const SLOWEST_FILES: usize = 100;

fn file_timings(files: HashMap<String, FileStats>) -> Vec<FileTimingEntry> {
    let mut entries: Vec<_> = files
        .into_iter()
        .map(|(path, stats)| {
            let parse_ms = stats.parse.as_secs_f64() * 1000.0;
            let resolve_ms = stats.resolve.as_secs_f64() * 1000.0;
            FileTimingEntry {
                path,
                size_bytes: stats.bytes,
                parse_ms,
                resolve_ms,
                total_ms: parse_ms + resolve_ms,
                language: <&str>::from(stats.language).to_owned(),
            }
        })
        .collect();
    entries.sort_by(|a, b| b.total_ms.total_cmp(&a.total_ms));
    entries
}

fn language_timings(files: &[FileTimingEntry]) -> Vec<LanguageTimings> {
    let mut by_language: BTreeMap<&str, LanguageTimings> = BTreeMap::new();
    for file in files {
        let timing = by_language
            .entry(&file.language)
            .or_insert_with(|| LanguageTimings {
                language: file.language.clone(),
                ..Default::default()
            });
        timing.file_count += 1;
        timing.total_bytes += file.size_bytes;
        timing.parse_ms += file.parse_ms;
        timing.resolve_ms += file.resolve_ms;
        timing.total_ms += file.total_ms;
    }
    let mut timings: Vec<_> = by_language.into_values().collect();
    timings.sort_by(|a, b| b.total_ms.total_cmp(&a.total_ms));
    timings
}
