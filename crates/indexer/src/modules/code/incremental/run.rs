use std::sync::Arc;

use super::super::pipeline::IndexingReport;
use code_graph::v2::{OnBatch, SinkError};
use code_graph_incremental::{Envelope, Scalar, export::ExportConfig, templates};

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
    use code_graph::v2::error::{AbortPhase, FaultedFile, FileFault, FileSkip, SkippedFile};
    use code_graph::v2::pipeline::{PhaseTimings, PipelineStats};
    let started = std::time::Instant::now();
    let mut stats = PipelineStats {
        files_discovered: repository.usage().files,
        bytes_discovered: repository.usage().bytes,
        files_parsed: code_graph_incremental::pipeline::candidates(&repository, options.max_files)
            .count(),
        files_skipped: repository
            .files()
            .filter(|file| matches!(file.decision(), orbit_utils::vfs::Decision::List(_)))
            .count(),
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
            let batches = output.convert(batches)?;
            stats.files_indexed += batches.files.num_rows();
            stats.directories_indexed += batches.directories.num_rows();
            stats.definitions_count += batches.definitions.num_rows();
            stats.imports_count += batches.imported_symbols.num_rows();
            stats.edges_count += batches.edges.num_rows();
            for (table, batch) in route_graph(batches, &tables)? {
                if cancelled() {
                    return Err(SinkError("indexing cancelled".into()));
                }
                on_batch(&table, batch)?;
            }
            Ok(())
        },
    )
    .map_err(|error| SinkError(error.to_string()))?;
    stats.phase_timings.total_ms = started.elapsed().as_secs_f64() * 1000.0;
    stats.phase_timings.language_processing_ms = stats.phase_timings.total_ms;
    Ok(IndexingReport {
        stats,
        errors: Vec::new(),
        skipped: report
            .skipped
            .into_iter()
            .map(|killed| {
                let phase = match killed.label {
                    "tree-sitter" => AbortPhase::Parse,
                    "rewrite" => AbortPhase::Walk,
                    "link" => AbortPhase::Ssa,
                    _ => AbortPhase::Sentinel,
                };
                SkippedFile {
                    detail: killed.to_string(),
                    path: killed.path,
                    kind: FileSkip::Timeout(phase),
                }
            })
            .collect(),
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
