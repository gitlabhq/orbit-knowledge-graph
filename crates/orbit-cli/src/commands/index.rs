use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, StringArray};
use code_graph::v2::{PipelineConfig, ProgressObserver, ProgressPhase, SilentProgress};
use ontology::Ontology;
use serde::Serialize;
use tracing::{Level, info};
use tracing_subscriber::fmt::format::FmtSpan;

use super::setup::spec;
use crate::tui;
use crate::workspace::{self, GitInfo};

const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

/// Per-file byte cap for local indexing; files above it are recorded as nodes
/// but not loaded or parsed.
const MAX_INDEXED_FILE_BYTES: u64 = 5_000_000;

#[derive(Serialize)]
pub(crate) struct IndexOutput {
    repository: String,
    path: String,
    time_seconds: f64,
    graph: GraphStats,
    processing: ProcessingStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    database_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detailed: Option<DetailedStats>,
}

#[derive(Serialize)]
struct GraphStats {
    directories: usize,
    files: usize,
    definitions: usize,
    imported_symbols: usize,
    relationships: usize,
}

#[derive(Serialize)]
struct ProcessingStats {
    skipped_files: usize,
    errored_files: usize,
}

#[derive(Debug, Clone, Default)]
struct IndexGraphStats {
    directories: usize,
    files: usize,
    definitions: usize,
    imported_symbols: usize,
    relationships: usize,
    relationship_types: HashMap<String, usize>,
    definition_types: HashMap<String, usize>,
}

struct IndexRunResult {
    total_processing_time: Duration,
    skipped_files: Vec<code_graph::v2::SkippedFile>,
    faulted_files: Vec<code_graph::v2::FaultedFile>,
    graph_stats: IndexGraphStats,
    database_path: Option<String>,
    slowest_files: Vec<code_graph::v2::FileTimingEntry>,
    language_timings: Vec<code_graph::v2::LanguageTimings>,
    phase_timings: code_graph::v2::PhaseTimings,
}

#[derive(Serialize)]
struct DetailedStats {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    skipped_files: Vec<SkippedFile>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errored_files: Vec<ErroredFile>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    slowest_files: Vec<SlowFile>,
    language_timings: Vec<LanguageTiming>,
    phase_timings: PhaseTiming,
    relationship_types: HashMap<String, usize>,
    definition_types: HashMap<String, usize>,
}

#[derive(Serialize)]
struct LanguageTiming {
    language: String,
    file_count: usize,
    total_bytes: u64,
    parse_ms: f64,
    graph_build_ms: f64,
    resolve_ms: f64,
    total_ms: f64,
}

#[derive(Serialize)]
struct PhaseTiming {
    file_discovery_ms: f64,
    structural_graph_ms: f64,
    language_processing_ms: f64,
    total_ms: f64,
}

#[derive(Serialize)]
struct SlowFile {
    path: String,
    language: String,
    size_bytes: u64,
    parse_ms: f64,
    resolve_ms: f64,
    total_ms: f64,
}

#[derive(Serialize)]
struct SkippedFile {
    path: String,
    reason: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    detail: String,
}

#[derive(Serialize)]
struct ErroredFile {
    path: String,
    kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    detail: String,
}

pub(crate) fn run(
    path: PathBuf,
    threads: usize,
    show_stats: bool,
    verbose: bool,
    db: Option<PathBuf>,
) -> Result<()> {
    let person_is_watching = std::io::stdout().is_terminal();
    install_tracing(verbose, person_is_watching);
    let indexer = LocalIndexer::open(path, threads, show_stats, db)?;

    if !person_is_watching {
        for output in indexer.index_all(&mut LogReporter)? {
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        return Ok(());
    }

    tui::intro("Orbit index")?;
    let mut reporter = TuiReporter::new(indexer.db_path.clone());
    match indexer.index_all(&mut reporter) {
        Ok(_) => tui::outro(reporter.closing_line()),
        Err(error) => {
            tui::outro_cancel(&error)?;
            Err(error)
        }
    }
}

/// Synchronous (the pipeline and DuckDB driver both block), so async callers
/// must wrap it in `spawn_blocking`.
pub(crate) fn collect(
    path: PathBuf,
    threads: usize,
    show_stats: bool,
    db: Option<PathBuf>,
) -> Result<Vec<IndexOutput>> {
    LocalIndexer::open(path, threads, show_stats, db)?.index_all(&mut LogReporter)
}

pub(crate) fn grep_command_line(definition_name: &str) -> String {
    let is_plain_word = definition_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_');
    let argument = match is_plain_word {
        true => definition_name.to_string(),
        false => format!("'{}'", definition_name.replace('\'', "'\\''")),
    };
    format!("{} grep {argument}", spec::launcher())
}

pub(crate) fn most_referenced_definition(repo: &Path, db: Option<PathBuf>) -> Option<String> {
    let git = workspace::git_info(&workspace::git_toplevel(repo).ok()?).ok()?;
    let batches = crate::sql::open_graph(db)
        .ok()?
        .query_arrow_json(
            "SELECT d.name FROM gl_definition d JOIN gl_edge e ON e.target_id = d.id \
             WHERE d.project_id = ?1 AND d.commit_sha = ?2 AND length(d.name) > 3 \
             GROUP BY d.name ORDER BY count(*) DESC, d.name LIMIT 1",
            &[git.project_id.into(), git.commit_sha.into()],
        )
        .ok()?;
    let names = batches
        .first()?
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()?;
    (!names.is_empty()).then(|| names.value(0).to_string())
}

fn install_tracing(verbose: bool, quiet: bool) {
    let level = match (verbose, quiet) {
        (true, _) => Level::DEBUG,
        (false, true) => Level::ERROR,
        (false, false) => Level::WARN,
    };
    let span_events = match verbose {
        true => FmtSpan::CLOSE,
        false => FmtSpan::NONE,
    };
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(verbose)
        .with_level(verbose)
        .with_ansi(true)
        .without_time()
        .with_span_events(span_events)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

pub(crate) trait IndexReporter {
    fn repository_started(&mut self, _git: &GitInfo) -> Arc<dyn ProgressObserver> {
        Arc::new(SilentProgress)
    }
    fn repository_indexed(&mut self, _output: &IndexOutput) {}
    fn repository_failed(&mut self, repo_path: &Path, error: &anyhow::Error);
}

struct LogReporter;

impl IndexReporter for LogReporter {
    fn repository_failed(&mut self, repo_path: &Path, error: &anyhow::Error) {
        tracing::error!("failed to index {}: {error:#}", repo_path.display());
    }
}

struct TuiReporter {
    db_path: PathBuf,
    bars: Option<Arc<RepositoryBars>>,
    suggested_grep: Option<String>,
}

impl TuiReporter {
    fn new(db_path: PathBuf) -> Self {
        Self {
            db_path,
            bars: None,
            suggested_grep: None,
        }
    }

    fn closing_line(&self) -> String {
        match &self.suggested_grep {
            Some(name) => format!("Try it:  {}", grep_command_line(name)),
            None => "Done.".to_string(),
        }
    }
}

impl IndexReporter for TuiReporter {
    fn repository_started(&mut self, git: &GitInfo) -> Arc<dyn ProgressObserver> {
        let title = format!(
            "{}  {} @ {}",
            repository_name(git),
            git.branch,
            git.short_sha()
        );
        let bars = Arc::new(RepositoryBars::open(title));
        self.bars = Some(bars.clone());
        bars
    }

    fn repository_indexed(&mut self, output: &IndexOutput) {
        if let Some(bars) = self.bars.take() {
            bars.close(output);
        }

        if let Some(detailed) = &output.detailed {
            let _ = tui::card("Timings", format_timings(detailed));
        }

        if self.suggested_grep.is_none() {
            self.suggested_grep =
                most_referenced_definition(Path::new(&output.path), Some(self.db_path.clone()));
        }
    }

    fn repository_failed(&mut self, repo_path: &Path, error: &anyhow::Error) {
        match self.bars.take() {
            Some(bars) => bars.fail(error),
            None => tui::error(format!("{}: {error:#}", repo_path.display())),
        }
    }
}

struct RepositoryBars {
    group: tui::ProgressGroup,
    phases: OnceLock<PhaseBars>,
}

struct PhaseBars {
    parse: tui::Bar,
    resolve: tui::Bar,
}

impl RepositoryBars {
    fn open(title: String) -> Self {
        Self {
            group: tui::progress_group(title),
            phases: OnceLock::new(),
        }
    }

    fn close(&self, output: &IndexOutput) {
        if let Some(phases) = self.phases.get() {
            phases
                .parse
                .finish(count_noun(output.graph.definitions, "definition"));
            phases.resolve.finish(format!(
                "{} · {:.1} s",
                count_noun(output.graph.relationships, "relationship"),
                output.time_seconds
            ));
        }
        self.group.close();
    }

    fn fail(&self, error: &anyhow::Error) {
        self.group.fail(format!("{error:#}"));
    }
}

impl ProgressObserver for RepositoryBars {
    fn discovery_finished(
        &self,
        total_files: usize,
        parseable_files: usize,
        files_per_family: &[(String, usize)],
    ) {
        self.group.note(format!(
            "{} · {} parseable · {}",
            count_noun(total_files, "file"),
            tui::format_with_thousands(parseable_files),
            count_noun(files_per_family.len(), "language")
        ));
        self.phases.get_or_init(|| PhaseBars {
            parse: self.group.bar("Parse  ", parseable_files),
            resolve: self.group.bar("Resolve", parseable_files),
        });
    }

    fn files_advanced(&self, phase: ProgressPhase, count: usize) {
        let Some(phases) = self.phases.get() else {
            return;
        };
        match phase {
            ProgressPhase::Parse => phases.parse.advance(count),
            ProgressPhase::Resolve => phases.resolve.advance(count),
        }
    }
}

fn count_noun(count: usize, noun: &str) -> String {
    let plural = match count {
        1 => "",
        _ => "s",
    };
    format!("{} {noun}{plural}", tui::format_with_thousands(count))
}

fn format_timings(detailed: &DetailedStats) -> String {
    let phases = &detailed.phase_timings;
    let mut rows = vec![format!(
        "discovery {:.0} ms · structure {:.0} ms · languages {:.0} ms · total {:.0} ms",
        phases.file_discovery_ms,
        phases.structural_graph_ms,
        phases.language_processing_ms,
        phases.total_ms
    )];
    rows.extend(detailed.language_timings.iter().map(|timing| {
        format!(
            "{:<12} {:>6} files  parse {:>7.0} ms  resolve {:>7.0} ms",
            timing.language, timing.file_count, timing.parse_ms, timing.resolve_ms
        )
    }));
    rows.join("\n")
}

fn repository_name(git: &GitInfo) -> String {
    git.repo_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "repository".to_string())
}

struct LocalIndexer {
    db_path: PathBuf,
    repos: Vec<PathBuf>,
    ontology: Ontology,
    pipeline_config: PipelineConfig,
    show_stats: bool,
}

impl LocalIndexer {
    fn open(path: PathBuf, threads: usize, show_stats: bool, db: Option<PathBuf>) -> Result<Self> {
        let db_path = workspace::resolve_db_path(db)?;
        let repos = workspace::Workspace::open_default()?.resolve_repos(&path)?;
        if repos.is_empty() {
            bail!(
                "no git repository found in {}. Pass a repository path, or a directory containing one.",
                path.display()
            );
        }

        let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
        workspace::ensure_graph_schema(&db_path, LOCAL_DDL)?;

        let pipeline_config = PipelineConfig {
            worker_threads: threads,
            per_file_timeout: Some(Duration::from_secs(2)),
            per_file_parse_timeout: Some(Duration::from_millis(100)),
            per_file_walk_timeout: Some(Duration::from_millis(100)),
            per_file_ssa_timeout: Some(Duration::from_millis(100)),
            cross_file_resolve_timeout: Some(Duration::from_secs(180)),
            ..Default::default()
        };

        Ok(Self {
            db_path,
            repos,
            ontology,
            pipeline_config,
            show_stats,
        })
    }

    fn index_all(&self, reporter: &mut dyn IndexReporter) -> Result<Vec<IndexOutput>> {
        let mut outputs = Vec::with_capacity(self.repos.len());
        for repo_path in &self.repos {
            match self.index_repository(repo_path, reporter) {
                Ok(output) => {
                    reporter.repository_indexed(&output);
                    outputs.push(output);
                }
                Err(error) => reporter.repository_failed(repo_path, &error),
            }
        }

        let failed = self.repos.len() - outputs.len();
        if failed > 0 {
            bail!(
                "{failed} of {} repositories failed to index",
                self.repos.len()
            );
        }
        Ok(outputs)
    }

    fn index_repository(
        &self,
        repo_path: &Path,
        reporter: &mut dyn IndexReporter,
    ) -> Result<IndexOutput> {
        let git = workspace::git_info(repo_path).inspect_err(|error| {
            workspace::record_git_info_failure(&self.db_path, repo_path, &error.to_string())
        })?;
        let key = git.repo_path.to_string_lossy().to_string();
        info!(
            "Indexing repository at: {key} (branch: {}, commit: {})",
            git.branch,
            git.short_sha()
        );
        self.record_status(
            &key,
            &git,
            workspace::RepoStatus::Indexing,
            None,
            Some(&git),
        )?;

        let mut pipeline_config = self.pipeline_config.clone();
        pipeline_config.progress = reporter.repository_started(&git);
        let result = index_repo(&git, &self.db_path, &self.ontology, pipeline_config).inspect_err(
            |error| {
                let recorded = self.record_status(
                    &key,
                    &git,
                    workspace::RepoStatus::Error,
                    Some(&error.to_string()),
                    None,
                );
                if let Err(manifest_error) = recorded {
                    tracing::warn!("failed to record error status in manifest: {manifest_error}");
                }
            },
        )?;
        Ok(build_index_output(
            &repository_name(&git),
            &key,
            &result,
            self.show_stats,
        ))
    }

    fn record_status(
        &self,
        key: &str,
        git: &GitInfo,
        status: workspace::RepoStatus,
        error: Option<&str>,
        git_for_manifest: Option<&GitInfo>,
    ) -> Result<()> {
        let client =
            duckdb_client::DuckDbClient::open(&self.db_path).context("failed to open DuckDB")?;
        workspace::set_status(
            &client,
            key,
            git.project_id,
            status,
            error,
            git_for_manifest,
        )
    }
}

fn fatal_pipeline_reason(errors: &[code_graph::v2::pipeline::PipelineError]) -> Option<String> {
    let fatal_count = errors.iter().filter(|e| e.fatal).count();
    let first = errors.iter().find(|e| e.fatal)?;
    Some(format!(
        "code indexing failed during {}: {} ({fatal_count} fatal pipeline error(s))",
        first.stage, first.error
    ))
}

fn index_repo(
    git: &workspace::GitInfo,
    db_path: &std::path::Path,
    ontology: &Ontology,
    pipeline_config: code_graph::v2::PipelineConfig,
) -> Result<IndexRunResult> {
    let key = git.repo_path.to_string_lossy().to_string();
    let root_path = key.clone();
    let start_time = std::time::Instant::now();

    let tracer = code_graph::v2::trace::Tracer::new(false);
    let mut filter = code_graph::v2::config::CodeFilter::new(
        Some(MAX_INDEXED_FILE_BYTES),
        None,
        code_graph::v2::config::detect_language_from_path,
    );
    let file_inventory = std::sync::Arc::new(
        orbit_utils::fs_walk::walk_dir(&git.repo_path, &mut filter)
            .context("failed to walk repository files")?,
    );

    let client =
        duckdb_client::DuckDbClient::open(db_path).context("failed to open DuckDB for writing")?;

    let node_tables: Vec<String> = ontology
        .local_entity_names()
        .iter()
        .map(|name| {
            ontology
                .get_node(name)
                .expect("local entity must exist")
                .destination_table
                .clone()
        })
        .collect();
    let edge_table = ontology
        .local_edge_table_name()
        .context("local_db.edge_table.name must be configured")?;

    client
        .delete_project(git.project_id, &node_tables, edge_table)
        .context("failed to clear existing project data")?;
    client
        .execute(
            &format!(
                "DROP TABLE IF EXISTS {}",
                duckdb_client::search::def_doc_table(git.project_id)
            ),
            &[],
        )
        .context("failed to clear existing search index")?;

    let converter: std::sync::Arc<dyn code_graph::v2::GraphConverter> =
        std::sync::Arc::new(duckdb_client::DuckDbConverter {
            project_id: git.project_id,
            branch: git.branch.clone(),
            commit_sha: git.commit_sha.clone(),
            ontology: std::sync::Arc::new(ontology.clone()),
        });
    let client = std::sync::Mutex::new(client);
    let on_batch: std::sync::Arc<code_graph::v2::OnBatch> = std::sync::Arc::new(
        move |table: &str, batch: arrow::record_batch::RecordBatch| {
            if batch.num_rows() == 0 {
                return Ok(());
            }
            client
                .lock()
                .unwrap()
                .insert_batch(table, &batch)
                .map_err(|e| code_graph::v2::SinkError(format!("DuckDB write to {table}: {e}")))
        },
    );

    let v2_result = code_graph::v2::Pipeline::run_with_tracer(
        std::path::Path::new(&root_path),
        file_inventory,
        pipeline_config.clone(),
        tracer,
        converter,
        on_batch,
    );

    for err in &v2_result.errors {
        tracing::warn!(stage = err.stage, error = %err.error, file = %err.file_path, "pipeline error");
    }
    if let Some(reason) = fatal_pipeline_reason(&v2_result.errors) {
        anyhow::bail!(reason);
    }

    let client =
        duckdb_client::DuckDbClient::open(db_path).context("failed to open DuckDB for status")?;
    let doc_table = duckdb_client::search::def_doc_table(git.project_id);
    client
        .load_extension("fts")
        .context("failed to load the DuckDB fts extension")?;
    client
        .execute(
            &duckdb_client::search::def_doc_sql(&doc_table, ontology)?,
            &[
                serde_json::json!(git.project_id),
                serde_json::json!(git.commit_sha),
            ],
        )
        .context("failed to build the search documents")?;
    duckdb_client::search::populate_def_doc_sources(
        &client,
        &doc_table,
        ontology,
        &git.repo_path,
        git.project_id,
        &git.commit_sha,
    )
    .context("failed to add definition sources to the search documents")?;
    client
        .execute(
            &duckdb_client::search::create_fts_index_sql(&doc_table),
            &[],
        )
        .context("failed to build the search index")?;
    workspace::set_status(
        &client,
        &key,
        git.project_id,
        workspace::RepoStatus::Indexed,
        None,
        Some(git),
    )?;

    Ok(IndexRunResult {
        total_processing_time: start_time.elapsed(),
        skipped_files: v2_result.skipped,
        faulted_files: v2_result.faults,
        graph_stats: IndexGraphStats {
            directories: v2_result.stats.directories_indexed,
            files: v2_result.stats.files_indexed,
            definitions: v2_result.stats.definitions_count,
            imported_symbols: v2_result.stats.imports_count,
            relationships: v2_result.stats.edges_count,
            relationship_types: HashMap::new(),
            definition_types: HashMap::new(),
        },
        database_path: Some(db_path.display().to_string()),
        slowest_files: v2_result.stats.slowest_files,
        language_timings: v2_result.stats.language_timings,
        phase_timings: v2_result.stats.phase_timings,
    })
}

fn build_index_output(
    repo_name: &str,
    path: &str,
    result: &IndexRunResult,
    show_stats: bool,
) -> IndexOutput {
    let stats = &result.graph_stats;
    let graph = GraphStats {
        directories: stats.directories,
        files: stats.files,
        definitions: stats.definitions,
        imported_symbols: stats.imported_symbols,
        relationships: stats.relationships,
    };

    let detailed = show_stats.then(|| DetailedStats {
        skipped_files: result
            .skipped_files
            .iter()
            .map(|s| SkippedFile {
                path: s.path.clone(),
                reason: s.kind.as_metric_label().to_string(),
                detail: s.detail.clone(),
            })
            .collect(),
        errored_files: result
            .faulted_files
            .iter()
            .map(|f| ErroredFile {
                path: f.path.clone(),
                kind: f.kind.as_metric_label().to_string(),
                detail: f.detail.clone(),
            })
            .collect(),
        slowest_files: result
            .slowest_files
            .iter()
            .map(|f| SlowFile {
                path: f.path.clone(),
                language: f.language.clone(),
                size_bytes: f.size_bytes,
                parse_ms: (f.parse_ms * 100.0).round() / 100.0,
                resolve_ms: (f.resolve_ms * 100.0).round() / 100.0,
                total_ms: (f.total_ms * 100.0).round() / 100.0,
            })
            .collect(),
        language_timings: result
            .language_timings
            .iter()
            .map(|lt| LanguageTiming {
                language: lt.language.clone(),
                file_count: lt.file_count,
                total_bytes: lt.total_bytes,
                parse_ms: (lt.parse_ms * 100.0).round() / 100.0,
                graph_build_ms: (lt.graph_build_ms * 100.0).round() / 100.0,
                resolve_ms: (lt.resolve_ms * 100.0).round() / 100.0,
                total_ms: (lt.total_ms * 100.0).round() / 100.0,
            })
            .collect(),
        phase_timings: PhaseTiming {
            file_discovery_ms: (result.phase_timings.file_discovery_ms * 100.0).round() / 100.0,
            structural_graph_ms: (result.phase_timings.structural_graph_ms * 100.0).round() / 100.0,
            language_processing_ms: (result.phase_timings.language_processing_ms * 100.0).round()
                / 100.0,
            total_ms: (result.phase_timings.total_ms * 100.0).round() / 100.0,
        },
        relationship_types: stats.relationship_types.clone(),
        definition_types: stats.definition_types.clone(),
    });

    IndexOutput {
        repository: repo_name.to_string(),
        path: path.to_string(),
        time_seconds: result.total_processing_time.as_secs_f64(),
        graph,
        processing: ProcessingStats {
            skipped_files: result.skipped_files.len(),
            errored_files: result.faulted_files.len(),
        },
        database_path: result.database_path.clone(),
        detailed,
    }
}

#[cfg(test)]
mod tests {
    use super::fatal_pipeline_reason;
    use code_graph::v2::pipeline::PipelineError;

    fn pipeline_error(stage: &'static str, msg: &str, fatal: bool) -> PipelineError {
        PipelineError {
            file_path: String::new(),
            error: msg.to_string(),
            stage,
            fatal,
        }
    }

    #[test]
    fn no_errors_is_not_fatal() {
        assert!(fatal_pipeline_reason(&[]).is_none());
    }

    #[test]
    fn non_fatal_errors_do_not_bail() {
        let errors = [
            pipeline_error("parse", "bad syntax", false),
            pipeline_error("walk", "skip", false),
        ];
        assert!(fatal_pipeline_reason(&errors).is_none());
    }

    #[test]
    fn a_fatal_error_bails_with_first_reason_and_count() {
        let errors = [
            pipeline_error("parse", "recoverable", false),
            pipeline_error("sink_write", "DuckDB write failed", true),
            pipeline_error("conversion", "arrow overflow", true),
        ];
        let reason = fatal_pipeline_reason(&errors).expect("fatal must bail");
        assert!(reason.contains("sink_write"), "{reason}");
        assert!(reason.contains("DuckDB write failed"), "{reason}");
        assert!(reason.contains("2 fatal"), "{reason}");
    }
}
