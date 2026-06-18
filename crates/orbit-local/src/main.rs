#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod descriptions;
mod list;
mod mcp;
mod sql;
mod sql_format;
mod workspace;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gitalisk_core::repository::gitalisk_repository::IterFileOptions;
use ontology::Ontology;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Level, info};
use tracing_subscriber::fmt::format::FmtSpan;

const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

#[derive(Serialize)]
struct IndexOutput {
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

#[derive(Parser)]
#[command(name = "orbit", version = env!("ORBIT_VERSION"))]
#[command(about = "Orbit - local code indexing and query CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the version string and exit.
    Version,
    #[command(about = descriptions::INDEX_SHORT)]
    Index {
        /// Path to the repository to index
        #[arg(value_name = "PATH")]
        path: PathBuf,

        /// Number of worker threads (0 = auto-detect based on CPU cores)
        #[arg(short, long, default_value = "0")]
        threads: usize,

        /// Include detailed statistics in output
        #[arg(short, long)]
        stats: bool,

        /// Verbose logging to stderr
        #[arg(short, long)]
        verbose: bool,
    },
    #[command(about = descriptions::RUN_SQL_SHORT)]
    Sql {
        /// SQL query, or `-` to read from stdin.
        #[arg(value_name = "QUERY", conflicts_with = "file")]
        query: Option<String>,

        /// Read SQL from a file.
        #[arg(long, short, value_name = "PATH")]
        file: Option<PathBuf>,

        /// Output format.
        #[arg(long, short = 'F', default_value = "table")]
        format: sql_format::Format,

        /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
        #[arg(long, value_name = "PATH")]
        db: Option<PathBuf>,
    },
    #[command(about = descriptions::GET_SCHEMA_SHORT)]
    Schema {
        /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
        #[arg(long, value_name = "PATH")]
        db: Option<PathBuf>,

        /// Emit JSON instead of the default table view.
        #[arg(long)]
        raw: bool,

        /// Optional table names to scope the output.
        /// When provided, only columns for those tables are shown.
        /// e.g. `orbit schema gl_definition gl_edge`
        #[arg(value_name = "TABLE")]
        tables: Vec<String>,
    },
    /// List the repositories indexed in the local DuckDB graph.
    List {
        /// Output format.
        #[arg(long, short = 'F', default_value = "table")]
        format: sql_format::Format,

        /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
        #[arg(long, value_name = "PATH")]
        db: Option<PathBuf>,
    },
    #[command(about = descriptions::MCP_SERVE_SHORT)]
    #[command(long_about = "Serve the local graph to MCP-compatible AI agents.\n\n\
                      Plug into editors that support MCP (Claude Code, Cursor, OpenCode, Codex) \
                      so the agent can call `run_sql`, `get_graph_schema`, and `index`.")]
    Mcp {
        #[command(subcommand)]
        command: McpCommands,
    },
}

#[derive(Subcommand)]
enum McpCommands {
    /// Start a stateless MCP server over stdio.
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("{}", env!("ORBIT_VERSION"));
            return Ok(());
        }
        Commands::Index {
            path,
            threads,
            stats,
            verbose,
        } => {
            let level = if verbose { Level::DEBUG } else { Level::WARN };
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(level)
                .with_target(verbose)
                .with_level(verbose)
                .with_ansi(true)
                .without_time()
                .with_span_events(if verbose {
                    FmtSpan::CLOSE
                } else {
                    FmtSpan::NONE
                })
                .with_writer(std::io::stderr)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            run_index(path, threads, stats).await
        }
        Commands::Sql {
            query,
            file,
            format,
            db,
        } => sql::run(query, file, format, db),
        Commands::Schema { db, raw, tables } => run_schema(db, raw, tables),
        Commands::List { format, db } => list::run(format, db),
        Commands::Mcp {
            command: McpCommands::Serve,
        } => {
            // Logs must go to stderr only — stdout is the MCP transport.
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(Level::INFO)
                .with_target(false)
                .with_ansi(false)
                .without_time()
                .with_writer(std::io::stderr)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
            mcp::serve().await
        }
    }
}

fn run_schema(db: Option<PathBuf>, raw: bool, tables: Vec<String>) -> Result<()> {
    let client = sql::open_graph(db)?;

    let batches = if tables.is_empty() {
        sql::query(&client, sql::SCHEMA_INTROSPECTION_SQL)?
    } else {
        let placeholders = vec!["?"; tables.len()].join(", ");
        let query = format!(
            "SELECT table_name, column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = 'main' \
             AND table_name IN ({placeholders}) \
             ORDER BY table_name, ordinal_position"
        );
        let params: Vec<serde_json::Value> = tables.iter().map(|t| serde_json::json!(t)).collect();
        let batches = client
            .query_arrow_json(&query, &params)
            .context("failed to read information_schema.columns")?;

        let found: std::collections::HashSet<String> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("table_name")
                    .and_then(|col| col.as_any().downcast_ref::<arrow::array::StringArray>())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|s| s.map(String::from))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect();
        let missing: Vec<_> = tables.iter().filter(|t| !found.contains(*t)).collect();
        if !missing.is_empty() {
            anyhow::bail!(
                "no table named {} in the local graph. Run `orbit schema` to list tables.",
                missing
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        batches
    };

    let stdout = std::io::stdout().lock();
    if raw {
        sql_format::write_json(stdout, &batches)
    } else {
        sql_format::write_table(stdout, &batches)
    }
}

async fn run_index(path: PathBuf, threads: usize, show_stats: bool) -> Result<()> {
    for output in index_collect(path, threads, show_stats)? {
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}

/// Synchronous (the pipeline and DuckDB driver both block), so async callers
/// must wrap it in `spawn_blocking`.
pub(crate) fn index_collect(
    path: PathBuf,
    threads: usize,
    show_stats: bool,
) -> Result<Vec<IndexOutput>> {
    let store = workspace::Workspace::open_default()?;
    let repos = store.resolve_repos(&path)?;

    if repos.is_empty() {
        info!("No git repositories found in {}", path.display());
        return Ok(Vec::new());
    }

    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;

    // Ensure schema exists, then drop the connection so we don't hold
    // the write lock during parsing.
    {
        let db_path = store.db_path();
        let client =
            duckdb_client::DuckDbClient::open(&db_path).context("failed to open DuckDB")?;
        client
            .initialize_schema(LOCAL_DDL)
            .context("failed to create schema")?;
    }

    let pipeline_config = code_graph::v2::PipelineConfig {
        max_file_size: 5_000_000,
        worker_threads: threads,
        per_file_timeout: Some(std::time::Duration::from_secs(2)),
        per_file_parse_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_walk_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_ssa_timeout: Some(std::time::Duration::from_millis(100)),
        cross_file_resolve_timeout: Some(std::time::Duration::from_secs(180)),
        ..Default::default()
    };

    let mut failed = 0usize;
    let mut outputs = Vec::with_capacity(repos.len());

    for repo_path in &repos {
        let git = match workspace::git_info(repo_path) {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("skipping {}: {e:#}", repo_path.display());
                failed += 1;
                continue;
            }
        };
        let key = git.repo_path.to_string_lossy().to_string();
        let db_path = store.db_path();

        info!(
            "Indexing repository at: {} (branch: {}, commit: {})",
            key,
            git.branch,
            git.commit_sha.get(..8).unwrap_or(&git.commit_sha)
        );

        // Mark as indexing before we start parsing.
        {
            let client =
                duckdb_client::DuckDbClient::open(&db_path).context("failed to open DuckDB")?;
            workspace::set_status(
                &client,
                &key,
                git.project_id,
                workspace::RepoStatus::Indexing,
                None,
                Some(&git),
            )?;
        }

        let result = index_repo(&git, &store, &ontology, pipeline_config.clone());
        match result {
            Ok(result) => {
                let repo_name = git
                    .repo_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "repository".to_string());
                let mut output = build_index_output(&repo_name, &key, &result, show_stats);
                output.database_path = Some(db_path.display().to_string());
                outputs.push(output);
            }
            Err(e) => {
                tracing::error!("failed to index {key}: {e:#}");
                failed += 1;
                if let Ok(client) = duckdb_client::DuckDbClient::open(&db_path)
                    && let Err(manifest_err) = workspace::set_status(
                        &client,
                        &key,
                        git.project_id,
                        workspace::RepoStatus::Error,
                        Some(&e.to_string()),
                        None,
                    )
                {
                    tracing::warn!("failed to record error status in manifest: {manifest_err}");
                }
            }
        }
    }

    if failed > 0 {
        anyhow::bail!("{failed} of {} repositories failed to index", repos.len());
    }
    Ok(outputs)
}

fn index_repo(
    git: &workspace::GitInfo,
    store: &workspace::Workspace,
    ontology: &Ontology,
    pipeline_config: code_graph::v2::PipelineConfig,
) -> Result<IndexRunResult> {
    let key = git.repo_path.to_string_lossy().to_string();
    let root_path = key.clone();
    let start_time = std::time::Instant::now();

    let tracer = code_graph::v2::trace::Tracer::new(false);
    let file_inventory = gitalisk_file_inventory(git)?;

    let db_path = store.db_path();
    let client =
        duckdb_client::DuckDbClient::open(&db_path).context("failed to open DuckDB for writing")?;

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

    let converter: std::sync::Arc<dyn code_graph::v2::GraphConverter> =
        std::sync::Arc::new(duckdb_client::DuckDbConverter {
            project_id: git.project_id,
            branch: git.branch.clone(),
            commit_sha: git.commit_sha.clone(),
            ontology: std::sync::Arc::new(ontology.clone()),
        });
    let sink: std::sync::Arc<dyn code_graph::v2::BatchSink> =
        std::sync::Arc::new(duckdb_client::DuckDbSink::new(client));

    let v2_result = code_graph::v2::Pipeline::run_with_tracer(
        std::path::Path::new(&root_path),
        file_inventory,
        pipeline_config.clone(),
        tracer,
        converter,
        sink,
    );

    if !v2_result.errors.is_empty() {
        for err in &v2_result.errors {
            tracing::warn!("pipeline error: {} ({})", err.error, err.file_path);
        }
    }
    // Re-open for workspace status (client was moved into sink)
    let client =
        duckdb_client::DuckDbClient::open(&db_path).context("failed to open DuckDB for status")?;
    workspace::set_status(
        &client,
        &key,
        git.project_id,
        workspace::RepoStatus::Indexed,
        None,
        Some(git),
    )?;

    for err in &v2_result.errors {
        tracing::warn!(stage = err.stage, error = %err.error, "task-level pipeline error");
    }

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

fn gitalisk_file_inventory(
    git: &workspace::GitInfo,
) -> Result<Arc<[code_graph::v2::FileInventoryEntry]>> {
    let files = git
        .repository()
        .get_repo_files(IterFileOptions {
            include_ignored: false,
            include_hidden: true,
            exclude_patterns: Vec::new(),
        })
        .with_context(|| {
            format!(
                "failed to list repository files with Gitalisk in {}",
                git.repo_path.display()
            )
        })?;

    let entries: Vec<_> = files
        .into_iter()
        .map(|file| {
            let path = file.path();
            let relative_path = path
                .strip_prefix(&git.repo_path)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string();
            let size = std::fs::symlink_metadata(path)
                .with_context(|| format!("failed to read metadata for {}", path.display()))?
                .len();
            Ok(code_graph::v2::FileInventoryEntry {
                path: relative_path,
                size,
            })
        })
        .collect::<Result<_>>()?;
    Ok(Arc::from(entries))
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
