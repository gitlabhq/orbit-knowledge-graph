mod content;
mod local_pipeline;
mod workspace;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use code_graph::linker::indexer::{IndexingConfig, RepositoryIndexer, RepositoryIndexingResult};
use code_graph::linker::loading::DirectoryFileSource;
use ontology::Ontology;
use query_engine::compiler::SecurityContext;
use query_engine::formatters::{self, ResultFormatter};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{Level, info};
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
enum OutputFormat {
    #[default]
    Pretty,
    Json,
}

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

#[derive(Serialize)]
struct DetailedStats {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    skipped_files: Vec<SkippedFile>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errored_files: Vec<ErroredFile>,
    relationship_types: HashMap<String, usize>,
    definition_types: HashMap<String, usize>,
}

#[derive(Serialize)]
struct SkippedFile {
    path: String,
    reason: String,
}

#[derive(Serialize)]
struct ErroredFile {
    path: String,
    error: String,
}

#[derive(Parser)]
#[command(name = "orbit")]
#[command(about = "Orbit - local code indexing and query CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Index a code repository and output graph statistics as JSON
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
    /// Query the local DuckDB graph (~/.orbit/graph.duckdb)
    Query {
        /// JSON query payload
        #[arg(value_name = "JSON")]
        json: String,

        /// Path to ontology directory (default: config/ontology)
        #[arg(long, short)]
        ontology: Option<PathBuf>,

        /// Output raw JSON graph (default is LLM-friendly text)
        #[arg(long)]
        raw: bool,
    },
    /// Compile a query to SQL without executing it
    Compile {
        /// JSON query payload
        #[arg(value_name = "JSON")]
        json: String,

        /// Traversal paths for security context (e.g., "1/2/3/"). Org ID is parsed from the first segment.
        #[arg(long, short, num_args = 1..)]
        traversal_paths: Vec<String>,

        /// Path to ontology directory (default: config/ontology)
        #[arg(long, short)]
        ontology: Option<PathBuf>,

        /// Output format: pretty (default) or json
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,

        /// Compile for local DuckDB instead of ClickHouse
        #[arg(long)]
        local: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Index {
            path,
            threads,
            stats,
            verbose,
        } => {
            let level = if verbose { Level::DEBUG } else { Level::INFO };
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
        Commands::Query {
            json,
            ontology,
            raw,
        } => {
            let output = run_local_query(json, ontology)?;
            if raw {
                let formatted = formatters::GraphFormatter.format(&output);
                println!("{}", serde_json::to_string(&formatted)?);
            } else {
                let formatted = formatters::GoonFormatter.format(&output);
                println!("{}", serde_json::to_string_pretty(&formatted)?);
            }
            Ok(())
        }
        Commands::Compile {
            json,
            traversal_paths,
            ontology,
            format,
            local,
        } => run_compile(json, traversal_paths, ontology, format, local),
    }
}

async fn run_index(path: PathBuf, threads: usize, show_stats: bool) -> Result<()> {
    let store = workspace::Workspace::open_default()?;
    let repos = store.resolve_repos(&path)?;

    if repos.is_empty() {
        info!("No git repositories found in {}", path.display());
        return Ok(());
    }

    let ontology_dir = std::path::PathBuf::from(env!("ONTOLOGY_DIR"));
    let ontology = Ontology::load_from_dir(&ontology_dir).context("failed to load ontology")?;

    // Ensure schema exists, then drop the connection so we don't hold
    // the write lock during parsing.
    {
        let db_path = store.db_path();
        let client =
            duckdb_client::DuckDbClient::open(&db_path).context("failed to open DuckDB")?;
        client
            .initialize_schema()
            .context("failed to create schema")?;
    }

    let config = IndexingConfig {
        worker_threads: threads,
        max_file_size: 5_000_000,
        respect_gitignore: true,
    };

    let mut failed = 0usize;

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

        match index_repo(&git, &config, &store, &ontology).await {
            Ok(result) => {
                let repo_name = git
                    .repo_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "repository".to_string());
                let mut output = build_index_output(&repo_name, &key, &result, show_stats);
                output.database_path = Some(db_path.display().to_string());
                info!("{}", serde_json::to_string_pretty(&output)?);
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
    Ok(())
}

async fn index_repo(
    git: &workspace::GitInfo,
    config: &IndexingConfig,
    store: &workspace::Workspace,
    ontology: &Ontology,
) -> Result<RepositoryIndexingResult> {
    let key = git.repo_path.to_string_lossy().to_string();
    let repo_name = git
        .repo_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "repository".to_string());

    let file_source = DirectoryFileSource::new(key.clone());
    let indexer = RepositoryIndexer::new(repo_name, key.clone());

    let mut result = indexer
        .index_files(file_source, config)
        .await
        .context("indexing failed")?;

    if let Some(ref mut graph_data) = result.graph_data {
        graph_data.assign_node_ids(git.project_id, &git.branch);

        let local_data = duckdb_client::convert_graph_data(
            graph_data,
            git.project_id,
            &git.branch,
            &git.commit_sha,
            ontology,
        )
        .context("failed to convert graph data to Arrow")?;

        let db_path = store.db_path();
        let client = duckdb_client::DuckDbClient::open(&db_path)
            .context("failed to open DuckDB for writing")?;

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
            .insert_graph(local_data)
            .context("failed to insert graph data")?;
        workspace::set_status(
            &client,
            &key,
            git.project_id,
            workspace::RepoStatus::Indexed,
            None,
            Some(git),
        )?;
    }

    Ok(result)
}

fn build_index_output(
    repo_name: &str,
    path: &str,
    result: &RepositoryIndexingResult,
    show_stats: bool,
) -> IndexOutput {
    let (graph, rel_counts, def_counts) = match result.graph_data {
        Some(ref gd) => {
            let mut rel_counts: HashMap<String, usize> = HashMap::new();
            for rel in &gd.relationships {
                *rel_counts
                    .entry(format!("{:?}", rel.relationship_type))
                    .or_default() += 1;
            }
            let mut def_counts: HashMap<String, usize> = HashMap::new();
            for def in &gd.definition_nodes {
                *def_counts
                    .entry(format!("{:?}", def.definition_type))
                    .or_default() += 1;
            }
            (
                GraphStats {
                    directories: gd.directory_nodes.len(),
                    files: gd.file_nodes.len(),
                    definitions: gd.definition_nodes.len(),
                    imported_symbols: gd.imported_symbol_nodes.len(),
                    relationships: gd.relationships.len(),
                },
                rel_counts,
                def_counts,
            )
        }
        None => (
            GraphStats {
                directories: 0,
                files: 0,
                definitions: 0,
                imported_symbols: 0,
                relationships: 0,
            },
            HashMap::new(),
            HashMap::new(),
        ),
    };

    let detailed = show_stats.then(|| DetailedStats {
        skipped_files: result
            .skipped_files
            .iter()
            .map(|s| SkippedFile {
                path: s.file_path.clone(),
                reason: s.reason.clone(),
            })
            .collect(),
        errored_files: result
            .errored_files
            .iter()
            .map(|e| ErroredFile {
                path: e.file_path.clone(),
                error: e.error_message.clone(),
            })
            .collect(),
        relationship_types: rel_counts,
        definition_types: def_counts,
    });

    IndexOutput {
        repository: repo_name.to_string(),
        path: path.to_string(),
        time_seconds: result.total_processing_time.as_secs_f64(),
        graph,
        processing: ProcessingStats {
            skipped_files: result.skipped_files.len(),
            errored_files: result.errored_files.len(),
        },
        database_path: None,
        detailed,
    }
}

#[derive(Serialize)]
struct CompileResult {
    input: Value,
    sql: String,
    params: HashMap<String, Value>,
    rendered_sql: String,
}

/// Parse a single query JSON and load the ontology.
fn parse_query_input(
    json_input: &str,
    ontology_path: Option<PathBuf>,
) -> Result<(Value, Ontology)> {
    let ontology_dir = ontology_path.unwrap_or_else(|| PathBuf::from(env!("ONTOLOGY_DIR")));
    let ontology = Ontology::load_from_dir(&ontology_dir)
        .with_context(|| format!("failed to load ontology from {}", ontology_dir.display()))?;

    let value: Value = serde_json::from_str(json_input).context("failed to parse JSON input")?;
    if value.get("query_type").is_none() {
        anyhow::bail!("JSON must contain a \"query_type\" field");
    }

    Ok((value, ontology))
}

fn run_local_query(
    json_input: String,
    ontology_path: Option<PathBuf>,
) -> Result<query_engine::shared::PipelineOutput> {
    let (_, ontology) = parse_query_input(&json_input, ontology_path)?;
    let ontology = Arc::new(ontology);

    let store = workspace::Workspace::open_default()?;
    let db_path = store.db_path();
    if !db_path.exists() {
        anyhow::bail!(
            "no local graph found at {}. Run `orbit index` first.",
            db_path.display()
        );
    }

    let project_roots = store.project_roots()?;

    local_pipeline::run(&json_input, ontology, &db_path, project_roots).context("query failed")
}

fn run_compile(
    json_input: String,
    traversal_paths: Vec<String>,
    ontology_path: Option<PathBuf>,
    format: OutputFormat,
    local: bool,
) -> Result<()> {
    let (input, ontology) = parse_query_input(&json_input, ontology_path)?;

    let security_ctx = if local {
        None
    } else {
        let first_path = traversal_paths
            .first()
            .context("--traversal-paths required for server compilation")?;
        let org_id: i64 = first_path
            .split('/')
            .next()
            .context("traversal path is empty")?
            .parse()
            .context("first segment of traversal path must be a valid org ID")?;
        Some(
            SecurityContext::new(org_id, traversal_paths)
                .map_err(|e| anyhow::anyhow!("invalid security context: {}", e))?,
        )
    };

    let compile_result = if local {
        query_engine::compiler::compile_local(&json_input, &ontology)
    } else {
        query_engine::compiler::compile(&json_input, &ontology, security_ctx.as_ref().unwrap(), "")
    };

    match compile_result {
        Ok(result) => {
            let rendered_sql = result.base.render();
            let output = CompileResult {
                input,
                sql: result.base.sql,
                params: result
                    .base
                    .params
                    .into_iter()
                    .map(|(k, v)| (k, v.value))
                    .collect(),
                rendered_sql,
            };

            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
                OutputFormat::Pretty => {
                    println!("**SQL:**\n```sql\n{}\n```\n", output.sql);
                    if !output.params.is_empty() {
                        println!(
                            "**Params:**\n```json\n{}\n```\n",
                            serde_json::to_string_pretty(&output.params)?
                        );
                    }
                    println!("**Rendered SQL:**\n```sql\n{}\n```", output.rendered_sql);
                }
            }
        }
        Err(e) => {
            anyhow::bail!("compilation failed: {e}");
        }
    }

    Ok(())
}
