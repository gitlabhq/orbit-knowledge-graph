mod local_converter;
mod workspace;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use code_graph::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::loading::DirectoryFileSource;
use duckdb_client::DuckDbClient;
use ontology::Ontology;
use query_engine::{SecurityContext, SqlDialect};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
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
    detailed: Option<DetailedStats>,
    db_path: String,
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
    /// Index a code repository and persist the graph to a local DuckDB database
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
    /// Execute a graph query against a local DuckDB index or compile to SQL
    Query {
        /// Path to JSON file containing queries, or use --json for inline JSON
        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,

        /// Inline JSON payload (alternative to file path)
        #[arg(long, conflicts_with = "file")]
        json: Option<String>,

        /// Execute query against a local DuckDB index at the given repo path
        #[arg(long, value_name = "REPO_PATH")]
        local: Option<PathBuf>,

        /// Traversal paths for security context (required for non-local queries)
        #[arg(long, short, num_args = 1..)]
        traversal_paths: Vec<String>,

        /// Path to ontology directory (default: config/ontology)
        #[arg(long, short)]
        ontology: Option<PathBuf>,

        /// Output format: pretty (default) or json
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,
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
            file,
            json,
            local,
            traversal_paths,
            ontology,
            format,
        } => {
            if let Some(repo_path) = local {
                run_local_query(file, json, repo_path, ontology, format)
            } else {
                run_query(file, json, traversal_paths, ontology, format)
            }
        }
    }
}

fn project_id_from_path(path: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

async fn run_index(path: PathBuf, threads: usize, show_stats: bool) -> Result<()> {
    let store = workspace::IndexStore::open_default()?;
    let repos = store.resolve_repos(&path).await?;

    if repos.is_empty() {
        info!("No git repositories found in {}", path.display());
        return Ok(());
    }

    let config = IndexingConfig {
        worker_threads: threads,
        max_file_size: 5_000_000,
        respect_gitignore: true,
    };

    for repo_path in &repos {
        let key = repo_path.to_string_lossy().to_string();
        store
            .set_status(&key, workspace::Status::Indexing, None)
            .await?;

        info!("Indexing repository at: {}", key);

        let repo_name = repo_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "repository".to_string());

        let file_source = DirectoryFileSource::new(key.clone());
        let indexer = RepositoryIndexer::new(repo_name.clone(), key.clone());

        let mut result = match indexer.index_files(file_source, &config).await {
            Ok(r) => r,
            Err(e) => {
                store
                    .set_status(&key, workspace::Status::Error, Some(e.to_string()))
                    .await?;
                anyhow::bail!("{e}");
            }
        };

        let project_id = project_id_from_path(&key);
        let branch = "HEAD";
        let db_path = store.db_path(&key);

        if let Some(ref mut graph_data) = result.graph_data {
            graph_data.assign_node_ids(project_id, branch);

            let converted = local_converter::convert_graph_data(graph_data, project_id, branch)
                .context("failed to convert graph data to Arrow batches")?;

            let client = DuckDbClient::open(&db_path).context("failed to open DuckDB database")?;
            client
                .initialize_schema()
                .context("failed to initialize DuckDB schema")?;
            client
                .delete_project_data(project_id, branch)
                .context("failed to delete existing data")?;

            client.insert_arrow("gl_directory", &converted.directories)?;
            client.insert_arrow("gl_file", &converted.files)?;
            client.insert_arrow("gl_definition", &converted.definitions)?;
            client.insert_arrow("gl_imported_symbol", &converted.imported_symbols)?;
            client.insert_arrow("gl_edge", &converted.edges)?;

            info!("Persisted graph to DuckDB at {}", db_path.display());
        }

        store
            .set_status(&key, workspace::Status::Indexed, None)
            .await?;

        let output = build_index_output(
            &repo_name,
            &key,
            &result,
            show_stats,
            db_path.to_string_lossy().to_string(),
        );
        info!("{}", serde_json::to_string_pretty(&output)?);
    }

    Ok(())
}

fn build_index_output(
    repo_name: &str,
    path: &str,
    result: &code_graph::indexer::RepositoryIndexingResult,
    show_stats: bool,
    db_path: String,
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
        detailed,
        db_path,
    }
}

#[derive(Serialize)]
struct QueryResult {
    label: String,
    input: Value,
    sql: String,
    params: HashMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<Vec<HashMap<String, Value>>>,
}

#[derive(Serialize)]
struct QueryError {
    label: String,
    input: Value,
    error: String,
}

#[derive(Serialize)]
#[serde(untagged)]
enum QueryOutput {
    Success(QueryResult),
    Error(QueryError),
}

fn load_query_json(file: Option<PathBuf>, json_input: Option<String>) -> Result<String> {
    match (file, json_input) {
        (Some(path), None) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read file: {}", path.display())),
        (None, Some(json)) => Ok(json),
        (None, None) => anyhow::bail!("either FILE or --json must be provided"),
        (Some(_), Some(_)) => unreachable!("clap prevents this"),
    }
}

fn load_ontology(ontology_path: Option<PathBuf>) -> Result<Ontology> {
    let ontology_dir = ontology_path.unwrap_or_else(|| PathBuf::from(env!("ONTOLOGY_DIR")));
    Ontology::load_from_dir(&ontology_dir)
        .with_context(|| format!("failed to load ontology from {}", ontology_dir.display()))
}

fn run_local_query(
    file: Option<PathBuf>,
    json_input: Option<String>,
    repo_path: PathBuf,
    ontology_path: Option<PathBuf>,
    format: OutputFormat,
) -> Result<()> {
    let json_str = load_query_json(file, json_input)?;
    let queries: HashMap<String, Value> = serde_json::from_str(&json_str)
        .context("failed to parse JSON as object with string keys")?;
    let ontology = load_ontology(ontology_path)?;

    let canonical = dunce::canonicalize(&repo_path)
        .with_context(|| format!("failed to canonicalize path: {}", repo_path.display()))?;
    let key = canonical.to_string_lossy().to_string();

    let store = workspace::IndexStore::open_default()?;
    let db_path = store.db_path(&key);

    if !db_path.exists() {
        anyhow::bail!(
            "No local index found at {}. Run `orbit index {}` first.",
            db_path.display(),
            repo_path.display()
        );
    }

    let client = DuckDbClient::open(&db_path)
        .with_context(|| format!("failed to open DuckDB at {}", db_path.display()))?;

    let mut results: Vec<QueryOutput> = Vec::with_capacity(queries.len());
    let mut sorted_queries: Vec<_> = queries.into_iter().collect();
    sorted_queries.sort_by(|a, b| a.0.cmp(&b.0));

    for (label, input) in sorted_queries {
        let input_json = serde_json::to_string(&input).context("failed to serialize input")?;

        match query_engine::compile_local(&input_json, &ontology, SqlDialect::DuckDb) {
            Ok(compiled) => {
                let sql = &compiled.base.sql;
                let params = &compiled.base.params;

                let duckdb_params: Vec<Box<dyn duckdb_client::duckdb::ToSql>> =
                    build_duckdb_params(params);

                let param_map: HashMap<String, Value> = params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.value.clone()))
                    .collect();

                match client.query_arrow_params(sql, &duckdb_params) {
                    Ok(batches) => {
                        let rows = extract_rows(&batches);
                        results.push(QueryOutput::Success(QueryResult {
                            label,
                            input,
                            sql: sql.clone(),
                            params: param_map,
                            results: Some(rows),
                        }));
                    }
                    Err(e) => {
                        results.push(QueryOutput::Error(QueryError {
                            label,
                            input,
                            error: format!("query execution failed: {e}\nSQL: {sql}"),
                        }));
                    }
                }
            }
            Err(e) => {
                results.push(QueryOutput::Error(QueryError {
                    label,
                    input,
                    error: e.to_string(),
                }));
            }
        }
    }

    print_results(&results, format)
}

fn build_duckdb_params(
    params: &HashMap<String, query_engine::ParamValue>,
) -> Vec<Box<dyn duckdb_client::duckdb::ToSql>> {
    let mut sorted: Vec<_> = params.iter().collect();
    sorted.sort_by_key(|(k, _)| {
        k.strip_prefix('$')
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
    });

    sorted
        .into_iter()
        .map(|(_, pv)| -> Box<dyn duckdb_client::duckdb::ToSql> {
            match &pv.value {
                Value::String(s) => Box::new(s.clone()),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Box::new(i)
                    } else if let Some(f) = n.as_f64() {
                        Box::new(f)
                    } else {
                        Box::new(n.to_string())
                    }
                }
                Value::Bool(b) => Box::new(*b),
                Value::Null => Box::new(Option::<String>::None),
                _ => Box::new(pv.value.to_string()),
            }
        })
        .collect()
}

fn extract_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<HashMap<String, Value>> {
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray,
    };

    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = HashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.insert(field.name().clone(), Value::Null);
                    continue;
                }
                let value = if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    Value::String(a.value(row_idx).to_string())
                } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                    Value::String(a.value(row_idx).to_string())
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    serde_json::json!(a.value(row_idx))
                } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                    serde_json::json!(a.value(row_idx))
                } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                    Value::Bool(a.value(row_idx))
                } else {
                    Value::String(format!("{:?}", col))
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(row);
        }
    }
    rows
}

fn print_results(results: &[QueryOutput], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(results)?);
        }
        OutputFormat::Pretty => {
            for (i, result) in results.iter().enumerate() {
                if i > 0 {
                    println!("\n{}", "=".repeat(80));
                }
                match result {
                    QueryOutput::Success(r) => {
                        println!("\n### {}\n", r.label);
                        println!(
                            "**Input:**\n```json\n{}\n```\n",
                            serde_json::to_string_pretty(&r.input)?
                        );
                        println!("**SQL:**\n```sql\n{}\n```\n", r.sql);
                        if !r.params.is_empty() {
                            println!(
                                "**Params:**\n```json\n{}\n```",
                                serde_json::to_string_pretty(&r.params)?
                            );
                        }
                        if let Some(ref rows) = r.results {
                            println!("\n**Results ({} rows):**", rows.len());
                            if !rows.is_empty() {
                                println!("```json\n{}\n```", serde_json::to_string_pretty(rows)?);
                            }
                        }
                    }
                    QueryOutput::Error(e) => {
                        println!("\n### {} [ERROR]\n", e.label);
                        println!(
                            "**Input:**\n```json\n{}\n```\n",
                            serde_json::to_string_pretty(&e.input)?
                        );
                        println!("**Error:** {}", e.error);
                    }
                }
            }
        }
    }
    Ok(())
}

fn run_query(
    file: Option<PathBuf>,
    json_input: Option<String>,
    traversal_paths: Vec<String>,
    ontology_path: Option<PathBuf>,
    format: OutputFormat,
) -> Result<()> {
    if traversal_paths.is_empty() {
        anyhow::bail!("--traversal-paths is required for non-local queries");
    }

    let json_str = load_query_json(file, json_input)?;

    let first_path = traversal_paths
        .first()
        .context("at least one traversal path is required")?;
    let org_id: i64 = first_path
        .split('/')
        .next()
        .context("traversal path is empty")?
        .parse()
        .context("first segment of traversal path must be a valid org ID")?;

    let security_ctx = SecurityContext::new(org_id, traversal_paths)
        .map_err(|e| anyhow::anyhow!("invalid security context: {}", e))?;

    let queries: HashMap<String, Value> = serde_json::from_str(&json_str)
        .context("failed to parse JSON as object with string keys")?;

    let ontology = load_ontology(ontology_path)?;

    let mut results: Vec<QueryOutput> = Vec::with_capacity(queries.len());

    let mut sorted_queries: Vec<_> = queries.into_iter().collect();
    sorted_queries.sort_by(|a, b| a.0.cmp(&b.0));

    for (label, input) in sorted_queries {
        let input_json = serde_json::to_string(&input).context("failed to serialize input")?;

        match query_engine::compile(&input_json, &ontology, &security_ctx) {
            Ok(result) => {
                results.push(QueryOutput::Success(QueryResult {
                    label,
                    input,
                    sql: result.base.sql,
                    params: result
                        .base
                        .params
                        .into_iter()
                        .map(|(k, v)| (k, v.value))
                        .collect(),
                    results: None,
                }));
            }
            Err(e) => {
                results.push(QueryOutput::Error(QueryError {
                    label,
                    input,
                    error: e.to_string(),
                }));
            }
        }
    }

    print_results(&results, format)
}
