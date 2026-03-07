use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use code_graph::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::loading::DirectoryFileSource;
use ontology::Ontology;
use query_engine::SecurityContext;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
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
#[command(name = "gkg")]
#[command(about = "Knowledge Graph Indexer - indexes code repositories into graph structures")]
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
    /// Execute query engine on JSON payloads and output SQL
    ///
    /// Takes a JSON object where each key is a query description and each value
    /// is a query payload for the query engine. Outputs the label, input JSON,
    /// and generated SQL for each query.
    Query {
        /// Path to JSON file containing queries, or use --json for inline JSON
        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,

        /// Inline JSON payload (alternative to file path)
        #[arg(long, conflicts_with = "file")]
        json: Option<String>,

        /// Traversal paths for security context (e.g., "1/2/3/"). Org ID is parsed from the first segment.
        #[arg(long, short, required = true, num_args = 1..)]
        traversal_paths: Vec<String>,

        /// Path to ontology directory (default: fixtures/ontology)
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
            traversal_paths,
            ontology,
            format,
        } => run_query(file, json, traversal_paths, ontology, format),
    }
}

async fn run_index(path: PathBuf, threads: usize, show_stats: bool) -> Result<()> {
    let canonical_path = dunce::canonicalize(&path)?;
    let path_str = canonical_path.to_string_lossy().to_string();

    info!("Indexing repository at: {}", path_str);

    let repo_name = canonical_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "repository".to_string());

    let file_source = DirectoryFileSource::new(path_str.clone());

    let config = IndexingConfig {
        worker_threads: threads,
        max_file_size: 5_000_000,
        respect_gitignore: true,
    };

    info!("Using indexing config: {:?}", config);

    let indexer = RepositoryIndexer::new(repo_name.clone(), path_str.clone());
    let result = indexer.index_files(file_source, &config).await?;

    let (graph, rel_counts, def_counts) = if let Some(ref graph_data) = result.graph_data {
        let mut rel_counts: HashMap<String, usize> = HashMap::new();
        for rel in &graph_data.relationships {
            let type_str = format!("{:?}", rel.relationship_type);
            *rel_counts.entry(type_str).or_insert(0) += 1;
        }

        let mut def_counts: HashMap<String, usize> = HashMap::new();
        for def in &graph_data.definition_nodes {
            let type_str = format!("{:?}", def.definition_type);
            *def_counts.entry(type_str).or_insert(0) += 1;
        }

        (
            GraphStats {
                directories: graph_data.directory_nodes.len(),
                files: graph_data.file_nodes.len(),
                definitions: graph_data.definition_nodes.len(),
                imported_symbols: graph_data.imported_symbol_nodes.len(),
                relationships: graph_data.relationships.len(),
            },
            rel_counts,
            def_counts,
        )
    } else {
        (
            GraphStats {
                directories: 0,
                files: 0,
                definitions: 0,
                imported_symbols: 0,
                relationships: 0,
            },
            HashMap::new(),
            HashMap::new(),
        )
    };

    let detailed = if show_stats {
        Some(DetailedStats {
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
        })
    } else {
        None
    };

    let output = IndexOutput {
        repository: repo_name,
        path: path_str,
        time_seconds: result.total_processing_time.as_secs_f64(),
        graph,
        processing: ProcessingStats {
            skipped_files: result.skipped_files.len(),
            errored_files: result.errored_files.len(),
        },
        detailed,
    };

    info!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

#[derive(Serialize)]
struct QueryResult {
    label: String,
    input: Value,
    sql: String,
    params: HashMap<String, Value>,
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

fn run_query(
    file: Option<PathBuf>,
    json_input: Option<String>,
    traversal_paths: Vec<String>,
    ontology_path: Option<PathBuf>,
    format: OutputFormat,
) -> Result<()> {
    let json_str = match (file, json_input) {
        (Some(path), None) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read file: {}", path.display()))?,
        (None, Some(json)) => json,
        (None, None) => anyhow::bail!("either FILE or --json must be provided"),
        (Some(_), Some(_)) => unreachable!("clap prevents this"),
    };

    // Parse org_id from first segment of first traversal path
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

    let ontology_dir = ontology_path.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("fixtures/ontology")
    });

    let ontology = Ontology::load_from_dir(&ontology_dir)
        .with_context(|| format!("failed to load ontology from {}", ontology_dir.display()))?;

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

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&results)?);
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
