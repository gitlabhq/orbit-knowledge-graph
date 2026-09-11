#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod commands;
mod descriptions;
mod list;
mod mcp;
mod refresh;
mod remote;
mod settings;
mod skill;
mod sql;
mod sql_format;
mod telemetry;
mod workspace;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use ontology::Ontology;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{Level, debug, info};
use tracing_subscriber::fmt::format::FmtSpan;

const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

const SKILL_LONG_ABOUT: &str = "Print the bundled, version-matched orbit-cli skill content.\n\n\
                                With no argument, prints SKILL.md (the manifest). Pass a relative path \
                                such as `references/sql.md` or `references/repo_map.md` to print that file.";

/// Per-file byte cap for local indexing; files above it are recorded as nodes
/// but not loaded or parsed.
const MAX_INDEXED_FILE_BYTES: u64 = 5_000_000;

/// Only bounds commands too fast to hide a round trip behind their own work.
/// Raising it buys no extra delivery and lengthens exit against a dead collector.
const TELEMETRY_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

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
#[command(about = "Orbit - query the local code graph or the remote Orbit API")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("index"))]
struct IndexArgs {
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

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("grep"))]
#[command(
    long_about = "Search the local graph for definitions matching one concept.\n\n\
                  Use 1-3 identifier keywords for one concept; multiword identifiers \
                  such as 'rate limit' are fine. Read relevant bodies with context, \
                  not cat, head, sed, or raw Read. Use those bodies for exact edits; \
                  use context --file for a file overview with imports and signatures. \
                  Start implementing once the edit point and nearby pattern are clear. \
                  Shows the top ten matches, with source for the first five in a shared 24,000-character output budget. \
                  Remaining source space is divided equally among the selected matches. \
                  Long bodies stop at a line boundary with a truncation notice and a context command. \
                  Search matches indexed names and paths, not source bodies or regexes. \
                  Changed and new source files are refreshed on demand together with their neighbors \
                  (files they import, that import them, or that share a relationship); other files are not reparsed. \
                  Name/path matches do not establish a code connection or dataflow. \
                  Trace relationships with `context <fqn> --related`. \
                  --kind takes one comma-separated list, e.g. `Class,Method`.\n\n\
                  For weak or unmatched terms, inspect a candidate with context. \
                  Retry with one concept or a source identifier; if that still misses, \
                  fall back to text grep."
)]
struct GrepArgs {
    #[arg(
        help = "One concept per query, e.g. 'rate limit'; batch only independent lookups. Omit with --path to list definitions under that path.",
        value_name = "QUERY",
        required_unless_present = "path"
    )]
    query: Vec<String>,

    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Only search definitions under this repo-relative directory or file
    /// (e.g. `crates/query-engine`); repeatable, and accepts globs such as
    /// `crates/*/src/lib.rs`.
    #[arg(long, value_name = "PATH")]
    path: Vec<String>,

    /// Only search definitions of these types, as printed in grep's `[Kind]`
    /// column: one kind or a comma-separated list such as `Class,Method`
    /// (`"Class|Method"` also works when quoted); case-insensitive.
    #[arg(long, value_name = "KINDS", value_parser = parse_kinds)]
    kind: Option<Kinds>,

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
struct Kinds(Vec<String>);

fn parse_kinds(value: &str) -> Result<Kinds, String> {
    let kinds: Vec<String> = value
        .split([',', '|'])
        .map(str::trim)
        .filter(|kind| !kind.is_empty())
        .map(str::to_string)
        .collect();
    if kinds.is_empty() {
        return Err("expected one kind or a comma-separated list such as `Class,Method`".into());
    }
    Ok(Kinds(kinds))
}

fn kind_names(kinds: Option<Kinds>) -> Vec<String> {
    kinds.map(|Kinds(names)| names).unwrap_or_default()
}

fn context_fqn_arg_help() -> String {
    format!(
        "Fully qualified names as printed by `{} grep`, their unqualified tails such as \
         `Type::method` when that names one definition, or globs such as \
         `crate::module::*` to print every definition they match. Several may be given at once.",
        commands::setup::spec::launcher()
    )
}

fn context_long_about() -> String {
    format!(
        "Read definition bodies, a file overview, or a definition's relationships.\n\n\
         Accepts FQNs from `{launcher} grep`, unique tails like `Type::method`, or globs \
         like `crate::module::*`, and prints their full bodies in file order. \
         `--file <path>` alone shows imports, definition signatures, and nested members. \
         With names, `--file` restricts lookup and accepts bare names. \
         Add `--related` to list connections instead of bodies; arrows show direction and \
         each line names its edge kind. `--tests` expands test connections.\n\n\
         Refreshes changed and new source files on demand together with their neighbors; \
         other files are not reparsed. \
         If source cannot be verified, file requests report an unavailable outline; \
         definition requests show the full file labeled `ranges=unverified`. \
         Relationships are re-resolved across the reparsed set; an import of a project file the \
         indexer cannot parse leaves a stale-relationship warning until a full `index`.",
        launcher = commands::setup::spec::launcher()
    )
}

fn sql_long_about() -> String {
    format!(
        "Run a read-only SQL query against the local DuckDB graph.\n\n\
         Tables are pre-filtered to the current checkout's indexed commit, so ad-hoc \
         SQL needs no project_id or commit_sha predicates; --all queries every indexed \
         commit and --repo <path> scopes to another checkout. `{} schema` lists the tables.",
        commands::setup::spec::launcher()
    )
}

#[derive(Args, Debug, PartialEq)]
#[command(about = "Read definition bodies, a file overview, or relationships")]
#[command(long_about = context_long_about())]
struct ContextArgs {
    #[arg(value_name = "FQN", help = context_fqn_arg_help(), required_unless_present = "file")]
    fqn: Vec<String>,

    #[arg(
        long,
        value_name = "PATH",
        visible_alias = "path",
        help = "Show a file overview, or restrict named definitions to this file (repo-relative or absolute)"
    )]
    file: Option<String>,

    /// Only print definitions of these types, as printed in grep's `[Kind]`
    /// column: one kind or a comma-separated list such as `Class,Method`
    /// (`"Class|Method"` also works when quoted); case-insensitive.
    /// Narrows a glob or --file and disambiguates a bare name.
    #[arg(long, value_name = "KINDS", value_parser = parse_kinds)]
    kind: Option<Kinds>,

    /// List every connection of the named definitions instead of their bodies:
    /// calls in both directions, imports, and extends, including uses via members.
    #[arg(long, requires = "fqn")]
    related: bool,

    /// List test, fixture, and generated connections instead of collapsing them into a count.
    #[arg(long, requires = "related")]
    tests: bool,

    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("run_sql"))]
#[command(long_about = sql_long_about())]
struct SqlArgs {
    /// SQL query, or `-` to read from stdin.
    #[arg(value_name = "QUERY", conflicts_with = "file")]
    query: Option<String>,

    /// Read SQL from a file.
    #[arg(long, short, value_name = "PATH")]
    file: Option<PathBuf>,

    /// Output format.
    #[arg(long, short = 'F', default_value = "table")]
    format: sql_format::Format,

    /// Checkout whose commit scopes the tables (default: current directory).
    #[arg(long, value_name = "PATH", conflicts_with = "all")]
    repo: Option<PathBuf>,

    /// Query every indexed repository and commit instead of the current
    /// checkout.
    #[arg(long)]
    all: bool,

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("get_graph_schema"))]
struct SchemaArgs {
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
}

/// List the repositories indexed in the local DuckDB graph.
#[derive(Args, Debug, PartialEq)]
struct ListArgs {
    /// Output format.
    #[arg(long, short = 'F', default_value = "table")]
    format: sql_format::Format,

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("mcp_serve"))]
#[command(long_about = "Serve the local graph to MCP-compatible AI agents.\n\n\
                  Plug into editors that support MCP (Claude Code, Cursor, OpenCode, Codex) \
                  so the agent can call `run_sql`, `get_graph_schema`, and `index`.")]
struct McpArgs {
    #[command(subcommand)]
    command: McpCommands,
}

#[derive(Args, Debug, PartialEq)]
#[command(name = "repo-map", about = descriptions::short("repo_map"))]
#[command(
    long_about = "Produce a high-level, LLM-oriented map of a locally indexed repository.\n\n\
                   Scoped to the current commit; if it is not indexed, prints the index \
                   command and exits. Running with no subcommand defaults to `overview`. \
                   Drill down with `tree`, `api`, `class`, `extends`, and `imports`."
)]
struct RepoMapArgs {
    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Limit output to source files with these extensions (repeat or
    /// comma-separate; a leading dot is optional).
    #[arg(long = "ext", value_name = "EXT")]
    extensions: Vec<String>,

    /// Override the DuckDB path (default: ~/.orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<commands::repo_map::RepoMapCommand>,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the version string and exit.
    Version,
    Index(IndexArgs),
    Grep(GrepArgs),
    Context(ContextArgs),
    Sql(SqlArgs),
    Schema(SchemaArgs),
    List(ListArgs),
    Mcp(McpArgs),
    #[command(name = "repo-map")]
    RepoMap(RepoMapArgs),
    #[command(about = descriptions::short("skill"), long_about = SKILL_LONG_ABOUT)]
    Skill {
        /// Skill file to print, relative to the skill root (default: SKILL.md).
        #[arg(value_name = "PATH")]
        path: Option<String>,
    },
    /// Configure AI coding assistants to consult the graph.
    #[command(
        long_about = "Configure AI coding assistants to consult the graph.\n\n\
                      Writes a managed section into each assistant's user-global \
                      instruction file (default) or the project's with `--project`/`--dir`, \
                      telling the assistant to prefer graph queries over grepping raw files, \
                      plus nudge hooks where the platform supports them (Claude Code, \
                      OpenCode). Pre-existing files get a one-time `.orbit-backup` sibling \
                      before their first modification. Re-running updates the section in \
                      place; `--remove` uninstalls."
    )]
    Setup {
        /// Assistants to configure. Required when installing; `--remove`
        /// without assistants removes the setup for all of them.
        #[arg(value_name = "ASSISTANT", value_parser = commands::setup::assistant_value_parser(), required_unless_present = "remove")]
        assistants: Vec<String>,

        /// Remove the configuration written by `orbit setup`.
        #[arg(long)]
        remove: bool,

        /// Write into the current project instead of the user-global config
        /// files.
        #[arg(long)]
        project: bool,

        /// Project directory (implies --project; default: current directory).
        #[arg(long, value_name = "PATH")]
        dir: Option<PathBuf>,
    },
    #[command(hide = true)]
    HookGuard {
        #[arg(value_name = "KIND")]
        kind: commands::hook_guard::Kind,

        #[arg(long, hide = true, value_name = "MODE")]
        mode: Option<String>,
    },
    /// POST a query envelope to the remote Orbit API and stream the response.
    Query {
        /// Query body file, or `-`/omitted to read from stdin.
        #[arg(value_name = "FILE")]
        source: Option<String>,

        /// Server response format. Overrides the body's `response_format`;
        /// defaults to `llm` when neither is set.
        #[arg(long, value_enum)]
        response_format: Option<remote::ResponseFormat>,
    },
    /// Show Orbit cluster health.
    Status,
    /// Show the remote Orbit ontology.
    Ontology {
        /// Node names to expand with full properties and edge lists.
        #[arg(value_name = "NODE")]
        nodes: Vec<String>,
    },
    /// Show the Orbit query DSL JSON Schema.
    Dsl,
    /// Show the Orbit MCP tool manifest.
    Tools,
    /// Show indexing progress for a namespace or project.
    #[command(name = "graph-status")]
    #[command(group(clap::ArgGroup::new("graph_status_scope").required(true).args(["full_path", "namespace_id", "project_id"])))]
    GraphStatus {
        /// Full path of a project or group, such as `gitlab-org/gitlab`.
        #[arg(long)]
        full_path: Option<String>,

        /// Namespace (group) ID to inspect.
        #[arg(long)]
        namespace_id: Option<i64>,

        /// Project ID to inspect.
        #[arg(long)]
        project_id: Option<i64>,

        /// Server response format. Defaults to `raw` (structured JSON).
        #[arg(long, value_enum)]
        response_format: Option<remote::ResponseFormat>,
    },
    /// Read and write persisted CLI settings (`~/.orbit/settings.json`).
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
}

impl Commands {
    fn targets_remote(&self) -> bool {
        matches!(
            self,
            Commands::Query { .. }
                | Commands::Status
                | Commands::Ontology { .. }
                | Commands::Dsl
                | Commands::Tools
                | Commands::GraphStatus { .. }
        )
    }
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Print the saved value of a setting.
    Get {
        #[arg(value_name = "KEY")]
        key: String,
    },
    /// Save a setting, such as `telemetry.enabled false`.
    Set {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(value_name = "VALUE")]
        value: String,
    },
    /// List all known settings and their saved values.
    List,
}

#[derive(Subcommand, Debug, PartialEq)]
enum McpCommands {
    /// Start a stateless MCP server over stdio.
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).expect("clap already validated the arguments");

    let coding_agent = telemetry::detect_coding_agent(|key| std::env::var(key).ok());

    let tracker = telemetry::resolve_from_env().build_tracker();
    if let Some(tracker) = &tracker {
        telemetry::emit_command_event(
            tracker,
            &subcommand_path(&matches),
            cli.command.targets_remote(),
            coding_agent.as_deref(),
        );
        // One event never reaches labkit's batch threshold, so without this the
        // round trip would not start until shutdown.
        tracker.flush();
    }

    let result = dispatch(cli.command).await;

    flush_telemetry(tracker.as_ref()).await;
    if let Err(err) = &result
        && let Some(remote) = err.downcast_ref::<remote::error::RemoteError>()
    {
        eprintln!("{}", remote.message);
        std::process::exit(remote.exit_code);
    }
    result
}

fn subcommand_path(matches: &clap::ArgMatches) -> String {
    matches
        .subcommand_name()
        .map(|top| top.replace('-', "_"))
        .unwrap_or_default()
}

async fn flush_telemetry(tracker: Option<&orbit_analytics::SnowplowAnalyticsTracker>) {
    if let Some(tracker) = tracker
        && tokio::time::timeout(TELEMETRY_FLUSH_TIMEOUT, tracker.shutdown())
            .await
            .is_err()
    {
        debug!("telemetry flush timed out; dropping the event");
    }
}

async fn dispatch(command: Commands) -> Result<()> {
    match command {
        Commands::Version => {
            println!("{}", env!("ORBIT_VERSION"));
            Ok(())
        }
        Commands::Index(IndexArgs {
            path,
            threads,
            stats,
            verbose,
            db,
        }) => {
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

            run_index(path, threads, stats, db).await
        }
        Commands::Grep(GrepArgs {
            query,
            repo,
            path,
            kind,
            db,
        }) => commands::grep::run(
            query,
            repo,
            db,
            path,
            orbit_search::RecallFilter {
                kinds: kind_names(kind),
            },
        ),
        Commands::Context(args) if args.related => commands::relations::run(args),
        Commands::Context(args) => commands::context::run(args),
        Commands::Sql(SqlArgs {
            query,
            file,
            format,
            repo,
            all,
            db,
        }) => sql::run(query, file, format, db, repo, all),
        Commands::Schema(SchemaArgs { db, raw, tables }) => run_schema(db, raw, tables),
        Commands::List(ListArgs { format, db }) => list::run(format, db),
        Commands::Mcp(McpArgs {
            command: McpCommands::Serve,
        }) => {
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
        Commands::RepoMap(RepoMapArgs {
            repo,
            extensions,
            db,
            command,
        }) => commands::repo_map::run(
            repo,
            extensions,
            db,
            command.unwrap_or(commands::repo_map::RepoMapCommand::Overview),
        ),
        Commands::Config { command } => match command {
            ConfigCommands::Get { key } => commands::config::get(&key),
            ConfigCommands::Set { key, value } => commands::config::set(&key, &value),
            ConfigCommands::List => commands::config::list(),
        },
        Commands::Skill { path } => skill::run(path),
        Commands::Setup {
            assistants,
            remove,
            project,
            dir,
        } => {
            let target = if project || dir.is_some() {
                commands::setup::Target::project(dir)?
            } else {
                commands::setup::Target::Global
            };
            commands::setup::run(assistants, remove, target)
        }
        Commands::HookGuard { kind, mode: _ } => {
            commands::hook_guard::run(kind);
            Ok(())
        }
        Commands::Query {
            source,
            response_format,
        } => Ok(remote::run_query(source, response_format).await?),
        Commands::Status => Ok(remote::run_status().await?),
        Commands::Ontology { nodes } => Ok(remote::run_ontology(nodes).await?),
        Commands::Dsl => Ok(remote::run_dsl().await?),
        Commands::Tools => Ok(remote::run_tools().await?),
        Commands::GraphStatus {
            full_path,
            namespace_id,
            project_id,
            response_format,
        } => Ok(
            remote::run_graph_status(full_path, namespace_id, project_id, response_format).await?,
        ),
    }
}

fn run_schema(db: Option<PathBuf>, raw: bool, tables: Vec<String>) -> Result<()> {
    let client = sql::open_graph(db)?;

    let batches = if tables.is_empty() {
        sql::query(&client, &sql::schema_introspection_sql())?
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
                "no table named {} in the local graph. Run `{} schema` to list tables.",
                missing
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                commands::setup::spec::launcher()
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

async fn run_index(
    path: PathBuf,
    threads: usize,
    show_stats: bool,
    db: Option<PathBuf>,
) -> Result<()> {
    for output in index_collect(path, threads, show_stats, db)? {
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
    db: Option<PathBuf>,
) -> Result<Vec<IndexOutput>> {
    let db_path = workspace::resolve_db_path(db)?;
    let store = workspace::Workspace::open_default()?;
    let repos = store.resolve_repos(&path)?;

    if repos.is_empty() {
        anyhow::bail!(
            "no git repository found in {}. Pass a repository path, or a directory containing one.",
            path.display()
        );
    }

    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;

    workspace::ensure_graph_schema(&db_path, LOCAL_DDL)?;

    let pipeline_config = pipeline_config(threads);

    let mut failed = 0usize;
    let mut outputs = Vec::with_capacity(repos.len());

    for repo_path in &repos {
        let git = match workspace::git_info(repo_path) {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("skipping {}: {e:#}", repo_path.display());
                failed += 1;
                workspace::record_git_info_failure(&db_path, repo_path, &e.to_string());
                continue;
            }
        };
        let key = git.repo_path.to_string_lossy().to_string();

        info!(
            "Indexing repository at: {} (branch: {}, commit: {})",
            key,
            git.branch,
            git.short_sha()
        );

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

        let result = index_repo(&git, &db_path, &ontology, pipeline_config.clone());
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

fn pipeline_config(threads: usize) -> code_graph::v2::PipelineConfig {
    code_graph::v2::PipelineConfig {
        worker_threads: threads,
        per_file_timeout: Some(std::time::Duration::from_secs(2)),
        per_file_parse_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_walk_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_ssa_timeout: Some(std::time::Duration::from_millis(100)),
        cross_file_resolve_timeout: Some(std::time::Duration::from_secs(180)),
        ..Default::default()
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
    let (file_inventory, filter) = refresh::inventory(&git.repo_path)?;

    let mut sources = workspace::fingerprint_files(&git.repo_path, &file_inventory);
    let client =
        duckdb_client::DuckDbClient::open(db_path).context("failed to open DuckDB for writing")?;
    refresh::mark_stale(
        &client,
        git.project_id,
        "the whole project until indexing completes",
    )?;
    workspace::store_source_fingerprints(&client, git.project_id, &Default::default())?;

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
        file_inventory.clone(),
        pipeline_config.clone(),
        filter.file_reasons(),
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
    refresh::rebuild_search(&client, git)?;
    let current_sources = workspace::fingerprint_files(&git.repo_path, &file_inventory);
    let stable_sources = sources == current_sources;
    sources.retain(|path, fingerprint| current_sources.get(path) == Some(fingerprint));
    let sources = sources
        .into_iter()
        .map(|(path, (hash, _))| (path, hash))
        .collect();
    workspace::store_source_fingerprints(&client, git.project_id, &sources)?;
    if stable_sources {
        refresh::clear_stale(&client, git.project_id)?;
    }
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
    use super::{Cli, Commands, IndexArgs, SchemaArgs, fatal_pipeline_reason};
    use clap::{CommandFactory, Parser};
    use code_graph::v2::pipeline::PipelineError;

    #[test]
    fn cli_command_tree_verifies() {
        Cli::command().debug_assert();
    }

    fn action_for(argv: &[&str]) -> String {
        let matches = Cli::command()
            .try_get_matches_from(argv)
            .expect("valid argv");
        super::subcommand_path(&matches)
    }

    #[test]
    fn subcommand_path_names_the_top_level_verb() {
        assert_eq!(action_for(&["orbit", "version"]), "version");
        assert_eq!(action_for(&["orbit", "query"]), "query");
        assert_eq!(
            action_for(&["orbit", "graph-status", "--full-path", "a/b"]),
            "graph_status"
        );
        assert_eq!(action_for(&["orbit", "sql", "SELECT 1"]), "sql");
        assert_eq!(action_for(&["orbit", "config", "set", "k", "v"]), "config");
        assert_eq!(action_for(&["orbit", "repo-map", "tree"]), "repo_map");
        assert_eq!(action_for(&["orbit", "mcp", "serve"]), "mcp");
    }

    #[test]
    fn only_remote_api_verbs_target_remote() {
        for argv in [
            ["orbit", "query"].as_slice(),
            &["orbit", "status"],
            &["orbit", "ontology", "User"],
            &["orbit", "dsl"],
            &["orbit", "tools"],
            &["orbit", "graph-status", "--project-id", "1"],
        ] {
            assert!(Cli::parse_from(argv).command.targets_remote(), "{argv:?}");
        }
        for argv in [
            ["orbit", "grep", "x"].as_slice(),
            &["orbit", "schema"],
            &["orbit", "sql", "SELECT 1"],
            &["orbit", "version"],
        ] {
            assert!(!Cli::parse_from(argv).command.targets_remote(), "{argv:?}");
        }
    }

    #[test]
    fn former_local_and_remote_verbs_parse_at_top_level() {
        let Commands::Index(index) =
            Cli::parse_from(["orbit", "index", "/tmp/repo", "--threads", "4"]).command
        else {
            panic!("expected index");
        };
        assert_eq!(
            index,
            IndexArgs {
                path: "/tmp/repo".into(),
                threads: 4,
                stats: false,
                verbose: false,
                db: None,
            }
        );

        let Commands::Schema(schema) =
            Cli::parse_from(["orbit", "schema", "gl_edge", "--raw"]).command
        else {
            panic!("expected schema");
        };
        assert_eq!(
            schema,
            SchemaArgs {
                db: None,
                raw: true,
                tables: vec!["gl_edge".to_string()],
            }
        );

        assert!(matches!(
            Cli::parse_from(["orbit", "grep", "who calls this"]).command,
            Commands::Grep(_)
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "sql", "SELECT 1"]).command,
            Commands::Sql(_)
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "list"]).command,
            Commands::List(_)
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "mcp", "serve"]).command,
            Commands::Mcp(_)
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "repo-map", "overview"]).command,
            Commands::RepoMap(_)
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "context", "a::b"]).command,
            Commands::Context(_)
        ));

        let Commands::Ontology { nodes } =
            Cli::parse_from(["orbit", "ontology", "User", "Project"]).command
        else {
            panic!("expected ontology");
        };
        assert_eq!(nodes, vec!["User".to_string(), "Project".to_string()]);
        let Commands::Query {
            source,
            response_format,
        } = Cli::parse_from(["orbit", "query", "--response-format", "raw", "-"]).command
        else {
            panic!("expected query");
        };
        assert_eq!(source.as_deref(), Some("-"));
        assert_eq!(response_format, Some(super::remote::ResponseFormat::Raw));
        assert!(matches!(
            Cli::parse_from(["orbit", "status"]).command,
            Commands::Status
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "dsl"]).command,
            Commands::Dsl
        ));
        assert!(matches!(
            Cli::parse_from(["orbit", "tools"]).command,
            Commands::Tools
        ));
        let Commands::GraphStatus { full_path, .. } =
            Cli::parse_from(["orbit", "graph-status", "--full-path", "a/b"]).command
        else {
            panic!("expected graph-status");
        };
        assert_eq!(full_path.as_deref(), Some("a/b"));

        for argv in [
            ["orbit", "local", "grep", "x"].as_slice(),
            &["orbit", "remote", "status"],
            &["orbit", "ask", "x"],
            &["orbit", "grep", "x", "--body"],
            &["orbit", "grep", "x", "--limit", "5"],
            &["orbit", "context", "x", "--outline"],
            &["orbit", "grep", "x", "--related"],
            &["orbit", "context", "--file", "a.rs", "--related"],
            &["orbit", "context", "x", "--callers"],
            &["orbit", "context", "x", "--tests"],
            &["orbit", "setup", "claude", "--local"],
        ] {
            assert!(
                Cli::try_parse_from(argv).is_err(),
                "{argv:?} must be rejected"
            );
        }
    }

    #[test]
    fn hook_guard_ignores_the_legacy_mode_flag() {
        for argv in [
            ["orbit", "hook-guard", "search"].as_slice(),
            &["orbit", "hook-guard", "search", "--mode", "remote"],
            &["orbit", "hook-guard", "read", "--mode", "local"],
        ] {
            assert!(
                matches!(Cli::parse_from(argv).command, Commands::HookGuard { .. }),
                "{argv:?}"
            );
        }
    }

    fn err(stage: &'static str, msg: &str, fatal: bool) -> PipelineError {
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
            err("parse", "bad syntax", false),
            err("walk", "skip", false),
        ];
        assert!(fatal_pipeline_reason(&errors).is_none());
    }

    #[test]
    fn a_fatal_error_bails_with_first_reason_and_count() {
        let errors = [
            err("parse", "recoverable", false),
            err("sink_write", "DuckDB write failed", true),
            err("conversion", "arrow overflow", true),
        ];
        let reason = fatal_pipeline_reason(&errors).expect("fatal must bail");
        assert!(reason.contains("sink_write"), "{reason}");
        assert!(reason.contains("DuckDB write failed"), "{reason}");
        assert!(reason.contains("2 fatal"), "{reason}");
    }
}
