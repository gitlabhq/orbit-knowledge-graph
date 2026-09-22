#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod commands;
mod descriptions;
mod list;
mod mcp;
mod remote;
mod settings;
mod skill;
mod sql;
mod sql_format;
mod telemetry;
mod tui;
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

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("grep"), long_about = descriptions::long("grep"))]
struct GrepArgs {
    /// Plain-language queries, e.g. "NATS message publish"; several may be
    /// given and are searched in one call. Omit them with --path to list
    /// every definition under that path instead.
    #[arg(value_name = "QUERY", required_unless_present = "path")]
    query: Vec<String>,

    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Maximum matched definitions to show, shared across the queries of one
    /// call (at least three each).
    #[arg(long, default_value = "10")]
    limit: usize,

    /// Only search definitions under this repo-relative directory or file
    /// (e.g. `crates/query-engine`); repeatable, and accepts globs such as
    /// `crates/*/src/lib.rs`.
    #[arg(long, value_name = "PATH")]
    path: Vec<String>,

    #[arg(long, value_name = "KINDS", value_parser = parse_kinds, help = KIND_ARG_HELP)]
    kind: Option<Kinds>,

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

const KIND_ARG_HELP: &str = "Only definitions of these types, as printed in grep's `[Kind]` \
                             column. One kind or a comma-separated list such as `Class,Method` \
                             (quoted `\"Class|Method\"` also works); case-insensitive.";

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

fn context_target_help() -> String {
    format!(
        "Definition:<id> references printed by `{} grep`, or one file path inside the current checkout. Repeat Definition references to read several definitions.",
        commands::setup::spec::launcher()
    )
}

fn sql_long_about() -> String {
    format!(
        "Run a read-only SQL query against the local DuckDB graph.\n\n\
         The current checkout's indexed commit scopes the tables, so queries need \
         no project_id or commit_sha predicates. `{} schema` lists the tables.",
        commands::setup::spec::launcher()
    )
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("context"), long_about = descriptions::long("context"))]
struct ContextArgs {
    #[arg(value_name = "TARGET", help = context_target_help(), required = true)]
    target: Vec<String>,

    /// Show relationships to test, fixture, and generated definitions.
    #[arg(long)]
    tests: bool,

    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
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

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("get_graph_schema"))]
struct SchemaArgs {
    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
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

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("mcp_serve"), long_about = descriptions::long("mcp_serve"))]
struct McpArgs {
    #[command(subcommand)]
    command: McpCommands,
}

#[derive(Args, Debug, PartialEq)]
#[command(name = "repo-map", about = descriptions::short("repo_map"), long_about = descriptions::long("repo_map"))]
struct RepoMapArgs {
    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Limit output to source files with these extensions (repeat or
    /// comma-separate; a leading dot is optional).
    #[arg(long = "ext", value_name = "EXT")]
    extensions: Vec<String>,

    /// Override the DuckDB path (default: ~/.gitlab/orbit/graph.duckdb).
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<commands::repo_map::RepoMapCommand>,
}

#[derive(Args, Debug, PartialEq)]
#[command(about = descriptions::short("skills"), long_about = descriptions::long("skills"))]
struct SkillsArgs {
    #[command(subcommand)]
    command: Option<SkillsCommands>,

    /// Skill name or a path in the default orbit skill.
    #[arg(value_name = "NAME_OR_PATH", hide = true)]
    name_or_path: Option<String>,

    /// File to print from the named skill.
    #[arg(value_name = "PATH", hide = true)]
    path: Option<String>,
}

#[derive(Subcommand, Debug, PartialEq)]
enum SkillsCommands {
    #[command(
        about = "Print a bundled agent skill file.",
        long_about = "Print a file from an agent skill bundled with this binary without installing it."
    )]
    Get {
        /// Skill name.
        #[arg(value_name = "NAME")]
        name: String,

        /// File relative to the skill root.
        #[arg(value_name = "PATH", default_value = "SKILL.md")]
        path: String,
    },
}

#[derive(Args, Debug, PartialEq)]
struct SetupFlags {
    /// Skip the agent picker and apply to the pre-selected agents.
    #[arg(long, short = 'y')]
    yes: bool,

    /// Print what would change and exit without writing.
    #[arg(long)]
    dry_run: bool,

    /// List every file touched instead of a per-component summary.
    #[arg(long, short = 'v')]
    verbose: bool,

    /// Write into the current project instead of the user-global config
    /// files.
    #[arg(long)]
    project: bool,

    /// Project directory (implies --project; default: current directory).
    #[arg(long, value_name = "PATH")]
    dir: Option<PathBuf>,
}

impl SetupFlags {
    fn to_options(
        &self,
        agents: Vec<String>,
        all: bool,
        components: std::collections::BTreeSet<commands::setup::Component>,
    ) -> commands::setup::Options {
        commands::setup::Options {
            agents,
            all,
            yes: self.yes,
            dry_run: self.dry_run,
            verbose: self.verbose,
            components,
        }
    }

    fn target(self) -> Result<commands::setup::Target> {
        if self.project || self.dir.is_some() {
            commands::setup::Target::project(self.dir)
        } else {
            Ok(commands::setup::Target::Global)
        }
    }
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
    #[command(name = "skills", alias = "skill")]
    Skills(SkillsArgs),
    #[command(about = descriptions::short("setup"), long_about = descriptions::long("setup"))]
    Setup {
        /// Agents to pre-select in the picker. Default: every agent detected
        /// on this machine.
        #[arg(value_name = "AGENT", value_parser = commands::setup::agent_name_parser())]
        agents: Vec<String>,

        /// Configure every supported agent, detected or not.
        #[arg(long, conflicts_with = "agents")]
        all: bool,

        /// Also register the `orbit` MCP server. Off by default.
        #[arg(long)]
        mcp: bool,

        /// Leave a component out (repeatable).
        #[arg(long, value_enum, value_name = "COMPONENT")]
        skip: Vec<commands::setup::Component>,

        #[command(flatten)]
        flags: SetupFlags,
    },
    #[command(about = descriptions::short("uninstall"), long_about = descriptions::long("uninstall"))]
    Uninstall {
        /// Agents to clean up. Default: all of them.
        #[arg(value_name = "AGENT", value_parser = commands::setup::agent_name_parser())]
        agents: Vec<String>,

        #[command(flatten)]
        flags: SetupFlags,
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
    /// Read and write persisted CLI settings (`~/.gitlab/orbit/settings.json`).
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

    // labkit-events ships no TLS provider; the tracker below builds an HTTPS client.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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
            limit,
            path,
            kind,
            db,
        }) => commands::grep::run(
            query,
            repo,
            db,
            limit,
            path,
            orbit_search::RecallFilter {
                kinds: kind_names(kind),
            },
        ),
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
        Commands::Skills(SkillsArgs {
            command,
            name_or_path,
            path,
        }) => match command {
            Some(SkillsCommands::Get { name, path }) => skill::get(name, path),
            None => skill::run(name_or_path, path),
        },
        Commands::Setup {
            agents,
            all,
            mcp,
            skip,
            flags,
        } => {
            let components = commands::setup::Component::from_flags(mcp, &skip);
            let options = flags.to_options(agents, all, components);
            let machine = commands::setup::detect::Machine::current()?;
            commands::setup::install(options, flags.target()?, &machine)
        }
        Commands::Uninstall { agents, flags } => {
            let options = flags.to_options(agents, false, Default::default());
            commands::setup::uninstall(options, flags.target()?)
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

    let pipeline_config = code_graph::v2::PipelineConfig {
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
    use super::{Cli, Commands, IndexArgs, SchemaArgs, fatal_pipeline_reason};
    use clap::{CommandFactory, Parser};
    use code_graph::v2::pipeline::PipelineError;

    #[test]
    fn cli_command_tree_verifies() {
        Cli::command().debug_assert();
    }

    #[test]
    fn remote_skill_commands_exist_in_the_clap_inventory() {
        let extracted: std::collections::BTreeSet<_> = env!("ORBIT_SKILL_REMOTE_COMMANDS")
            .split(',')
            .map(str::to_string)
            .collect();
        assert!(
            !extracted.is_empty(),
            "remote skill command extraction must not be empty"
        );
        let mut clap_commands: std::collections::BTreeSet<_> = Cli::command()
            .get_subcommands()
            .map(|command| command.get_name().to_string())
            .collect();
        let generated_help_is_materialized = clap_commands.remove(orbit_prompts::CLAP_HELP_COMMAND);
        assert!(
            !generated_help_is_materialized,
            "get_subcommands excludes clap's generated help command"
        );
        let unknown: Vec<_> = extracted
            .iter()
            .filter(|command| {
                command.as_str() != orbit_prompts::CLAP_HELP_COMMAND
                    && !clap_commands.contains(*command)
            })
            .collect();
        assert!(unknown.is_empty(), "unknown skill commands: {unknown:?}");
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
            Cli::parse_from(["orbit", "grep", "who calls this", "--limit", "5"]).command,
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
            Cli::parse_from(["orbit", "context", "Definition:7"]).command,
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
            &["orbit", "setup", "claude", "--local"],
        ] {
            assert!(
                Cli::try_parse_from(argv).is_err(),
                "{argv:?} must be rejected"
            );
        }
    }

    #[test]
    fn context_accepts_definition_references_or_a_file_target() {
        let Commands::Context(args) =
            Cli::parse_from(["orbit", "context", "Definition:7", "Definition:9"]).command
        else {
            panic!("expected context");
        };
        assert_eq!(args.target, vec!["Definition:7", "Definition:9"]);
        let Commands::Context(with_tests) =
            Cli::parse_from(["orbit", "context", "Definition:7", "--tests"]).command
        else {
            panic!("expected context");
        };
        assert!(with_tests.tests);
        assert!(matches!(
            Cli::parse_from(["orbit", "context", "src/lib.rs"]).command,
            Commands::Context(_)
        ));
        assert!(Cli::try_parse_from(["orbit", "context"]).is_err());
        assert!(Cli::try_parse_from(["orbit", "context", "--file", "src/lib.rs"]).is_err());
        for removed in ["--outline", "--related"] {
            assert!(
                Cli::try_parse_from(["orbit", "context", "Definition:7", removed]).is_err(),
                "{removed}"
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
