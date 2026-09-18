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
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{Level, debug};

/// Only bounds commands too fast to hide a round trip behind their own work.
/// Raising it buys no extra delivery and lengthens exit against a dead collector.
const TELEMETRY_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

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
    /// Repository path, or a directory that holds repositories (default: current directory).
    #[arg(value_name = "PATH", default_value = ".")]
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
    #[arg(
        value_name = "QUERY",
        required_unless_present = "path",
        help = "One query. Quote 'a|b|c' for OR alternatives; omit with --path to list definitions."
    )]
    query: Option<String>,

    /// Repository path (default: current directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    #[arg(
        long,
        default_value = "10",
        value_parser = parse_positive_usize,
        help = "Maximum matched definitions across all alternatives"
    )]
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

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "expected a positive integer".to_string())?;
    if value == 0 {
        return Err("expected a positive integer".to_string());
    }
    Ok(value)
}

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
        "File:<id>, Definition:<id> from `{} grep`, file paths, path:start-end line ranges, or directories. Mix or repeat targets; quote paths with spaces.",
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
        index: bool,
        components: std::collections::BTreeSet<commands::setup::Component>,
    ) -> commands::setup::Options {
        commands::setup::Options {
            agents,
            all,
            yes: self.yes,
            dry_run: self.dry_run,
            verbose: self.verbose,
            index,
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

        /// Do not index the current repository after configuring.
        #[arg(long)]
        no_index: bool,

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
    /// POST a query to the remote Orbit API and stream the response.
    Query {
        /// With `--language json`: a query body file. `-` or omitted reads stdin.
        /// With `--language gql`: required inline query text.
        #[arg(value_name = "FILE|QUERY", required_if_eq("language", "gql"))]
        source: Option<String>,

        /// Input shape: `json` (a query object or envelope file) or `gql`
        /// (inline query text). The server decides which language it accepts.
        #[arg(long, value_enum, default_value = "json")]
        language: remote::query::QueryLanguage,

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

    let started = Instant::now();
    let result = dispatch(cli.command, tracker.clone(), coding_agent.clone()).await;
    let exit_code = result.as_ref().map_or_else(exit_code_for, |()| 0);

    if let Some(tracker) = &tracker {
        telemetry::emit_command_event(
            tracker,
            &subcommand_path(&matches),
            exit_code,
            started.elapsed(),
            coding_agent.as_deref(),
        );
    }
    flush_telemetry(tracker.as_ref()).await;

    let Err(err) = &result else {
        return result;
    };
    if let Some(remote) = err.downcast_ref::<remote::error::RemoteError>() {
        eprintln!("{}", remote.message);
    } else if let Some(message) = workspace::describe_graph_lock_conflict(err) {
        eprintln!("{message}");
    } else if !tui::is_cancelled(err) {
        return result;
    }
    std::process::exit(exit_code);
}

fn exit_code_for(err: &anyhow::Error) -> i32 {
    if let Some(remote) = err.downcast_ref::<remote::error::RemoteError>() {
        remote.exit_code
    } else if tui::is_cancelled(err) {
        130
    } else {
        1
    }
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

async fn dispatch(
    command: Commands,
    tracker: Option<orbit_analytics::SnowplowAnalyticsTracker>,
    coding_agent: Option<String>,
) -> Result<()> {
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
        }) => commands::index::run(path, threads, stats, verbose, db),
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
            mcp::serve(tracker, coding_agent).await
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
            no_index,
            flags,
        } => {
            let components = commands::setup::Component::from_flags(mcp, &skip);
            let options = flags.to_options(agents, all, !no_index, components);
            let machine = commands::setup::detect::Machine::current()?;
            commands::setup::install(options, flags.target()?, &machine)
        }
        Commands::Uninstall { agents, flags } => {
            let options = flags.to_options(agents, false, false, Default::default());
            let machine = commands::setup::detect::Machine::current()?;
            commands::setup::uninstall(options, flags.target()?, &machine)
        }
        Commands::HookGuard { kind, mode: _ } => {
            commands::hook_guard::run(kind);
            Ok(())
        }
        Commands::Query {
            source,
            response_format,
            language,
        } => Ok(remote::run_query(source, response_format, language).await?),
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

#[cfg(test)]
mod tests {
    use super::{Cli, Commands, IndexArgs, SchemaArgs};
    use clap::{CommandFactory, Parser};

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
    fn every_subcommand_emits_a_telemetry_event() {
        let tracker = orbit_analytics::InMemoryAnalyticsTracker::new();
        let actions: Vec<String> = Cli::command()
            .get_subcommands()
            .map(|sub| sub.get_name().replace('-', "_"))
            .collect();
        for action in &actions {
            crate::telemetry::emit_command_event(
                &tracker,
                action,
                0,
                std::time::Duration::ZERO,
                None,
            );
        }
        let emitted: Vec<String> = tracker
            .drain()
            .iter()
            .map(|event| event.action().to_string())
            .collect();
        assert_eq!(emitted, actions);
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
        assert!(Cli::try_parse_from(["orbit", "grep", "App", "--limit", "0"]).is_err());
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
            language,
        } = Cli::parse_from(["orbit", "query", "--response-format", "raw", "-"]).command
        else {
            panic!("expected query");
        };
        assert_eq!(source.as_deref(), Some("-"));
        assert_eq!(response_format, Some(super::remote::ResponseFormat::Raw));
        assert_eq!(language, super::remote::query::QueryLanguage::Json);
        assert!(Cli::try_parse_from(["orbit", "query", "--language", "gql"]).is_err());
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
        assert!(matches!(
            Cli::parse_from(["orbit", "context", "src/lib.rs"]).command,
            Commands::Context(_)
        ));
        assert!(Cli::try_parse_from(["orbit", "context"]).is_err());
        assert!(Cli::try_parse_from(["orbit", "context", "--file", "src/lib.rs"]).is_err());
        for removed in ["--outline", "--related", "--tests"] {
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
}
