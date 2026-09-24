use anyhow::Result;
use clap::{Parser, Subcommand};

mod dashboards;
mod ddl;
mod integration_lanes;
mod loadtest;
mod metrics_catalog;
mod migration_ledger;
mod query_docs;
mod schema;
mod schema_public_output;
mod synth;

/// GKG development task runner.
///
/// Automates common development workflows like synthetic data generation,
/// query evaluation, and schema management.
#[derive(Parser)]
#[command(name = "xtask", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Synthetic graph data pipeline (generate, load, evaluate).
    Synth {
        #[command(subcommand)]
        command: SynthCommand,
    },
    /// Generate JSON Schema for the server configuration.
    Schema {
        /// Write schema to a file instead of stdout.
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
    /// Generate the shared metrics catalog consumed by runbooks dashboards.
    MetricsCatalog {
        /// Write catalog JSON to this path instead of the default.
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
        /// Diff the regenerated catalog against the committed file and
        /// return a non-zero exit if they differ.
        #[arg(long)]
        check: bool,
    },
    /// Generate graph DDL from the ontology.
    Ddl {
        /// Target database dialect.
        #[arg(long, short, default_value = "remote")]
        target: DdlTarget,

        /// Path to ontology directory (default: embedded).
        #[arg(long, short)]
        ontology: Option<std::path::PathBuf>,

        /// Table prefix (e.g., "v1_"; remote only).
        #[arg(long, short, default_value = "")]
        prefix: String,

        /// Diff generated DDL against an existing .sql file (remote only).
        #[arg(long, short)]
        diff: Option<std::path::PathBuf>,
    },
    /// Generate the Orbit Grafana dashboards from the metric catalog.
    Dashboards {
        /// Write dashboards under this directory instead of the default.
        #[arg(short, long)]
        dir: Option<std::path::PathBuf>,
        /// Diff regenerated dashboards against the committed files and
        /// return a non-zero exit if they differ.
        #[arg(long)]
        check: bool,
    },
    /// Generate or check hashes of every public schema introspection encoding.
    SchemaPublicOutput {
        #[arg(long)]
        check: bool,
        #[arg(long, default_value = "origin/main")]
        base: String,
        #[arg(long)]
        skip_pin_check: bool,
    },
    /// Manage the schema-migration ledger (config/schema-migrations.yaml).
    MigrationLedger {
        #[command(subcommand)]
        command: MigrationLedgerCommand,
    },
    /// Verify that the integration lanes partition every container test.
    IntegrationLanes {
        /// Check the filters in .gitlab-ci.yml.
        #[arg(long)]
        check: bool,
    },
    /// Regenerate the auto-derived tables in the query language reference doc
    /// from the ontology (currently the text-indexed properties table).
    QueryDocs {
        /// Path to the doc to update instead of the default.
        #[arg(short, long)]
        doc: Option<std::path::PathBuf>,
        /// Diff the regenerated table against the committed doc and return a
        /// non-zero exit if they differ.
        #[arg(long)]
        check: bool,
    },
    /// Run a gRPC load test against a running Orbit server, replaying the
    /// performance query corpus and reporting latency percentiles.
    ///
    /// Requires GKG_JWT_SECRET (base64-encoded HMAC key, same as the server).
    Loadtest {
        /// gRPC endpoint of the Orbit server (plaintext HTTP/2).
        #[arg(long, env = "ORBIT_ENDPOINT", default_value = "http://127.0.0.1:50054")]
        endpoint: String,

        /// Maximum in-flight requests per query.
        #[arg(long, default_value_t = 20)]
        concurrency: usize,

        /// Number of rounds; total requests per query = concurrency * rounds.
        #[arg(long, default_value_t = 5)]
        rounds: usize,

        /// Directory of scenario YAML files to replay (recursively).
        #[arg(
            long,
            default_value = "crates/integration-tests/tests/server/performance/scenarios"
        )]
        scenarios: std::path::PathBuf,

        /// Run a single scenario whose label contains this substring.
        #[arg(long)]
        query: Option<String>,

        /// Sign the JWT as a non-admin user (admin bypasses path scoping).
        #[arg(long)]
        no_admin: bool,

        /// Per-request gRPC deadline, in seconds.
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
}

#[derive(Subcommand)]
enum MigrationLedgerCommand {
    /// Recompute fingerprints, derive the scope, and append or amend an entry.
    Bump {
        /// Widen the derived scope: `*`, `sdlc`, or `code`.
        #[arg(long)]
        scope: Option<String>,
        /// Comma-separated SDLC entities (with `--scope sdlc`) to widen the entry.
        #[arg(long)]
        entities: Option<String>,
        /// Note recorded on the entry.
        #[arg(long)]
        note: Option<String>,
        /// Ref whose schema pin decides bump-vs-amend (default origin/main).
        #[arg(long)]
        base: Option<String>,
        /// Force amend when the base ref is unavailable.
        #[arg(long)]
        amend: bool,
        /// Force a new bump when the base ref is unavailable.
        #[arg(long)]
        new: bool,
    },
    /// Verify the committed snapshot and ledger match the working ontology.
    Check {
        /// Enforce the under-declaration guard against this base ref.
        #[arg(long)]
        base: Option<String>,
    },
    /// Snapshot auxiliary tables and refreshable views without changing the schema pin.
    Snapshot,
}

#[derive(Debug, Clone, Copy)]
enum DdlTarget {
    Remote(RemoteLifecycle),
    Local,
}

#[derive(Debug, Clone, Copy)]
enum RemoteLifecycle {
    Versioned,
    Persistent,
}

impl Default for DdlTarget {
    fn default() -> Self {
        DdlTarget::Remote(RemoteLifecycle::Versioned)
    }
}

impl clap::ValueEnum for DdlTarget {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            DdlTarget::Remote(RemoteLifecycle::Versioned),
            DdlTarget::Remote(RemoteLifecycle::Persistent),
            DdlTarget::Local,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            DdlTarget::Remote(RemoteLifecycle::Versioned) => {
                clap::builder::PossibleValue::new("remote")
                    .help("Versioned ClickHouse graph (tables, dictionaries, materialized views)")
            }
            DdlTarget::Remote(RemoteLifecycle::Persistent) => {
                clap::builder::PossibleValue::new("remote-persistent")
                    .help("Durable unversioned ClickHouse objects created once at boot")
            }
            DdlTarget::Local => clap::builder::PossibleValue::new("local")
                .help("DuckDB DDL (local graph tables + manifest)"),
        })
    }
}

#[derive(Subcommand)]
enum SynthCommand {
    /// Generate synthetic SDLC data to Parquet files.
    Generate {
        /// Path to YAML configuration file.
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"))]
        config: std::path::PathBuf,

        /// Print the generation plan without executing.
        #[arg(long)]
        dry_run: bool,

        /// Force regeneration even if data exists.
        #[arg(long)]
        force: bool,
    },
    /// Load generated Parquet data into ClickHouse.
    Load {
        /// Path to YAML configuration file.
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"))]
        config: std::path::PathBuf,

        /// Skip creating/dropping tables (useful for reloading).
        #[arg(long)]
        no_schema: bool,

        /// Skip loading data (useful for just adding indexes/projections).
        #[arg(long)]
        no_data: bool,

        /// Skip adding indexes.
        #[arg(long)]
        no_indexes: bool,

        /// Skip adding projections.
        #[arg(long)]
        no_projections: bool,

        /// Use clickhouse-client CLI for loading (faster, more reliable).
        #[arg(long)]
        use_cli: bool,
    },
    /// Execute SDLC queries and collect statistics.
    Evaluate {
        /// Path to YAML configuration file.
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"))]
        config: std::path::PathBuf,

        /// Verbose output.
        #[arg(short, long)]
        verbose: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Synth { command } => match command {
            SynthCommand::Generate {
                config,
                dry_run,
                force,
            } => synth::generator::run::run(&config, dry_run, force),
            SynthCommand::Load {
                config,
                no_schema,
                no_data,
                no_indexes,
                no_projections,
                use_cli,
            } => {
                synth::load::run::run(
                    &config,
                    no_schema,
                    no_data,
                    no_indexes,
                    no_projections,
                    use_cli,
                )
                .await
            }
            SynthCommand::Evaluate { config, verbose } => {
                synth::evaluation::run::run(&config, verbose).await
            }
        },
        Command::Ddl {
            target,
            ontology,
            prefix,
            diff,
        } => match target {
            DdlTarget::Remote(RemoteLifecycle::Versioned) => {
                ddl::run_remote(ontology, prefix, diff)
            }
            DdlTarget::Remote(RemoteLifecycle::Persistent) => {
                if !prefix.is_empty() {
                    anyhow::bail!("--prefix is only supported for --target remote");
                }
                ddl::run_persistent(ontology, diff)
            }
            DdlTarget::Local => {
                if !prefix.is_empty() || diff.is_some() {
                    anyhow::bail!(
                        "--prefix is only supported for --target remote; --diff is supported for --target remote and --target remote-persistent"
                    );
                }
                ddl::run_local(ontology)
            }
        },
        Command::Schema { output } => schema::run(output),
        Command::SchemaPublicOutput {
            check,
            base,
            skip_pin_check,
        } => schema_public_output::run(check, &base, skip_pin_check),
        Command::MigrationLedger { command } => match command {
            MigrationLedgerCommand::Bump {
                scope,
                entities,
                note,
                base,
                amend,
                new,
            } => migration_ledger::bump(scope, entities, note, base, amend, new),
            MigrationLedgerCommand::Check { base } => migration_ledger::check(base),
            MigrationLedgerCommand::Snapshot => migration_ledger::snapshot(),
        },
        Command::MetricsCatalog { output, check } => metrics_catalog::run(output, check),
        Command::Dashboards { dir, check } => dashboards::run(dir, check),
        Command::IntegrationLanes { check } => integration_lanes::run(check),
        Command::QueryDocs { doc, check } => query_docs::run(doc, check),
        Command::Loadtest {
            endpoint,
            concurrency,
            rounds,
            scenarios,
            query,
            no_admin,
            timeout,
        } => {
            loadtest::run(loadtest::Options {
                endpoint,
                concurrency,
                rounds,
                scenarios,
                query,
                admin: !no_admin,
                per_call_timeout: std::time::Duration::from_secs(timeout),
            })
            .await
        }
    }
}
