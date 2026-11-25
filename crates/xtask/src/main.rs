use anyhow::Result;
use clap::{Parser, Subcommand};

mod code_indexing_benchmark;
mod dashboards;
mod ddl;
mod integration_lanes;
#[cfg(feature = "loadtest")]
mod loadtest;
mod metrics_catalog;
mod migration_ledger;
mod proto;
mod query_docs;
mod synth;

#[derive(Parser)]
#[command(
    name = "xtask",
    version,
    about = "GKG development task runner",
    long_about = "GKG development task runner.\n\n\
        Automates common development workflows like synthetic data generation, query evaluation, and schema management."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Run code-indexing benchmarks on pinned repositories")]
    CodeIndexingBenchmark {
        #[arg(value_enum)]
        action: code_indexing_benchmark::Action,
        #[arg(
            long,
            env = "ORBIT_BENCH_ROOT",
            help = "Cache root for repositories, builds, index data, and results"
        )]
        root: Option<std::path::PathBuf>,
        #[arg(
            long,
            default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/../../code-indexing-benchmark.yaml"),
            help = "Repository corpus configuration",
        )]
        config: std::path::PathBuf,
    },
    #[command(about = "Synthetic graph data pipeline (generate, load, evaluate)")]
    Synth {
        #[command(subcommand)]
        command: SynthCommand,
    },
    #[command(
        about = "Regenerate the prost types and descriptor set for the server configuration schema"
    )]
    ProtoConfig,
    #[command(about = "Generate the shared metrics catalog consumed by runbooks dashboards")]
    MetricsCatalog {
        #[arg(
            short,
            long,
            help = "Write catalog JSON to this path instead of the default"
        )]
        output: Option<std::path::PathBuf>,
        #[arg(
            long,
            help = "Diff the regenerated catalog against the committed file and return a non-zero exit if they differ"
        )]
        check: bool,
    },
    #[command(about = "Generate graph DDL from the ontology")]
    Ddl {
        #[arg(
            long,
            short,
            default_value = "remote",
            help = "Target database dialect"
        )]
        target: DdlTarget,

        #[arg(long, short, help = "Path to ontology directory (default: embedded)")]
        ontology: Option<std::path::PathBuf>,

        #[arg(
            long,
            short,
            default_value = "",
            help = "Table prefix (e.g., \"v1_\"; remote only)"
        )]
        prefix: String,

        #[arg(
            long,
            short,
            help = "Diff generated DDL against an existing .sql file (remote only)"
        )]
        diff: Option<std::path::PathBuf>,
    },
    #[command(about = "Generate the Orbit Grafana dashboards from the metric catalog")]
    Dashboards {
        #[arg(
            short,
            long,
            help = "Write dashboards under this directory instead of the default"
        )]
        dir: Option<std::path::PathBuf>,
        #[arg(
            long,
            help = "Diff regenerated dashboards against the committed files and return a non-zero exit if they differ"
        )]
        check: bool,
    },
    #[command(about = "Manage the schema-migration ledger (config/schema-migrations.yaml)")]
    MigrationLedger {
        #[command(subcommand)]
        command: MigrationLedgerCommand,
    },
    #[command(about = "Verify that the integration lanes partition every container test")]
    IntegrationLanes {
        #[arg(long, help = "Check the filters in .gitlab-ci.yml")]
        check: bool,
    },
    #[command(
        about = "Regenerate the auto-derived tables in the query language reference doc from the ontology (currently the text-indexed properties table)"
    )]
    QueryDocs {
        #[arg(short, long, help = "Path to the doc to update instead of the default")]
        doc: Option<std::path::PathBuf>,
        #[arg(
            long,
            help = "Diff the regenerated table against the committed doc and return a non-zero exit if they differ"
        )]
        check: bool,
    },
    #[command(
        about = "Run a gRPC load test against a running Orbit server, replaying the performance query corpus and reporting latency percentiles (plus optional ClickHouse server-side work per query)",
        long_about = "Run a gRPC load test against a running Orbit server, replaying the performance query corpus and reporting latency percentiles (plus optional ClickHouse server-side work per query).\n\n\
            Requires GKG_JWT_SECRET (base64-encoded HMAC key, same as the server)."
    )]
    #[cfg(feature = "loadtest")]
    Loadtest {
        #[arg(
            long,
            env = "ORBIT_ENDPOINT",
            default_value = "http://127.0.0.1:50054",
            help = "gRPC endpoint of the Orbit server (plaintext HTTP/2)"
        )]
        endpoint: String,

        #[arg(
            long,
            default_value_t = 20,
            help = "Requests per query per round, all in flight at once"
        )]
        concurrency: usize,

        #[arg(
            long,
            default_value_t = 5,
            help = "Measured rounds; each runs every query once in seeded shuffled order. Requests per query = concurrency * rounds"
        )]
        rounds: usize,

        #[arg(
            long,
            default_value_t = 1,
            help = "Discarded rounds, shaped like measured ones, before measuring (0 = none)"
        )]
        warmup_rounds: usize,

        #[arg(
            long,
            default_value_t = 42,
            help = "Seed for the per-round query order"
        )]
        seed: u64,

        #[arg(
            long,
            env = "ORBIT_PERF_CLICKHOUSE_URL",
            help = "ClickHouse HTTP URL for server-side stats from system.query_log (unset = skip)"
        )]
        clickhouse_url: Option<String>,

        #[arg(
            long,
            env = "ORBIT_PERF_CLICKHOUSE_USER",
            default_value = "default",
            help = "ClickHouse user for server-side stats"
        )]
        clickhouse_user: String,

        #[arg(
            long,
            env = "ORBIT_PERF_CLICKHOUSE_PASSWORD",
            hide_env_values = true,
            help = "ClickHouse password for server-side stats"
        )]
        clickhouse_password: Option<String>,

        #[arg(
            long,
            env = "CI_JOB_ID",
            help = "Run id embedded in every correlation id ([A-Za-z0-9-]; default: timestamp-derived)"
        )]
        run_id: Option<String>,

        #[arg(
            long,
            default_value = "crates/integration-tests/tests/server/performance/scenarios",
            help = "Directory of scenario YAML files to replay (recursively)"
        )]
        scenarios: std::path::PathBuf,

        #[arg(
            long,
            help = "Run a single scenario whose label contains this substring"
        )]
        query: Option<String>,

        #[arg(
            long,
            help = "Sign the JWT as a non-admin user (admin bypasses path scoping)"
        )]
        no_admin: bool,

        #[arg(
            long,
            default_value_t = 30,
            help = "Per-request gRPC deadline, in seconds"
        )]
        timeout: u64,

        #[arg(
            long,
            help = "Print each scenario's `<entity>\\t<node id>\\t<label>` and exit without connecting"
        )]
        list_node_ids: bool,
    },
}

#[derive(Subcommand)]
enum MigrationLedgerCommand {
    #[command(about = "Recompute fingerprints, derive the scope, and append or amend an entry")]
    Bump {
        #[arg(long, help = "Widen the derived scope: `*`, `sdlc`, or `code`")]
        scope: Option<String>,
        #[arg(
            long,
            help = "Comma-separated SDLC entities (with `--scope sdlc`) to widen the entry"
        )]
        entities: Option<String>,
        #[arg(long, help = "Note recorded on the entry")]
        note: Option<String>,
        #[arg(
            long,
            help = "Ref whose schema pin decides bump-vs-amend (default origin/main)"
        )]
        base: Option<String>,
        #[arg(long, help = "Force amend when the base ref is unavailable")]
        amend: bool,
        #[arg(long, help = "Force a new bump when the base ref is unavailable")]
        new: bool,
    },
    #[command(about = "Verify the committed snapshot and ledger match the working ontology")]
    Check {
        #[arg(
            long,
            help = "Enforce the under-declaration guard against this base ref"
        )]
        base: Option<String>,
    },
    #[command(
        about = "Snapshot auxiliary tables and refreshable views without changing the schema pin"
    )]
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
    #[command(about = "Generate synthetic SDLC data to Parquet files")]
    Generate {
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"), help = "Path to YAML configuration file")]
        config: std::path::PathBuf,

        #[arg(long, help = "Print the generation plan without executing")]
        dry_run: bool,

        #[arg(long, help = "Force regeneration even if data exists")]
        force: bool,
    },
    #[command(about = "Load generated Parquet data into ClickHouse")]
    Load {
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"), help = "Path to YAML configuration file")]
        config: std::path::PathBuf,

        #[arg(long, help = "Skip creating/dropping tables (useful for reloading)")]
        no_schema: bool,

        #[arg(
            long,
            help = "Skip loading data (useful for just adding indexes/projections)"
        )]
        no_data: bool,

        #[arg(long, help = "Skip adding indexes")]
        no_indexes: bool,

        #[arg(long, help = "Skip adding projections")]
        no_projections: bool,

        #[arg(
            long,
            help = "Use clickhouse-client CLI for loading (faster, more reliable)"
        )]
        use_cli: bool,
    },
    #[command(about = "Execute SDLC queries and collect statistics")]
    Evaluate {
        #[arg(short, long, default_value = concat!(env!("XTASK_DIR"), "/simulator.yaml"), help = "Path to YAML configuration file")]
        config: std::path::PathBuf,

        #[arg(short, long, help = "Verbose output")]
        verbose: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::CodeIndexingBenchmark {
            action,
            root,
            config,
        } => {
            code_indexing_benchmark::run(code_indexing_benchmark::Options {
                action,
                root,
                config,
            })
            .await
        }
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
        Command::ProtoConfig => proto::run(),
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
        #[cfg(feature = "loadtest")]
        Command::Loadtest {
            endpoint,
            concurrency,
            rounds,
            warmup_rounds,
            seed,
            clickhouse_url,
            clickhouse_user,
            clickhouse_password,
            run_id,
            scenarios,
            query,
            no_admin,
            timeout,
            list_node_ids,
        } => {
            if list_node_ids {
                return loadtest::list_node_ids(&scenarios, query.as_deref());
            }
            loadtest::run(loadtest::Options {
                endpoint,
                concurrency,
                rounds,
                warmup_rounds,
                seed,
                scenarios,
                query,
                admin: !no_admin,
                per_call_timeout: std::time::Duration::from_secs(timeout),
                run_id,
                clickhouse: clickhouse_url.map(|url| loadtest::ClickHouseOptions {
                    url,
                    user: clickhouse_user,
                    password: clickhouse_password,
                }),
            })
            .await
        }
    }
}
