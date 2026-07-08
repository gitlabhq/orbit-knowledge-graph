use anyhow::Result;
use clap::{Parser, Subcommand};

mod dashboards;
mod ddl;
mod metrics_catalog;
mod migration_ledger;
mod query_docs;
mod schema;
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
    /// Manage the schema-migration ledger (config/schema-migrations.yaml).
    MigrationLedger {
        #[command(subcommand)]
        command: MigrationLedgerCommand,
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
        /// Ref whose SCHEMA_VERSION decides bump-vs-amend (default origin/main).
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
    /// Regenerate the fingerprint snapshot without touching SCHEMA_VERSION or
    /// the ledger. For no-op ontology drift (e.g. removing a declaration that
    /// never emitted DDL) where `mise schema:bump` would wrongly imply a
    /// version bump is needed.
    Snapshot,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
enum DdlTarget {
    /// ClickHouse DDL (tables, dictionaries, materialized views).
    #[default]
    Remote,
    /// DuckDB DDL (local graph tables + manifest).
    Local,
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
            DdlTarget::Remote => ddl::run_remote(ontology, prefix, diff),
            DdlTarget::Local => {
                if !prefix.is_empty() || diff.is_some() {
                    anyhow::bail!("--prefix and --diff are only supported for --target remote");
                }
                ddl::run_local(ontology)
            }
        },
        Command::Schema { output } => schema::run(output),
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
        Command::QueryDocs { doc, check } => query_docs::run(doc, check),
    }
}
