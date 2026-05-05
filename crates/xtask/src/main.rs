use anyhow::Result;
use clap::{Parser, Subcommand};

mod bench;
mod dashboards;
mod metrics_catalog;
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
    /// Benchmark the indexer write pipeline against ClickHouse.
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
}

#[derive(Subcommand)]
enum BenchCommand {
    /// Benchmark extract → transform → write for the Job pipeline.
    WritePipeline {
        /// ClickHouse HTTP URL.
        #[arg(long, default_value = "http://localhost:8123")]
        clickhouse_url: String,
        /// ClickHouse database.
        #[arg(long, default_value = "default")]
        database: String,
        /// ClickHouse username.
        #[arg(long, default_value = "default")]
        username: String,
        /// ClickHouse password.
        #[arg(long)]
        password: Option<String>,
        /// Total rows to seed in siphon_p_ci_builds.
        #[arg(long, default_value = "100000000")]
        total_rows: u64,
        /// Rows per extraction batch.
        #[arg(long, default_value = "500000")]
        batch_size: u64,
        /// Max concurrent ClickHouse writes per batch.
        #[arg(long, default_value = "3")]
        max_concurrent_writes: usize,
        /// Rows per seed INSERT batch.
        #[arg(long, default_value = "1000000")]
        seed_batch_size: u64,
        /// Lock file to skip re-seeding.
        #[arg(long, default_value = ".bench-seeded.lock")]
        lock_file: std::path::PathBuf,
        /// Stop after N batches (for fast iteration).
        #[arg(long)]
        max_batches: Option<u64>,
        /// Use ClickHouse async_insert for writes.
        #[arg(long)]
        async_insert: bool,
        /// Merge all edge transform outputs into a single write.
        #[arg(long)]
        coalesce_edges: bool,
        /// Split each write into sub-batches of this many rows.
        #[arg(long)]
        write_chunk_size: Option<usize>,
        /// Overlap extract(N+1) with write(N) for each batch.
        #[arg(long)]
        pipeline: bool,
        /// Use OFFSET-based pagination instead of cursor-based.
        #[arg(long)]
        use_offset: bool,
        /// Use ZSTD compression for Arrow IPC instead of LZ4.
        #[arg(long)]
        zstd: bool,
    },
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
        Command::Schema { output } => schema::run(output),
        Command::MetricsCatalog { output, check } => metrics_catalog::run(output, check),
        Command::Dashboards { dir, check } => dashboards::run(dir, check),
        Command::Bench { command } => match command {
            BenchCommand::WritePipeline {
                clickhouse_url,
                database,
                username,
                password,
                total_rows,
                batch_size,
                max_concurrent_writes,
                seed_batch_size,
                lock_file,
                max_batches,
                async_insert,
                coalesce_edges,
                write_chunk_size,
                pipeline,
                use_offset,
                zstd,
            } => {
                bench::write_pipeline::run(&bench::write_pipeline::BenchConfig {
                    clickhouse_url,
                    database,
                    username,
                    password,
                    total_rows,
                    batch_size,
                    max_concurrent_writes,
                    seed_batch_size,
                    lock_file,
                    max_batches,
                    async_insert,
                    coalesce_edges,
                    write_chunk_size,
                    pipeline,
                    use_offset,
                    zstd,
                })
                .await
            }
        },
    }
}
