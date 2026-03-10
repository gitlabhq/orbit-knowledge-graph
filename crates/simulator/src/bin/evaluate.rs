//! CLI for query evaluation.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::Config;
use simulator::clickhouse::check_clickhouse_health;
use simulator::evaluation::{
    QueryExecutor, Report, ReportFormat, RunConfig, RunMetadata, load_queries,
};
use std::path::{Path, PathBuf};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Parser)]
#[command(name = "evaluate")]
#[command(about = "Execute SDLC queries and collect statistics")]
struct Args {
    /// Path to YAML configuration file
    #[arg(short, long, default_value = "simulator.yaml")]
    config: PathBuf,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let filter = if args.verbose {
        "simulator=debug,info"
    } else {
        "simulator=info,warn"
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)))
        .init();

    tracing::info!("Loading config from {:?}", args.config);
    let config = Config::load(&args.config)?;
    config.evaluation.validate()?;

    tracing::info!(
        "Loading ontology from {:?}",
        config.generation.ontology_path
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;

    tracing::info!("Loading queries from {:?}", config.evaluation.queries_path);
    let mut queries = load_queries(config.evaluation.queries_path.as_ref())?;

    if let Some(pattern) = &config.evaluation.filter {
        let pattern_lower = pattern.to_lowercase();
        queries.retain(|key, entry| {
            key.to_lowercase().contains(&pattern_lower)
                || entry.desc.to_lowercase().contains(&pattern_lower)
        });
        tracing::info!(
            "Filtered to {} queries matching '{}'",
            queries.len(),
            pattern
        );
    }

    if queries.is_empty() {
        tracing::warn!("No queries to execute");
        return Ok(());
    }

    tracing::info!("Loaded {} queries", queries.len());

    // Check ClickHouse connectivity before proceeding
    tracing::info!(
        "Checking ClickHouse connection at {}...",
        config.clickhouse.url
    );
    let client = config.clickhouse.build_client();
    check_clickhouse_health(&client).await?;
    tracing::info!("ClickHouse is healthy");

    let mut executor = QueryExecutor::new(client, ontology, config.evaluation.sample_size);

    if !config.evaluation.skip_cache_warm {
        tracing::info!("Warming parameter cache...");
        executor
            .warm_cache(&config.generation.namespace_entity)
            .await?;

        let stats = executor.cache_stats();
        for (entity, count) in &stats {
            tracing::debug!("  {}: {} IDs sampled", entity, count);
        }
        tracing::info!("Cache warmed: {} entity types", stats.len());
    }

    let concurrency = config.evaluation.concurrency;
    tracing::info!(
        "Executing {} queries ({} iteration(s), concurrency={})...",
        queries.len(),
        config.evaluation.iterations,
        concurrency
    );

    let mut run_metadata = RunMetadata::new(RunConfig {
        clickhouse_url: config.clickhouse.url.clone(),
        iterations: config.evaluation.iterations,
        sample_size: config.evaluation.sample_size,
        filter: config.evaluation.filter.clone(),
    });

    let mut all_results = Vec::new();

    for iteration in 0..config.evaluation.iterations {
        if config.evaluation.iterations > 1 {
            tracing::info!(
                "Iteration {}/{}",
                iteration + 1,
                config.evaluation.iterations
            );
        }

        let results = if concurrency > 1 {
            executor.execute_all_concurrent(&queries, concurrency).await
        } else {
            executor.execute_all(&queries).await
        };

        for (result, metadata) in results {
            log_result(&result);
            run_metadata.add_query(metadata);
            all_results.push(result);
        }
    }

    run_metadata.complete();
    if let Some(ref dir) = config.evaluation.metadata_dir {
        run_metadata.save_to_dir(Path::new(dir))?;
    }

    let report = Report::new(all_results);
    let format: ReportFormat = config.evaluation.output.format.parse().unwrap_or_default();
    let output = report.format(format);

    let wrote_to_file = if let Some(ref path) = config.evaluation.output.path {
        std::fs::write(path, &output)?;
        tracing::info!("Report written to {:?}", path);
        true
    } else {
        println!("{}", output);
        false
    };

    if wrote_to_file {
        eprintln!(
            "Completed: {}/{} successful ({:.1}%)",
            report.summary.successful,
            report.summary.total_queries,
            report.summary.success_rate()
        );
    }

    Ok(())
}

fn log_result(result: &simulator::evaluation::ExecutionResult) {
    if result.success {
        tracing::debug!(
            "✓ {} - {} rows in {:.2}ms",
            result.query_name,
            result.row_count.unwrap_or(0),
            result.execution_time.as_secs_f64() * 1000.0
        );
    } else {
        tracing::warn!(
            "✗ {} - {}",
            result.query_name,
            result.error.as_deref().unwrap_or("unknown error")
        );
    }
}
