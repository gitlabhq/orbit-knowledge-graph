//! Query evaluation CLI.
//!
//! Loads SDLC queries, samples parameters, executes queries, and reports results.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::evaluation::{QueryExecutor, Report, ReportFormat, load_queries};
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Parser)]
#[command(name = "evaluate")]
#[command(about = "Execute SDLC queries and collect statistics")]
struct Args {
    /// Path to the queries JSON file
    #[arg(short, long, default_value = "fixtures/queries/sdlc_queries.json")]
    queries: PathBuf,

    /// Path to the ontology directory
    #[arg(short, long, default_value = "fixtures/ontology")]
    ontology: PathBuf,

    /// ClickHouse URL
    #[arg(short, long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// Number of IDs to sample per entity type
    #[arg(short, long, default_value = "100")]
    sample_size: usize,

    /// Output format (text, json, markdown)
    #[arg(short, long, default_value = "text")]
    format: String,

    /// Output file (stdout if not specified)
    #[arg(short = 'O', long)]
    output: Option<PathBuf>,

    /// Run queries multiple times
    #[arg(short, long, default_value = "1")]
    iterations: usize,

    /// Skip cache warming
    #[arg(long)]
    no_warm_cache: bool,

    /// Only run queries matching this pattern
    #[arg(long)]
    filter: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let filter = if args.verbose {
        "simulator=debug,info"
    } else {
        "simulator=info,warn"
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)))
        .init();

    tracing::info!("Loading ontology from {:?}", args.ontology);
    let ontology = Ontology::load_from_dir(&args.ontology)?;

    tracing::info!("Loading queries from {:?}", args.queries);
    let mut queries = load_queries(&args.queries)?;

    // Apply filter if specified
    if let Some(pattern) = &args.filter {
        let pattern_lower = pattern.to_lowercase();
        queries.retain(|name, _| name.to_lowercase().contains(&pattern_lower));
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

    // Create executor
    let mut executor = QueryExecutor::new(&args.clickhouse_url, ontology, args.sample_size);

    // Warm cache
    if !args.no_warm_cache {
        tracing::info!("Warming parameter cache...");
        executor.warm_cache().await?;

        let stats = executor.cache_stats();
        for (entity, count) in &stats {
            tracing::debug!("  {}: {} IDs sampled", entity, count);
        }
        tracing::info!("Cache warmed: {} entity types", stats.len());
    }

    // Execute queries
    tracing::info!(
        "Executing {} queries ({} iteration(s))...",
        queries.len(),
        args.iterations
    );

    let mut all_results = Vec::new();

    for iteration in 0..args.iterations {
        if args.iterations > 1 {
            tracing::info!("Iteration {}/{}", iteration + 1, args.iterations);
        }

        let results = executor.execute_all(&queries).await;

        for result in &results {
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

        all_results.extend(results);
    }

    // Generate report
    let report = Report::new(all_results);
    let format: ReportFormat = args.format.parse().unwrap_or_default();
    let output = report.format(format);

    // Write output
    let wrote_to_file = if let Some(ref path) = args.output {
        std::fs::write(path, &output)?;
        tracing::info!("Report written to {:?}", path);
        true
    } else {
        println!("{}", output);
        false
    };

    // Print quick summary to stderr if outputting to file
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
