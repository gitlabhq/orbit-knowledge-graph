use super::{ExecutionResult, QueryExecutor, Report, ReportFormat, RunConfig, RunMetadata};
use crate::synth::clickhouse::check_clickhouse_health;
use crate::synth::config::Config;
use anyhow::Result;
use ontology::Ontology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEntry {
    pub desc: String,
    pub query: String,
}

/// Load queries from a YAML file.
///
/// ```yaml
/// q1:
///   desc: List active users
///   query: |
///     MATCH (u:User {state: 'active'}) RETURN * LIMIT 30
/// ```
pub fn load_queries(path: &Path) -> Result<HashMap<String, QueryEntry>> {
    use anyhow::Context;

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read queries file: {}", path.display()))?;

    orbit_utils::yaml::from_str(&content)
        .with_context(|| format!("Failed to parse queries file: {}", path.display()))
}

pub async fn run(config_path: &Path, verbose: bool) -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    let filter = if verbose {
        "xtask=debug,info"
    } else {
        "xtask=info,warn"
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)))
        .init();

    orbit_log_fields::info!(expanded(LOCAL_PATH) = %config_path.display(), "loading config");
    let config = Config::load(config_path)?;
    config.evaluation.validate()?;

    orbit_log_fields::info!(
        expanded(LOCAL_PATH) = %config.generation.ontology_path,
        "loading ontology"
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;

    orbit_log_fields::info!(
        expanded(LOCAL_PATH) = %config.evaluation.queries_path,
        "loading queries"
    );
    let mut queries = load_queries(config.evaluation.queries_path.as_ref())?;

    if let Some(pattern) = &config.evaluation.filter {
        let pattern_lower = pattern.to_lowercase();
        queries.retain(|key, entry| {
            key.to_lowercase().contains(&pattern_lower)
                || entry.desc.to_lowercase().contains(&pattern_lower)
        });
        orbit_log_fields::info!(
            safe(COUNT) = queries.len(),
            expanded(ADDITIONAL_DETAILS) = %pattern,
            "filtered queries by pattern"
        );
    }

    if queries.is_empty() {
        orbit_log_fields::warn!("no queries to execute");
        return Ok(());
    }

    orbit_log_fields::info!(safe(COUNT) = queries.len(), "loaded queries");

    orbit_log_fields::info!(
        expanded(URL) = %config.clickhouse.url,
        "checking ClickHouse connection"
    );
    let client = config.clickhouse.build_client();
    check_clickhouse_health(&client).await?;
    orbit_log_fields::info!("ClickHouse is healthy");

    let mut executor = QueryExecutor::new(
        client,
        ontology,
        config.evaluation.sample_size,
        &config.evaluation.settings,
    );

    if !config.evaluation.skip_cache_warm {
        orbit_log_fields::info!("warming parameter cache");
        executor
            .warm_cache(&config.generation.namespace_entity)
            .await?;

        let stats = executor.cache_stats();
        for (entity, count) in &stats {
            orbit_log_fields::debug!(
                safe(ENTITY) = entity.as_str(),
                safe(COUNT) = *count,
                "sampled IDs"
            );
        }
        orbit_log_fields::info!(
            safe(COUNT) = stats.len(),
            "warmed parameter cache for entity types"
        );
    }

    let concurrency = config.evaluation.concurrency;
    orbit_log_fields::info!(
        safe(COUNT) = queries.len(),
        safe(ITERATION_COUNT) = config.evaluation.iterations,
        safe(CONCURRENCY) = concurrency,
        "executing queries"
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
            orbit_log_fields::info!(
                safe(ITERATION) = iteration + 1,
                safe(ITERATION_COUNT) = config.evaluation.iterations,
                "starting iteration"
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
        orbit_log_fields::info!(expanded(LOCAL_PATH) = %path, "report written");
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

fn log_result(result: &ExecutionResult) {
    if result.success {
        orbit_log_fields::debug!(
            safe(QUERY_NAME) = result.query_name.as_str(),
            safe(ROW_COUNT) = result.row_count.unwrap_or(0),
            safe(DURATION_S) = result.execution_time.as_secs_f64(),
            "query succeeded"
        );
    } else {
        orbit_log_fields::warn!(
            safe(QUERY_NAME) = result.query_name.as_str(),
            expanded(ERROR_MESSAGE) = result.error.as_deref().unwrap_or("unknown error"),
            "query failed"
        );
    }
}
