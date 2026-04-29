//! Query evaluation runner.

use super::{ExecutionResult, QueryExecutor, Report, ReportFormat, RunConfig, RunMetadata};
use crate::synth::clickhouse::check_clickhouse_health;
use crate::synth::config::Config;
use anyhow::Result;
use ontology::Ontology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A query entry from the queries YAML file.
///
/// Each entry has a stable key (`q1`..`qN`), a human-readable description,
/// and the raw GKG query DSL as an inline JSON string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEntry {
    /// Human-readable description of what this query tests.
    pub desc: String,
    /// Raw JSON query DSL string, passed to the query engine compiler.
    pub query: String,
}

impl QueryEntry {
    /// Parse the `query` JSON string into a `serde_json::Value`.
    pub fn parse_query(&self) -> Result<serde_json::Value> {
        serde_json::from_str(&self.query).map_err(Into::into)
    }
}

/// Load queries from a YAML file.
///
/// ```yaml
/// q1:
///   desc: List active users
///   query: |
///     {"query_type": "traversal", ...}
/// ```
pub fn load_queries(path: &Path) -> Result<HashMap<String, QueryEntry>> {
    use anyhow::Context;

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read queries file: {}", path.display()))?;

    let queries: HashMap<String, QueryEntry> = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse queries file: {}", path.display()))?;

    for (key, entry) in &queries {
        entry
            .parse_query()
            .with_context(|| format!("Invalid JSON in query '{}' ({})", key, entry.desc))?;
    }
    Ok(queries)
}

/// Parameters extracted from a query that need sampling.
#[derive(Debug, Clone, Default)]
pub struct QueryParameters {
    /// Entity type -> list of node_ids fields that need sampling
    pub node_ids: HashMap<String, Vec<String>>,
}

/// Extract parameters that need sampling from a query's JSON value.
pub fn extract_parameters(query_value: &serde_json::Value) -> QueryParameters {
    let mut params = QueryParameters::default();

    let mut all_nodes: Vec<&serde_json::Value> = Vec::new();
    if let Some(nodes) = query_value.get("nodes").and_then(|n| n.as_array()) {
        all_nodes.extend(nodes);
    }
    if let Some(node) = query_value.get("node") {
        all_nodes.push(node);
    }

    for node in all_nodes {
        if let Some(obj) = node.as_object()
            && obj.contains_key("node_ids")
            && let Some(entity) = obj.get("entity").and_then(|e| e.as_str())
            && let Some(id) = obj.get("id").and_then(|i| i.as_str())
        {
            params
                .node_ids
                .entry(entity.to_string())
                .or_default()
                .push(id.to_string());
        }
    }

    params
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

    tracing::info!("Loading config from {:?}", config_path);
    let config = Config::load(config_path)?;
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

    let mut executor = QueryExecutor::new(
        client,
        ontology,
        config.evaluation.sample_size,
        &config.evaluation.settings,
    );

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

fn log_result(result: &ExecutionResult) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_parameters() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "merger", "entity": "User", "node_ids": [42]}
            ],
            "relationships": [
                {"type": "MERGED", "from": "merger", "to": "mr"}
            ],
            "limit": 30
        }"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();

        let params = extract_parameters(&value);
        assert!(params.node_ids.contains_key("User"));
        assert_eq!(params.node_ids["User"], vec!["merger".to_string()]);
    }

    #[test]
    fn test_query_entry_parse() {
        let entry = QueryEntry {
            desc: "test query".into(),
            query: r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"}}"#.into(),
        };
        let value = entry.parse_query().unwrap();
        assert_eq!(value["query_type"], "traversal");
    }
}
