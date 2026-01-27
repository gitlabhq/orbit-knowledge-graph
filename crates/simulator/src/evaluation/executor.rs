//! Query execution and statistics collection.

use super::{QueryDefinition, ParameterSampler};
use anyhow::Result;
use clickhouse::Client;
use ontology::Ontology;
use query_engine::compile;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Maximum memory per query (500MB) to prevent exhausting server memory.
/// Keep this well under the ClickHouse server's total memory limit.
const MAX_MEMORY_USAGE: &str = "500000000";

/// Result of executing a single query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Name of the query.
    pub query_name: String,
    /// Whether the query executed successfully.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of rows returned.
    pub row_count: Option<u64>,
    /// Execution time.
    pub execution_time: Duration,
    /// The SQL that was executed (for debugging).
    pub sql: Option<String>,
    /// Parameters used.
    pub params: Option<serde_json::Value>,
}

impl ExecutionResult {
    pub fn success(
        query_name: String,
        row_count: u64,
        execution_time: Duration,
        sql: String,
        params: serde_json::Value,
    ) -> Self {
        Self {
            query_name,
            success: true,
            error: None,
            row_count: Some(row_count),
            execution_time,
            sql: Some(sql),
            params: Some(params),
        }
    }

    pub fn failure(query_name: String, error: String, execution_time: Duration) -> Self {
        Self {
            query_name,
            success: false,
            error: Some(error),
            row_count: None,
            execution_time,
            sql: None,
            params: None,
        }
    }
}

/// Executes queries against ClickHouse and collects statistics.
pub struct QueryExecutor {
    client: Client,
    ontology: Ontology,
    sampler: ParameterSampler,
}

impl QueryExecutor {
    pub fn new(clickhouse_url: &str, ontology: Ontology, sample_size: usize) -> Self {
        let client = Client::default().with_url(clickhouse_url);
        let sampler = ParameterSampler::new(clickhouse_url, sample_size);
        Self {
            client,
            ontology,
            sampler,
        }
    }

    /// Warm the parameter cache.
    pub async fn warm_cache(&mut self) -> Result<()> {
        self.sampler.warm_cache(&self.ontology).await
    }

    /// Execute a single query with sampled parameters.
    pub async fn execute_query(
        &mut self,
        name: &str,
        query: &QueryDefinition,
    ) -> ExecutionResult {
        let start = Instant::now();

        // Substitute parameters with sampled values
        let substituted = match self.substitute_parameters(query).await {
            Ok(q) => q,
            Err(e) => {
                return ExecutionResult::failure(
                    name.to_string(),
                    format!("Parameter substitution failed: {}", e),
                    start.elapsed(),
                );
            }
        };

        // Compile JSON to SQL
        let json_str = match serde_json::to_string(&substituted) {
            Ok(s) => s,
            Err(e) => {
                return ExecutionResult::failure(
                    name.to_string(),
                    format!("JSON serialization failed: {}", e),
                    start.elapsed(),
                );
            }
        };

        let compiled = match compile(&json_str, &self.ontology) {
            Ok(c) => c,
            Err(e) => {
                return ExecutionResult::failure(
                    name.to_string(),
                    format!("Query compilation failed: {}", e),
                    start.elapsed(),
                );
            }
        };

        // Build the final SQL with parameters substituted
        let final_sql = substitute_params_in_sql(&compiled.sql, &compiled.params);

        // Execute the query
        match self.execute_sql(&final_sql).await {
            Ok(row_count) => ExecutionResult::success(
                name.to_string(),
                row_count,
                start.elapsed(),
                compiled.sql,
                serde_json::to_value(&compiled.params).unwrap_or_default(),
            ),
            Err(e) => ExecutionResult::failure(
                name.to_string(),
                format!("Execution failed: {}", e),
                start.elapsed(),
            ),
        }
    }

    /// Execute all queries and return results.
    /// 
    /// Includes a small delay between queries to allow ClickHouse to recover
    /// from memory pressure.
    pub async fn execute_all(
        &mut self,
        queries: &std::collections::HashMap<String, QueryDefinition>,
    ) -> Vec<ExecutionResult> {
        let mut results = Vec::with_capacity(queries.len());

        for (name, query) in queries {
            let result = self.execute_query(name, query).await;
            
            // If we hit a memory error, give ClickHouse time to recover
            if !result.success {
                if let Some(ref err) = result.error {
                    if err.contains("MEMORY_LIMIT") || err.contains("network error") {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
            
            results.push(result);
        }

        results
    }

    /// Substitute placeholder node_ids with sampled values.
    async fn substitute_parameters(
        &mut self,
        query: &QueryDefinition,
    ) -> Result<serde_json::Value> {
        let mut query_value = serde_json::to_value(query)?;

        if let Some(nodes) = query_value.get_mut("nodes").and_then(|n| n.as_array_mut()) {
            for node in nodes.iter_mut() {
                if let Some(obj) = node.as_object_mut() {
                    // Check if this node has node_ids
                    if obj.contains_key("node_ids") {
                        if let Some(entity) = obj.get("entity").and_then(|e| e.as_str()) {
                            // Get the current node_ids to determine how many we need
                            let count = obj
                                .get("node_ids")
                                .and_then(|ids| ids.as_array())
                                .map(|arr| arr.len())
                                .unwrap_or(1);

                            // Sample new IDs
                            let sampled_ids = self
                                .sampler
                                .random_ids(entity, count, &self.ontology)
                                .await?;

                            if !sampled_ids.is_empty() {
                                obj.insert(
                                    "node_ids".to_string(),
                                    serde_json::to_value(&sampled_ids)?,
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(query_value)
    }

    /// Execute raw SQL and return row count.
    /// 
    /// For correctness testing, we just verify the query runs and count results.
    /// We add memory limits and execution time limits to prevent resource exhaustion.
    async fn execute_sql(&self, sql: &str) -> Result<u64> {
        let count_sql = format!(
            "SELECT count() FROM ({}) SETTINGS max_memory_usage = {}, max_execution_time = 30",
            sql, MAX_MEMORY_USAGE
        );

        let count: u64 = self.client.query(&count_sql).fetch_one().await?;
        Ok(count)
    }

    /// Get sampler cache statistics.
    pub fn cache_stats(&self) -> std::collections::HashMap<String, usize> {
        self.sampler.cache_stats()
    }
}

/// Substitute ClickHouse parameter placeholders with actual values.
fn substitute_params_in_sql(
    sql: &str,
    params: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let mut result = sql.to_string();

    for (name, value) in params {
        // Match patterns like {p0:String}, {p1:Int64}, etc.
        let patterns = [
            format!("{{{name}:String}}"),
            format!("{{{name}:Int64}}"),
            format!("{{{name}:Float64}}"),
            format!("{{{name}:Bool}}"),
        ];

        let replacement = match value {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            _ => value.to_string(),
        };

        for pattern in &patterns {
            result = result.replace(pattern, &replacement);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_params() {
        let sql = "SELECT * FROM users WHERE name = {p0:String} AND id = {p1:Int64}";
        let mut params = std::collections::HashMap::new();
        params.insert("p0".to_string(), serde_json::json!("alice"));
        params.insert("p1".to_string(), serde_json::json!(42));

        let result = substitute_params_in_sql(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE name = 'alice' AND id = 42"
        );
    }

    #[test]
    fn test_execution_result_success() {
        let result = ExecutionResult::success(
            "test".to_string(),
            10,
            Duration::from_millis(100),
            "SELECT 1".to_string(),
            serde_json::json!({}),
        );
        assert!(result.success);
        assert_eq!(result.row_count, Some(10));
    }

    #[test]
    fn test_execution_result_failure() {
        let result =
            ExecutionResult::failure("test".to_string(), "error".to_string(), Duration::from_millis(50));
        assert!(!result.success);
        assert_eq!(result.error, Some("error".to_string()));
    }
}
