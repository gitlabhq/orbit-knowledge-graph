//! Query execution and statistics collection.

use super::error::{ErrorCategory, ParsedError};
use super::{ParameterSampler, QueryDefinition};
use anyhow::Result;
use clickhouse::Client;
use ontology::Ontology;
use query_engine::compile;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// ClickHouse query settings to prevent server crashes.
///
/// - max_memory_usage: 200MB limit per query (fails instead of crashing)
/// - max_execution_time: 30 second timeout
/// - max_bytes_before_external_*: Spill to disk instead of using more RAM
/// - join_algorithm: Use disk-based partial_merge joins for large tables
const SAFE_QUERY_SETTINGS: &str = "\
    max_memory_usage = 200000000, \
    max_execution_time = 30, \
    max_bytes_before_external_group_by = 100000000, \
    max_bytes_before_external_sort = 100000000, \
    join_algorithm = 'partial_merge'";

/// Sample row from query results for peeking.
pub type SampleRow = Vec<String>;

/// Result of executing a single query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Name of the query.
    pub query_name: String,
    /// Whether the query executed successfully.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Parsed error with structured information.
    pub parsed_error: Option<ParsedError>,
    /// Number of rows returned.
    pub row_count: Option<u64>,
    /// Sample of first row(s) for peeking results.
    pub sample_rows: Option<Vec<SampleRow>>,
    /// Column names for sample rows.
    pub column_names: Option<Vec<String>>,
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
        sample_rows: Vec<SampleRow>,
        column_names: Vec<String>,
        execution_time: Duration,
        sql: String,
        params: serde_json::Value,
    ) -> Self {
        Self {
            query_name,
            success: true,
            error: None,
            parsed_error: None,
            row_count: Some(row_count),
            sample_rows: if sample_rows.is_empty() {
                None
            } else {
                Some(sample_rows)
            },
            column_names: if column_names.is_empty() {
                None
            } else {
                Some(column_names)
            },
            execution_time,
            sql: Some(sql),
            params: Some(params),
        }
    }

    pub fn failure(query_name: String, error: String, execution_time: Duration) -> Self {
        Self::failure_with_sql(query_name, error, execution_time, None)
    }

    pub fn failure_with_sql(
        query_name: String,
        error: String,
        execution_time: Duration,
        sql: Option<String>,
    ) -> Self {
        let parsed_error = ParsedError::parse(&error);
        Self {
            query_name,
            success: false,
            error: Some(error),
            parsed_error: Some(parsed_error),
            row_count: None,
            sample_rows: None,
            column_names: None,
            execution_time,
            sql,
            params: None,
        }
    }

    /// Get the error category if this is a failure.
    pub fn error_category(&self) -> Option<ErrorCategory> {
        self.parsed_error.as_ref().map(|e| e.category)
    }

    /// Check if this error is transient (can retry).
    pub fn is_transient_error(&self) -> bool {
        self.parsed_error.as_ref().is_some_and(|e| e.is_transient())
    }

    /// Check if this error needs a query fix.
    pub fn needs_query_fix(&self) -> bool {
        self.parsed_error
            .as_ref()
            .is_some_and(|e| e.needs_query_fix())
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
    pub async fn execute_query(&mut self, name: &str, query: &QueryDefinition) -> ExecutionResult {
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

        // Execute the query and get sample rows
        match self.execute_sql_with_sample(&final_sql).await {
            Ok((row_count, sample_rows, column_names)) => ExecutionResult::success(
                name.to_string(),
                row_count,
                sample_rows,
                column_names,
                start.elapsed(),
                compiled.sql,
                serde_json::to_value(&compiled.params).unwrap_or_default(),
            ),
            Err(e) => ExecutionResult::failure_with_sql(
                name.to_string(),
                format!("Execution failed: {}", e),
                start.elapsed(),
                Some(final_sql),
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
            if !result.success
                && let Some(ref err) = result.error
                && (err.contains("MEMORY_LIMIT") || err.contains("network error"))
            {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
                    if obj.contains_key("node_ids")
                        && let Some(entity) = obj.get("entity").and_then(|e| e.as_str())
                    {
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
                            obj.insert("node_ids".to_string(), serde_json::to_value(&sampled_ids)?);
                        }
                    }
                }
            }
        }

        Ok(query_value)
    }

    /// Execute raw SQL and return row count plus sample rows.
    ///
    /// For correctness testing, we verify the query runs, count results,
    /// and peek at the first few rows. We add memory limits and execution
    /// time limits to prevent resource exhaustion.
    ///
    /// Key safety settings:
    /// - max_memory_usage: Limits RAM per query (fails instead of crashing server)
    /// - max_bytes_before_external_*: Spills to disk instead of using RAM
    /// - join_algorithm: Uses disk-based joins for large tables
    async fn execute_sql_with_sample(
        &self,
        sql: &str,
    ) -> Result<(u64, Vec<SampleRow>, Vec<String>)> {
        let settings = format!("SETTINGS {}", SAFE_QUERY_SETTINGS);

        // Get row count
        let count_sql = format!("SELECT count() FROM ({}) {}", sql, settings);
        let count: u64 = self.client.query(&count_sql).fetch_one().await?;

        // Get sample rows (limit 3) with column names using JSONEachRow format
        let sample_sql = format!("SELECT * FROM ({}) LIMIT 3 {}", sql, settings);
        let (sample_rows, column_names) = self.fetch_sample_rows(&sample_sql).await?;

        Ok((count, sample_rows, column_names))
    }

    /// Fetch sample rows as strings for display.
    async fn fetch_sample_rows(&self, sql: &str) -> Result<(Vec<SampleRow>, Vec<String>)> {
        // Use FORMAT JSONCompactEachRow to get both column names and values
        let json_sql = format!("{} FORMAT JSONCompactColumns", sql);

        // Fetch as raw bytes and parse
        let raw: Vec<u8> = self
            .client
            .query(&json_sql)
            .fetch_one()
            .await
            .unwrap_or_default();

        if raw.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Parse the JSONCompactColumns format: [[col1_values...], [col2_values...], ...]
        // Actually, let's use a simpler approach - fetch column names separately
        let columns = self.get_column_names(sql).await.unwrap_or_default();

        // Fetch data as tab-separated values which is easier to parse
        let tsv_sql = format!("{} FORMAT TabSeparated", sql);
        let tsv_raw: Vec<u8> = self
            .client
            .query(&tsv_sql)
            .fetch_one()
            .await
            .unwrap_or_default();

        if tsv_raw.is_empty() {
            return Ok((vec![], columns));
        }

        let tsv_str = String::from_utf8_lossy(&tsv_raw);
        let rows: Vec<SampleRow> = tsv_str
            .lines()
            .take(3)
            .map(|line| line.split('\t').map(|s| s.to_string()).collect())
            .collect();

        Ok((rows, columns))
    }

    /// Get column names from a query.
    async fn get_column_names(&self, sql: &str) -> Result<Vec<String>> {
        // Use DESCRIBE to get column info
        let describe_sql = format!("DESCRIBE ({})", sql);
        let raw: Vec<u8> = self
            .client
            .query(&describe_sql)
            .fetch_one()
            .await
            .unwrap_or_default();

        if raw.is_empty() {
            return Ok(vec![]);
        }

        // Parse tab-separated output: name\ttype\t...
        let output = String::from_utf8_lossy(&raw);
        let columns: Vec<String> = output
            .lines()
            .filter_map(|line| line.split('\t').next())
            .map(|s| s.to_string())
            .collect();

        Ok(columns)
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
            vec![vec!["val1".to_string(), "val2".to_string()]],
            vec!["col1".to_string(), "col2".to_string()],
            Duration::from_millis(100),
            "SELECT 1".to_string(),
            serde_json::json!({}),
        );
        assert!(result.success);
        assert_eq!(result.row_count, Some(10));
        assert!(result.sample_rows.is_some());
    }

    #[test]
    fn test_execution_result_failure() {
        let result = ExecutionResult::failure(
            "test".to_string(),
            "error".to_string(),
            Duration::from_millis(50),
        );
        assert!(!result.success);
        assert_eq!(result.error, Some("error".to_string()));
    }
}
