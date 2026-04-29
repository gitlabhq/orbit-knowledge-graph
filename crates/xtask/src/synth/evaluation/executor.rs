//! Query execution and statistics collection.

use super::error::{ErrorCategory, ParsedError};
use super::metadata::{ErrorInfo, QueryMetadata, QueryMetadataBuilder, QueryPlan, SampleData};
use super::{ParameterSampler, QueryEntry};
use anyhow::Result;
use clickhouse_client::ArrowClickHouseClient;
use futures::stream::{self, StreamExt};
use ontology::Ontology;
use query_engine::compiler::{ParamValue, SecurityContext, compile};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Base ClickHouse query settings to prevent server crashes.
///
/// These are the safe defaults; user-provided settings from the YAML config
/// are merged on top (overriding any key that appears in both).
const BASE_QUERY_SETTINGS: &[(&str, &str)] = &[
    ("max_memory_usage", "1000000000"),
    ("max_execution_time", "30"),
    ("max_bytes_before_external_group_by", "100000000"),
    ("max_bytes_before_external_sort", "100000000"),
    ("join_algorithm", "'partial_merge'"),
];

/// Maximum bytes to collect for metadata sample data.
/// Prevents memory exhaustion when queries return rows with large column values.
const METADATA_SAMPLE_MAX_BYTES: usize = 1024 * 1024; // 1 MB

/// Sample row from query results for peeking.
pub type SampleRow = Vec<String>;

/// Information about how query parameters were sampled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingInfo {
    /// The traversal path used for scoped sampling, if any.
    pub traversal_path: Option<String>,
    /// Number of entities that had IDs sampled within the path.
    pub path_scoped_count: usize,
    /// Number of entities that fell back to global sampling.
    pub global_fallback_count: usize,
}

impl SamplingInfo {
    pub fn empty() -> Self {
        Self {
            traversal_path: None,
            path_scoped_count: 0,
            global_fallback_count: 0,
        }
    }

    /// Returns a short description of sampling behavior.
    pub fn description(&self) -> String {
        match (
            &self.traversal_path,
            self.path_scoped_count,
            self.global_fallback_count,
        ) {
            (Some(path), scoped, 0) if scoped > 0 => {
                format!("path-scoped ({} entities in '{}')", scoped, path)
            }
            (Some(path), scoped, global) if scoped > 0 && global > 0 => {
                format!(
                    "mixed ({} path-scoped in '{}', {} global)",
                    scoped, path, global
                )
            }
            (_, 0, global) if global > 0 => {
                format!("global ({} entities)", global)
            }
            _ => "no sampling needed".to_string(),
        }
    }
}

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
    /// Information about how parameters were sampled.
    #[serde(default)]
    pub sampling_info: Option<SamplingInfo>,
}

#[allow(clippy::too_many_arguments)]
impl ExecutionResult {
    pub fn success(
        query_name: String,
        row_count: u64,
        sample_rows: Vec<SampleRow>,
        column_names: Vec<String>,
        execution_time: Duration,
        sql: String,
        params: serde_json::Value,
        sampling_info: Option<SamplingInfo>,
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
            sampling_info,
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
            sampling_info: None,
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
    client: ArrowClickHouseClient,
    ontology: Ontology,
    sampler: ParameterSampler,
    /// Cached security contexts: (org_id, traversal_path) pairs
    security_contexts: Vec<(i64, String)>,
    /// Merged SETTINGS clause (base defaults + user overrides).
    query_settings: String,
}

impl QueryExecutor {
    pub fn new(
        client: ArrowClickHouseClient,
        ontology: Ontology,
        sample_size: usize,
        user_settings: &std::collections::HashMap<String, String>,
    ) -> Self {
        let sampler = ParameterSampler::new(client.clone(), sample_size);
        let query_settings = Self::build_settings(user_settings);
        Self {
            client,
            ontology,
            sampler,
            security_contexts: Vec::new(),
            query_settings,
        }
    }

    /// Merge base safe defaults with user-provided overrides into a SETTINGS clause.
    ///
    /// Values are classified as numeric (emitted bare), boolean (emitted bare),
    /// already-quoted (emitted as-is), or string (wrapped in single quotes).
    /// Output is sorted by key for deterministic ordering across runs.
    fn build_settings(user: &std::collections::HashMap<String, String>) -> String {
        let mut merged: std::collections::BTreeMap<&str, String> = BASE_QUERY_SETTINGS
            .iter()
            .map(|(k, v)| (*k, v.to_string()))
            .collect();
        for (k, v) in user {
            let formatted = if v.starts_with('\'')
                || v.parse::<u64>().is_ok()
                || matches!(v.as_str(), "true" | "false" | "0" | "1")
            {
                v.clone()
            } else {
                format!("'{v}'")
            };
            merged.insert(k.as_str(), formatted);
        }
        let pairs: Vec<String> = merged
            .into_iter()
            .map(|(k, v)| format!("{k} = {v}"))
            .collect();
        format!("SETTINGS {}", pairs.join(", "))
    }

    /// Warm the parameter cache and sample security contexts.
    ///
    /// `namespace_entity` specifies which entity type defines the namespace
    /// hierarchy (typically "Group") — used to sample traversal paths.
    pub async fn warm_cache(&mut self, namespace_entity: &str) -> Result<()> {
        self.sampler.warm_cache(&self.ontology).await?;
        self.security_contexts = self
            .sampler
            .sample_traversal_paths(namespace_entity, &self.ontology)
            .await?;
        Ok(())
    }

    /// Get a random security context for query execution.
    fn random_security_context(&self) -> Result<SecurityContext> {
        if self.security_contexts.is_empty() {
            anyhow::bail!("No security contexts available - call warm_cache first");
        }

        let mut rng = rand::rng();
        let idx = rng.random_range(0..self.security_contexts.len());
        let (org_id, path) = &self.security_contexts[idx];

        SecurityContext::new(*org_id, vec![path.clone()])
            .map_err(|e| anyhow::anyhow!("Invalid security context: {}", e))
    }

    /// Substitute placeholder node_ids with sampled values within the security context.
    ///
    /// Returns the substituted query and sampling info for diagnostics.
    async fn substitute_parameters(
        &self,
        mut query_value: serde_json::Value,
        security_ctx: &SecurityContext,
    ) -> Result<(serde_json::Value, SamplingInfo)> {
        // Get the first traversal path from the security context
        let traversal_path: Option<String> = security_ctx
            .traversal_paths
            .first()
            .map(|tp| tp.path.clone());

        let mut sampling_info = SamplingInfo {
            traversal_path: traversal_path.clone(),
            path_scoped_count: 0,
            global_fallback_count: 0,
        };

        // Track sampled IDs to ensure path-finding queries get distinct start/end
        let mut sampled_by_entity: std::collections::HashMap<String, Vec<i64>> =
            std::collections::HashMap::new();

        // Process `nodes` array (for traversal/aggregation/path_finding queries)
        if let Some(nodes) = query_value.get_mut("nodes").and_then(|n| n.as_array_mut()) {
            for node in nodes.iter_mut() {
                if let Some(obj) = node.as_object_mut() {
                    self.substitute_node_ids(
                        obj,
                        &traversal_path,
                        security_ctx,
                        &mut sampling_info,
                        &mut sampled_by_entity,
                    )
                    .await?;
                }
            }
        }

        // Process singular `node` field (for search queries)
        if let Some(node) = query_value.get_mut("node").and_then(|n| n.as_object_mut()) {
            self.substitute_node_ids(
                node,
                &traversal_path,
                security_ctx,
                &mut sampling_info,
                &mut sampled_by_entity,
            )
            .await?;
        }

        Ok((query_value, sampling_info))
    }

    /// Substitute node_ids in a single node object.
    ///
    /// Supports placeholder syntax:
    /// - `"$sample"` - sample 1 ID
    /// - `"$sample:N"` - sample N IDs (e.g., `"$sample:3"` for 3 IDs)
    async fn substitute_node_ids(
        &self,
        obj: &mut serde_json::Map<String, serde_json::Value>,
        traversal_path: &Option<String>,
        security_ctx: &SecurityContext,
        sampling_info: &mut SamplingInfo,
        sampled_by_entity: &mut std::collections::HashMap<String, Vec<i64>>,
    ) -> Result<()> {
        // Check if this node has node_ids that need substitution
        if !obj.contains_key("node_ids") {
            return Ok(());
        }

        let entity = match obj.get("entity").and_then(|e| e.as_str()) {
            Some(e) => e.to_string(),
            None => return Ok(()),
        };

        // Determine how many IDs to sample based on placeholder syntax
        let count = parse_sample_count(obj.get("node_ids"));

        // Sample IDs within the security context path if available
        let mut sampled_ids = if let Some(path) = traversal_path {
            let ids = self
                .sampler
                .random_ids_in_path(&entity, count, path, &self.ontology)
                .await?;
            if ids.is_empty() {
                // No entities in path, fall back to org-scoped sampling
                sampling_info.global_fallback_count += 1;
                self.sampler
                    .random_ids_in_org(&entity, count, security_ctx.org_id, &self.ontology)
                    .await?
            } else {
                sampling_info.path_scoped_count += 1;
                ids
            }
        } else {
            // No path available, use org-scoped sampling
            sampling_info.global_fallback_count += 1;
            self.sampler
                .random_ids_in_org(&entity, count, security_ctx.org_id, &self.ontology)
                .await?
        };

        // For path-finding queries, ensure we don't reuse IDs for the same entity type.
        // This prevents start/end being the same node.
        if let Some(already_sampled) = sampled_by_entity.get(&entity) {
            sampled_ids.retain(|id| !already_sampled.contains(id));
            // If we filtered out all IDs, try to sample more
            if sampled_ids.is_empty() {
                let extra_ids = if let Some(path) = traversal_path {
                    self.sampler
                        .random_ids_in_path(&entity, count * 2, path, &self.ontology)
                        .await?
                } else {
                    self.sampler
                        .random_ids_in_org(&entity, count * 2, security_ctx.org_id, &self.ontology)
                        .await?
                };
                sampled_ids = extra_ids
                    .into_iter()
                    .filter(|id| !already_sampled.contains(id))
                    .take(count)
                    .collect();
            }
        }

        if !sampled_ids.is_empty() {
            // Track these IDs as used for this entity type
            sampled_by_entity
                .entry(entity)
                .or_default()
                .extend(&sampled_ids);

            obj.insert("node_ids".to_string(), serde_json::to_value(&sampled_ids)?);
        }

        Ok(())
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
        let settings = &self.query_settings;

        // Get row count
        let count_sql = format!("SELECT count() FROM ({}) {}", sql, settings);
        let count: u64 = self.client.inner().query(&count_sql).fetch_one().await?;

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
            .inner()
            .query(&json_sql)
            .fetch_one()
            .await
            .unwrap_or_default();

        if raw.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Parse the JSONCompactColumns format: [[col1_values...], [col2_values...], ...]
        let columns = self.get_column_names(sql).await.unwrap_or_default();

        // Fetch data as tab-separated values which is easier to parse
        let tsv_sql = format!("{} FORMAT TabSeparated", sql);
        let tsv_raw: Vec<u8> = self
            .client
            .inner()
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
            .inner()
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

    /// Execute a single query with sampled parameters, returning metadata.
    pub async fn execute_query(
        &self,
        key: &str,
        entry: &QueryEntry,
    ) -> (ExecutionResult, QueryMetadata) {
        let start = Instant::now();
        let display_name = format!("{} ({})", key, entry.desc);

        let original_query = match entry.parse_query() {
            Ok(v) => v,
            Err(e) => {
                let error_msg = format!("Invalid query JSON: {}", e);
                let result = ExecutionResult::failure(
                    display_name.clone(),
                    error_msg.clone(),
                    start.elapsed(),
                );
                let metadata = QueryMetadataBuilder::new(&display_name)
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: "PARSE_ERROR".to_string(),
                        code: None,
                    })
                    .build();
                return (result, metadata);
            }
        };

        let mut builder =
            QueryMetadataBuilder::new(&display_name).original_query(original_query.clone());

        let security_ctx = match self.random_security_context() {
            Ok(ctx) => ctx,
            Err(e) => {
                let error_msg = format!("Security context error: {}", e);
                let result = ExecutionResult::failure(
                    display_name.clone(),
                    error_msg.clone(),
                    start.elapsed(),
                );
                let metadata = builder
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: "SECURITY_CONTEXT".to_string(),
                        code: None,
                    })
                    .build();
                return (result, metadata);
            }
        };

        let (substituted, sampling_info) = match self
            .substitute_parameters(original_query, &security_ctx)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let error_msg = format!("Parameter substitution failed: {}", e);
                let result = ExecutionResult::failure(
                    display_name.clone(),
                    error_msg.clone(),
                    start.elapsed(),
                );
                let metadata = builder
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: "PARAMETER_ERROR".to_string(),
                        code: None,
                    })
                    .build();
                return (result, metadata);
            }
        };

        builder = builder.substituted_query(substituted.clone());

        let json_str = match serde_json::to_string(&substituted) {
            Ok(s) => s,
            Err(e) => {
                let error_msg = format!("JSON serialization failed: {}", e);
                let result = ExecutionResult::failure(
                    display_name.clone(),
                    error_msg.clone(),
                    start.elapsed(),
                );
                let metadata = builder
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: "SERIALIZATION_ERROR".to_string(),
                        code: None,
                    })
                    .build();
                return (result, metadata);
            }
        };

        let compiled = match compile(&json_str, &self.ontology, &security_ctx) {
            Ok(c) => c,
            Err(e) => {
                let error_msg = format!("Query compilation failed: {}", e);
                let result = ExecutionResult::failure(
                    display_name.clone(),
                    error_msg.clone(),
                    start.elapsed(),
                );
                let metadata = builder
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: "COMPILATION_ERROR".to_string(),
                        code: None,
                    })
                    .build();
                return (result, metadata);
            }
        };

        let final_sql = substitute_params_in_sql(&compiled.base.sql, &compiled.base.params);

        builder = builder
            .sql(compiled.base.sql.clone())
            .final_sql(final_sql.clone())
            .params(
                compiled
                    .base
                    .params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.value.clone()))
                    .collect(),
            );

        if let Ok(plan) = self.get_query_plan(&final_sql).await {
            builder = builder.query_plan(plan);
        }

        match self.execute_sql_with_sample(&final_sql).await {
            Ok((row_count, sample_rows, column_names)) => {
                let sample_data = self.fetch_sample_for_metadata(&final_sql).await;

                let result = ExecutionResult::success(
                    display_name,
                    row_count,
                    sample_rows,
                    column_names,
                    start.elapsed(),
                    compiled.base.sql,
                    params_to_json(&compiled.base.params),
                    Some(sampling_info),
                );

                let mut metadata_builder =
                    builder.execution_time(start.elapsed()).success(row_count);
                if let Some(data) = sample_data {
                    metadata_builder = metadata_builder.sample_data(data);
                }
                let metadata = metadata_builder.build();

                (result, metadata)
            }
            Err(e) => {
                let error_msg = format!("Execution failed: {}", e);
                let parsed = ParsedError::parse(&error_msg);
                let result = ExecutionResult::failure_with_sql(
                    display_name,
                    error_msg.clone(),
                    start.elapsed(),
                    Some(final_sql),
                );
                let metadata = builder
                    .execution_time(start.elapsed())
                    .failure(ErrorInfo {
                        message: error_msg,
                        category: parsed.category.to_string(),
                        code: parsed.code,
                    })
                    .build();
                (result, metadata)
            }
        }
    }

    /// Execute all queries serially, returning results with metadata.
    ///
    /// Includes a small delay after memory/network errors to let ClickHouse recover.
    pub async fn execute_all(
        &self,
        queries: &std::collections::HashMap<String, QueryEntry>,
    ) -> Vec<(ExecutionResult, QueryMetadata)> {
        let mut results = Vec::with_capacity(queries.len());

        for (key, entry) in queries {
            let (result, metadata) = self.execute_query(key, entry).await;

            if !result.success
                && let Some(ref err) = result.error
                && (err.contains("MEMORY_LIMIT") || err.contains("network error"))
            {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }

            results.push((result, metadata));
        }

        results
    }

    /// Execute all queries concurrently with a bounded concurrency limit.
    ///
    /// Unlike `execute_all`, this does NOT back off on memory errors — the
    /// purpose is load testing under realistic conditions where multiple
    /// queries hit ClickHouse simultaneously.
    pub async fn execute_all_concurrent(
        &self,
        queries: &std::collections::HashMap<String, QueryEntry>,
        concurrency: usize,
    ) -> Vec<(ExecutionResult, QueryMetadata)> {
        let query_list: Vec<_> = queries.iter().collect();

        stream::iter(query_list)
            .map(|(key, entry)| self.execute_query(key, entry))
            .buffer_unordered(concurrency)
            .collect()
            .await
    }

    /// Get query plan using EXPLAIN.
    async fn get_query_plan(&self, sql: &str) -> Result<QueryPlan> {
        let explain_text = self.fetch_raw_text(&format!("EXPLAIN {}", sql)).await;
        let pipeline = self
            .fetch_raw_text(&format!("EXPLAIN PIPELINE {}", sql))
            .await;

        Ok(QueryPlan {
            explain_text,
            pipeline: if pipeline.is_empty() {
                None
            } else {
                Some(pipeline)
            },
            estimated_rows: None,
        })
    }

    /// Fetch raw text output from a query using fetch_bytes.
    async fn fetch_raw_text(&self, sql: &str) -> String {
        let result = self.client.inner().query(sql).fetch_bytes("TabSeparated");

        let mut cursor = match result {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!("fetch_bytes failed: {}", e);
                return String::new();
            }
        };

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => buffer.extend(chunk),
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!("fetch_bytes cursor error: {}", e);
                    return String::new();
                }
            }
        }

        String::from_utf8_lossy(&buffer).to_string()
    }

    /// Fetch sample rows for metadata.
    ///
    /// Limits memory usage by:
    /// - Restricting to 5 rows
    /// - Capping response size to 1MB to handle queries with large column values
    async fn fetch_sample_for_metadata(&self, sql: &str) -> Option<SampleData> {
        let settings = &self.query_settings;
        let sample_sql = format!(
            "SELECT * FROM ({}) AS _sample LIMIT 5 {} FORMAT JSONEachRow",
            sql, settings
        );

        let raw = self
            .fetch_raw_text_limited(&sample_sql, METADATA_SAMPLE_MAX_BYTES)
            .await;

        if raw.is_empty() {
            return None;
        }

        let mut columns: Vec<String> = vec![];
        let mut rows: Vec<Vec<serde_json::Value>> = vec![];

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(obj) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(line)
            {
                if columns.is_empty() {
                    columns = obj.keys().cloned().collect();
                }
                let row: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|k| obj.get(k).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                rows.push(row);
            }
        }

        if rows.is_empty() {
            None
        } else {
            Some(SampleData { columns, rows })
        }
    }

    /// Fetch raw text output with a byte limit to prevent memory exhaustion.
    async fn fetch_raw_text_limited(&self, sql: &str, max_bytes: usize) -> String {
        let result = self.client.inner().query(sql).fetch_bytes("TabSeparated");

        let mut cursor = match result {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!("fetch_bytes failed: {}", e);
                return String::new();
            }
        };

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => {
                    buffer.extend(&chunk);
                    if buffer.len() > max_bytes {
                        tracing::debug!(
                            "Truncating metadata sample at {} bytes (limit: {})",
                            buffer.len(),
                            max_bytes
                        );
                        buffer.truncate(max_bytes);
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!("fetch_bytes cursor error: {}", e);
                    return String::new();
                }
            }
        }

        String::from_utf8_lossy(&buffer).to_string()
    }
}

/// Substitute ClickHouse parameter placeholders with actual values.
fn substitute_params_in_sql(
    sql: &str,
    params: &std::collections::HashMap<String, ParamValue>,
) -> String {
    let mut result = sql.to_string();

    for (name, param) in params {
        let pattern = format!("{{{name}:{}}}", param.ch_type);

        let replacement = format_param_value(&param.value);

        result = result.replace(&pattern, &replacement);
    }

    result
}

/// Format a JSON value as a ClickHouse SQL literal.
fn format_param_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(format_param_value).collect();
            format!("[{}]", elements.join(", "))
        }
        other => panic!("unsupported param value type: {other}"),
    }
}

/// Convert `ParamValue` map to a JSON object with just the values (for serialization).
fn params_to_json(params: &std::collections::HashMap<String, ParamValue>) -> serde_json::Value {
    serde_json::Value::Object(
        params
            .iter()
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect(),
    )
}

/// Parse sample count from node_ids placeholder value.
///
/// Supports:
/// - `"$sample"` -> 1
/// - `"$sample:N"` -> N (e.g., `"$sample:3"` -> 3)
fn parse_sample_count(value: Option<&serde_json::Value>) -> usize {
    let Some(v) = value else { return 1 };

    let Some(s) = v.as_str() else { return 1 };

    if s == "$sample" {
        return 1;
    }
    if let Some(count_str) = s.strip_prefix("$sample:") {
        return count_str.parse().unwrap_or(1);
    }

    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sample_count() {
        assert_eq!(parse_sample_count(Some(&serde_json::json!("$sample"))), 1);
        assert_eq!(parse_sample_count(Some(&serde_json::json!("$sample:1"))), 1);
        assert_eq!(parse_sample_count(Some(&serde_json::json!("$sample:3"))), 3);
        assert_eq!(
            parse_sample_count(Some(&serde_json::json!("$sample:10"))),
            10
        );

        // Edge cases
        assert_eq!(parse_sample_count(None), 1);
        assert_eq!(parse_sample_count(Some(&serde_json::json!(42))), 1);
    }

    #[test]
    fn test_substitute_params() {
        use gkg_utils::clickhouse::ChType;

        let sql = "SELECT * FROM users WHERE name = {p0:String} AND id = {p1:Int64}";
        let mut params = std::collections::HashMap::new();
        params.insert(
            "p0".to_string(),
            ParamValue {
                ch_type: ChType::String,
                value: serde_json::json!("alice"),
            },
        );
        params.insert(
            "p1".to_string(),
            ParamValue {
                ch_type: ChType::Int64,
                value: serde_json::json!(42),
            },
        );

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
            None,
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
