//! Post-execution profiling queries against ClickHouse system tables.
//!
//! Retrieves EXPLAIN plans, query_log entries, processor profiles, and
//! instance health.

use std::collections::HashMap;

use crate::arrow_client::ArrowClickHouseClient;
use crate::error::ClickHouseError;
use crate::stats::{DiskInfo, InstanceHealth, ProcessorProfile, QueryLogEntry, TablePartsInfo};

impl ArrowClickHouseClient {
    pub(crate) async fn fetch_text(&self, sql: &str) -> Result<String, ClickHouseError> {
        self.fetch_as_string(sql, "TabSeparatedRaw").await
    }

    async fn fetch_json_text(&self, sql: &str) -> Result<String, ClickHouseError> {
        self.fetch_as_string(sql, "JSONEachRow").await
    }

    async fn fetch_as_string(&self, sql: &str, format: &str) -> Result<String, ClickHouseError> {
        let mut cursor = self
            .query(sql)
            .inner
            .fetch_bytes(format)
            .map_err(ClickHouseError::Query)?;

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => buffer.extend(chunk),
                Ok(None) => break,
                Err(e) => return Err(ClickHouseError::Query(e)),
            }
        }

        String::from_utf8(buffer).map_err(|e| ClickHouseError::BadResponse {
            status: 0,
            body: format!("non-UTF8 response: {e}"),
        })
    }

    pub async fn explain_plan(&self, sql: &str) -> Result<String, ClickHouseError> {
        self.fetch_text(&format!(
            "EXPLAIN PLAN indexes=1, actions=1, sorting=1 {sql}"
        ))
        .await
    }

    pub async fn explain_pipeline(&self, sql: &str) -> Result<String, ClickHouseError> {
        self.fetch_text(&format!("EXPLAIN PIPELINE {sql}")).await
    }

    /// Fetch a query_log entry by matching against `log_comment`.
    ///
    /// Flushes the query log first to ensure the entry is available.
    /// The `log_comment_match` should be a unique substring (e.g. a
    /// profiling UUID) that identifies the target query.
    pub async fn fetch_query_log(
        &self,
        log_comment_match: &str,
    ) -> Result<Option<QueryLogEntry>, ClickHouseError> {
        validate_log_comment_match(log_comment_match)?;
        self.execute("SYSTEM FLUSH LOGS").await?;

        let where_clause =
            format!("log_comment LIKE '%{log_comment_match}%' AND type = 'QueryFinish'");
        self.fetch_query_log_where(&where_clause).await
    }

    /// Fetch a query_log entry by `query_id`.
    pub async fn fetch_query_log_by_id(
        &self,
        query_id: &str,
    ) -> Result<Option<QueryLogEntry>, ClickHouseError> {
        validate_query_id(query_id)?;
        self.execute("SYSTEM FLUSH LOGS").await?;

        let where_clause = format!("query_id = '{query_id}' AND type = 'QueryFinish'");
        self.fetch_query_log_where(&where_clause).await
    }

    async fn fetch_query_log_where(
        &self,
        where_clause: &str,
    ) -> Result<Option<QueryLogEntry>, ClickHouseError> {
        let sql = format!(
            "SELECT \
                query_id, query_duration_ms, memory_usage, \
                ProfileEvents['RealTimeMicroseconds'] AS real_time_us, \
                ProfileEvents['UserTimeMicroseconds'] AS user_time_us, \
                ProfileEvents['SystemTimeMicroseconds'] AS system_time_us, \
                ProfileEvents['OSCPUWaitMicroseconds'] AS os_cpu_wait_us, \
                ProfileEvents['OSIOWaitMicroseconds'] AS os_io_wait_us, \
                ProfileEvents['SelectedParts'] AS selected_parts, \
                ProfileEvents['SelectedMarks'] AS selected_marks, \
                ProfileEvents['SelectedRows'] AS selected_rows, \
                ProfileEvents['MarkCacheHits'] AS mark_cache_hits, \
                ProfileEvents['MarkCacheMisses'] AS mark_cache_misses, \
                ProfileEvents['ExternalSortCompressedBytes'] AS external_sort_bytes, \
                ProfileEvents['ExternalAggregationCompressedBytes'] AS external_agg_bytes, \
                ProfileEvents['ExternalJoinCompressedBytes'] AS external_join_bytes, \
                read_rows, read_bytes, result_rows, result_bytes, \
                ProfileEvents.Names, ProfileEvents.Values \
            FROM system.query_log \
            WHERE {where_clause} \
            LIMIT 1"
        );

        let text = self.fetch_json_text(&sql).await?;
        if text.trim().is_empty() {
            return Ok(None);
        }

        let v: serde_json::Value =
            serde_json::from_str(text.trim()).map_err(|e| ClickHouseError::BadResponse {
                status: 0,
                body: format!("failed to parse query_log JSON: {e}"),
            })?;

        Ok(Some(parse_query_log_entry(&v)))
    }

    pub async fn fetch_processors_profile(
        &self,
        query_id: &str,
    ) -> Result<Vec<ProcessorProfile>, ClickHouseError> {
        validate_query_id(query_id)?;

        let sql = format!(
            "SELECT name, elapsed_us, input_wait_elapsed_us, output_wait_elapsed_us, \
                    input_rows, output_rows \
             FROM system.processors_profile_log \
             WHERE query_id = '{query_id}' \
             ORDER BY elapsed_us DESC"
        );

        let text = self.fetch_json_text(&sql).await?;
        if text.trim().is_empty() {
            return Ok(vec![]);
        }

        let mut profiles = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value =
                serde_json::from_str(line).map_err(|e| ClickHouseError::BadResponse {
                    status: 0,
                    body: format!("failed to parse processor profile JSON: {e}"),
                })?;

            profiles.push(ProcessorProfile {
                name: v["name"].as_str().unwrap_or("").to_string(),
                elapsed_us: v["elapsed_us"].as_u64().unwrap_or(0),
                input_wait_us: v["input_wait_elapsed_us"].as_u64().unwrap_or(0),
                output_wait_us: v["output_wait_elapsed_us"].as_u64().unwrap_or(0),
                input_rows: v["input_rows"].as_u64().unwrap_or(0),
                output_rows: v["output_rows"].as_u64().unwrap_or(0),
            });
        }

        Ok(profiles)
    }

    pub async fn fetch_instance_health(&self) -> Result<InstanceHealth, ClickHouseError> {
        let metrics_sql = "\
            SELECT metric, value FROM system.metrics \
            WHERE metric IN (\
                'Query', 'MemoryTracking', 'MarkCacheBytes', 'MarkCacheFiles', \
                'QueryCacheEntries', 'QueryCacheBytes', \
                'BackgroundMergesAndMutationsPoolTask', \
                'TemporaryFilesForSort', 'TemporaryFilesForAggregation', \
                'TemporaryFilesForJoin'\
            )";

        let uptime_sql = "\
            SELECT value FROM system.asynchronous_metrics \
            WHERE metric = 'Uptime'";

        let disks_sql = "\
            SELECT name, path, total_space AS total_bytes, \
                   free_space AS free_bytes, \
                   (total_space - free_space) AS used_bytes \
            FROM system.disks";

        let parts_sql = "\
            SELECT database, table, \
                   count() AS part_count, \
                   sum(rows) AS total_rows, \
                   sum(bytes_on_disk) AS bytes_on_disk, \
                   uniq(partition_id) AS partition_count, \
                   0 AS active_mutations, \
                   0 AS detached_parts, \
                   toString(max(modification_time)) AS last_modification_time \
            FROM system.parts \
            WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') \
            GROUP BY database, table \
            ORDER BY bytes_on_disk DESC";

        let (metrics_text, uptime_text, disks_text, parts_text) = tokio::try_join!(
            self.fetch_json_text(metrics_sql),
            self.fetch_json_text(uptime_sql),
            self.fetch_json_text(disks_sql),
            self.fetch_json_text(parts_sql),
        )?;

        let mut health = InstanceHealth::default();

        for line in metrics_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                let metric = v["metric"].as_str().unwrap_or("");
                let value = v["value"].as_u64().unwrap_or(0);
                match metric {
                    "Query" => health.current_queries = value,
                    "MemoryTracking" => health.memory_tracking_bytes = value,
                    "MarkCacheBytes" => health.mark_cache_bytes = value,
                    "MarkCacheFiles" => health.mark_cache_files = value,
                    "QueryCacheEntries" => health.query_cache_entries = value,
                    "QueryCacheBytes" => health.query_cache_bytes = value,
                    "BackgroundMergesAndMutationsPoolTask" => health.active_merges = value,
                    "TemporaryFilesForSort" => health.temp_files_sort = value,
                    "TemporaryFilesForAggregation" => health.temp_files_agg = value,
                    "TemporaryFilesForJoin" => health.temp_files_join = value,
                    _ => {}
                }
            }
        }

        for line in uptime_text.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) {
                health.uptime_seconds = v["value"].as_f64().unwrap_or(0.0) as u64;
            }
        }

        for line in disks_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                health.disk_usage.push(DiskInfo {
                    name: v["name"].as_str().unwrap_or("").to_string(),
                    path: v["path"].as_str().unwrap_or("").to_string(),
                    total_bytes: v["total_bytes"].as_u64().unwrap_or(0),
                    free_bytes: v["free_bytes"].as_u64().unwrap_or(0),
                    used_bytes: v["used_bytes"].as_u64().unwrap_or(0),
                });
            }
        }

        for line in parts_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                health.table_parts.push(TablePartsInfo {
                    database: v["database"].as_str().unwrap_or("").to_string(),
                    table: v["table"].as_str().unwrap_or("").to_string(),
                    part_count: v["part_count"].as_u64().unwrap_or(0),
                    total_rows: v["total_rows"].as_u64().unwrap_or(0),
                    bytes_on_disk: v["bytes_on_disk"].as_u64().unwrap_or(0),
                    partition_count: v["partition_count"].as_u64().unwrap_or(0),
                    active_mutations: v["active_mutations"].as_u64().unwrap_or(0),
                    detached_parts: v["detached_parts"].as_u64().unwrap_or(0),
                    last_modification_time: v["last_modification_time"].as_str().map(String::from),
                });
            }
        }

        Ok(health)
    }
}

fn validate_log_comment_match(s: &str) -> Result<(), ClickHouseError> {
    if s.is_empty()
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ClickHouseError::BadResponse {
            status: 0,
            body: format!("invalid log_comment match string: {s}"),
        });
    }
    Ok(())
}

fn validate_query_id(query_id: &str) -> Result<(), ClickHouseError> {
    if query_id.is_empty()
        || !query_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(ClickHouseError::BadResponse {
            status: 0,
            body: format!("invalid query_id format: {query_id}"),
        });
    }
    Ok(())
}

fn parse_query_log_entry(v: &serde_json::Value) -> QueryLogEntry {
    QueryLogEntry {
        query_id: v["query_id"].as_str().unwrap_or("").to_string(),
        query_duration_ms: v["query_duration_ms"].as_f64().unwrap_or(0.0),
        memory_usage: v["memory_usage"].as_u64().unwrap_or(0),
        peak_memory_usage: 0,
        read_rows: v["read_rows"].as_u64().unwrap_or(0),
        read_bytes: v["read_bytes"].as_u64().unwrap_or(0),
        result_rows: v["result_rows"].as_u64().unwrap_or(0),
        result_bytes: v["result_bytes"].as_u64().unwrap_or(0),
        real_time_us: v["real_time_us"].as_u64().unwrap_or(0),
        user_time_us: v["user_time_us"].as_u64().unwrap_or(0),
        system_time_us: v["system_time_us"].as_u64().unwrap_or(0),
        os_cpu_wait_us: v["os_cpu_wait_us"].as_u64().unwrap_or(0),
        os_io_wait_us: v["os_io_wait_us"].as_u64().unwrap_or(0),
        selected_parts: v["selected_parts"].as_u64().unwrap_or(0),
        selected_marks: v["selected_marks"].as_u64().unwrap_or(0),
        selected_rows: v["selected_rows"].as_u64().unwrap_or(0),
        mark_cache_hits: v["mark_cache_hits"].as_u64().unwrap_or(0),
        mark_cache_misses: v["mark_cache_misses"].as_u64().unwrap_or(0),
        external_sort_bytes: v["external_sort_bytes"].as_u64().unwrap_or(0),
        external_agg_bytes: v["external_agg_bytes"].as_u64().unwrap_or(0),
        external_join_bytes: v["external_join_bytes"].as_u64().unwrap_or(0),
        profile_events: build_profile_events(v),
    }
}

fn build_profile_events(v: &serde_json::Value) -> HashMap<String, u64> {
    let mut map = HashMap::new();

    let names = v["ProfileEvents.Names"].as_array();
    let values = v["ProfileEvents.Values"].as_array();

    if let (Some(names), Some(values)) = (names, values) {
        for (name, value) in names.iter().zip(values.iter()) {
            if let Some(n) = name.as_str() {
                let val = value.as_u64().unwrap_or(0);
                if val > 0 {
                    map.insert(n.to_string(), val);
                }
            }
        }
    }

    map
}
