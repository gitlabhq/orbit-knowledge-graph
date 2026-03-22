use std::collections::HashMap;
use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use uuid::Uuid;

use crate::error::ClickHouseError;
use crate::stats::{
    DiskInfo, InstanceHealth, ProcessorProfile, QueryLogEntry, QueryStats, TablePartsInfo,
};

const DEFAULT_QUERY_SETTINGS: &[(&str, &str)] = &[
    ("output_format_arrow_string_as_string", "1"),
    ("output_format_arrow_fixed_string_as_fixed_byte_array", "1"),
    ("join_algorithm", "direct,parallel_hash,hash"),
    ("query_plan_join_swap_table", "true"),
    ("use_query_condition_cache", "true"),
    ("join_use_nulls", "0"),
    ("use_skip_indexes_for_top_k", "1"),
];

pub struct QueryProfiler {
    http: once_cell::sync::OnceCell<reqwest::Client>,
    base_url: String,
    database: String,
    username: String,
    password: Option<String>,
    settings: HashMap<String, String>,
}

impl QueryProfiler {
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: Option<&str>,
        custom_settings: &HashMap<String, String>,
    ) -> Self {
        let mut settings: HashMap<String, String> = DEFAULT_QUERY_SETTINGS
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        for (k, v) in custom_settings {
            settings.insert(k.clone(), v.clone());
        }
        Self {
            http: once_cell::sync::OnceCell::new(),
            base_url: url.to_string(),
            database: database.to_string(),
            username: username.to_string(),
            password: password.map(String::from),
            settings,
        }
    }

    fn client(&self) -> &reqwest::Client {
        self.http.get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to build reqwest client")
        })
    }

    // Mirrors clickhouse-rs Query::do_execute request building:
    // - URL params: database, default_format, query settings, param_* bindings
    // - Auth: X-ClickHouse-User / X-ClickHouse-Key headers
    // - Body: SQL query text
    // Additionally sets wait_end_of_query=1 to capture X-ClickHouse-Summary
    // response headers (read_rows, read_bytes, memory_usage) which the
    // clickhouse-rs crate discards.
    pub async fn execute_with_stats(
        &self,
        sql: &str,
        query_params: &[(String, String)],
        extra_settings: &[(&str, &str)],
    ) -> Result<(Vec<RecordBatch>, QueryStats), ClickHouseError> {
        let query_id = Uuid::new_v4().to_string();

        let mut url_params: Vec<(String, String)> = vec![
            ("database".into(), self.database.clone()),
            ("default_format".into(), "ArrowStream".into()),
            ("wait_end_of_query".into(), "1".into()),
            ("query_id".into(), query_id.clone()),
        ];
        for (k, v) in &self.settings {
            url_params.push((k.clone(), v.clone()));
        }
        for (k, v) in query_params {
            url_params.push((format!("param_{k}"), v.clone()));
        }
        for (k, v) in extra_settings {
            url_params.push(((*k).into(), (*v).into()));
        }

        let url = reqwest::Url::parse_with_params(&self.base_url, &url_params).map_err(|e| {
            ClickHouseError::BadResponse {
                status: 0,
                body: format!("URL parse error: {e}"),
            }
        })?;

        let mut req = self.client().post(url).body(sql.to_string());
        req = req.header("X-ClickHouse-User", &self.username);
        if let Some(pw) = &self.password {
            req = req.header("X-ClickHouse-Key", pw);
        }

        let resp = req.send().await.map_err(ClickHouseError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            let body = truncate_body(&body);
            return Err(ClickHouseError::BadResponse { status, body });
        }

        let summary = resp
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let returned_id = resp
            .headers()
            .get("X-ClickHouse-Query-Id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(&query_id)
            .to_string();

        let stats = QueryStats::from_summary(summary, returned_id);

        let body = resp.bytes().await.map_err(ClickHouseError::Http)?;

        if body.is_empty() {
            return Ok((vec![], stats));
        }

        let reader = StreamReader::try_new(Cursor::new(body.as_ref()), None)
            .map_err(ClickHouseError::ArrowDecode)?;

        let batches: Result<Vec<_>, _> = reader
            .map(|r| r.map_err(ClickHouseError::ArrowDecode))
            .collect();

        Ok((batches?, stats))
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

    pub async fn explain_estimate(&self, sql: &str) -> Result<String, ClickHouseError> {
        self.fetch_text(&format!("EXPLAIN ESTIMATE {sql}")).await
    }

    pub async fn fetch_query_log(
        &self,
        query_id: &str,
    ) -> Result<Option<QueryLogEntry>, ClickHouseError> {
        validate_query_id(query_id)?;
        self.fetch_text("SYSTEM FLUSH LOGS").await?;

        let sql = format!(
            "SELECT \
                query_duration_ms, memory_usage, \
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
            WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
            LIMIT 1 \
            FORMAT JSONEachRow"
        );

        let text = self.fetch_text(&sql).await?;
        if text.trim().is_empty() {
            return Ok(None);
        }

        let v: serde_json::Value =
            serde_json::from_str(text.trim()).map_err(|e| ClickHouseError::BadResponse {
                status: 0,
                body: format!("failed to parse query_log JSON: {e}"),
            })?;

        let profile_events = build_profile_events(&v);

        Ok(Some(QueryLogEntry {
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
            profile_events,
        }))
    }

    pub async fn fetch_processors_profile(
        &self,
        query_id: &str,
    ) -> Result<Vec<ProcessorProfile>, ClickHouseError> {
        validate_query_id(query_id)?;
        self.fetch_text("SYSTEM FLUSH LOGS").await?;

        let sql = format!(
            "SELECT name, elapsed_us, input_wait_elapsed_us, output_wait_elapsed_us, \
                    input_rows, output_rows \
             FROM system.processors_profile_log \
             WHERE query_id = '{query_id}' \
             ORDER BY elapsed_us DESC \
             FORMAT JSONEachRow"
        );

        let text = self.fetch_text(&sql).await?;
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
            ) FORMAT JSONEachRow";

        let uptime_sql = "\
            SELECT value FROM system.asynchronous_metrics \
            WHERE metric = 'Uptime' FORMAT JSONEachRow";

        let disks_sql = "\
            SELECT name, path, total_space AS total_bytes, \
                   free_space AS free_bytes, \
                   (total_space - free_space) AS used_bytes \
            FROM system.disks FORMAT JSONEachRow";

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
            ORDER BY bytes_on_disk DESC \
            FORMAT JSONEachRow";

        let (metrics_text, uptime_text, disks_text, parts_text) = tokio::try_join!(
            self.fetch_text(metrics_sql),
            self.fetch_text(uptime_sql),
            self.fetch_text(disks_sql),
            self.fetch_text(parts_sql),
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

    async fn fetch_text(&self, sql: &str) -> Result<String, ClickHouseError> {
        let mut params: Vec<(String, String)> = vec![("database".into(), self.database.clone())];
        for (k, v) in &self.settings {
            params.push((k.clone(), v.clone()));
        }
        let url = reqwest::Url::parse_with_params(&self.base_url, &params).map_err(|e| {
            ClickHouseError::BadResponse {
                status: 0,
                body: format!("URL parse error: {e}"),
            }
        })?;

        let mut req = self.client().post(url).body(sql.to_string());
        req = req.header("X-ClickHouse-User", &self.username);
        if let Some(pw) = &self.password {
            req = req.header("X-ClickHouse-Key", pw);
        }

        let resp = req.send().await.map_err(ClickHouseError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            let body = truncate_body(&body);
            return Err(ClickHouseError::BadResponse { status, body });
        }

        resp.text().await.map_err(ClickHouseError::Http)
    }
}

fn validate_query_id(query_id: &str) -> Result<(), ClickHouseError> {
    if !query_id
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

fn truncate_body(body: &str) -> String {
    const MAX_LEN: usize = 1024;
    if body.len() <= MAX_LEN {
        body.to_string()
    } else {
        format!("{}... (truncated)", &body[..MAX_LEN])
    }
}

fn build_profile_events(v: &serde_json::Value) -> std::collections::HashMap<String, u64> {
    let mut map = std::collections::HashMap::new();

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

impl std::fmt::Debug for QueryProfiler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryProfiler")
            .field("base_url", &self.base_url)
            .field("database", &self.database)
            .finish()
    }
}

impl Clone for QueryProfiler {
    fn clone(&self) -> Self {
        Self {
            http: once_cell::sync::OnceCell::new(),
            base_url: self.base_url.clone(),
            database: self.database.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            settings: self.settings.clone(),
        }
    }
}
