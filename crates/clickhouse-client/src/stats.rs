use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub query_id: String,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub elapsed_ns: u64,
    pub memory_usage: i64,
}

impl QueryStats {
    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed_ns as f64 / 1_000_000.0
    }

    pub(crate) fn from_summary(header: &str, query_id: String) -> Self {
        let mut stats = Self {
            query_id,
            ..Default::default()
        };

        let Ok(v) = serde_json::from_str::<serde_json::Value>(header) else {
            return stats;
        };

        stats.read_rows = parse_u64(&v, "read_rows");
        stats.read_bytes = parse_u64(&v, "read_bytes");
        stats.result_rows = parse_u64(&v, "result_rows");
        stats.result_bytes = parse_u64(&v, "result_bytes");
        stats.elapsed_ns = parse_u64(&v, "elapsed_ns");
        stats.memory_usage = parse_i64(&v, "memory_usage");

        stats
    }
}

fn parse_u64(v: &serde_json::Value, key: &str) -> u64 {
    v.get(key)
        .and_then(|val| val.as_u64().or_else(|| val.as_str()?.parse().ok()))
        .unwrap_or(0)
}

fn parse_i64(v: &serde_json::Value, key: &str) -> i64 {
    v.get(key)
        .and_then(|val| val.as_i64().or_else(|| val.as_str()?.parse().ok()))
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryLogEntry {
    pub query_duration_ms: f64,
    pub memory_usage: u64,
    pub peak_memory_usage: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub real_time_us: u64,
    pub user_time_us: u64,
    pub system_time_us: u64,
    pub os_cpu_wait_us: u64,
    pub os_io_wait_us: u64,
    pub selected_parts: u64,
    pub selected_marks: u64,
    pub selected_rows: u64,
    pub mark_cache_hits: u64,
    pub mark_cache_misses: u64,
    pub external_sort_bytes: u64,
    pub external_agg_bytes: u64,
    pub external_join_bytes: u64,
    pub profile_events: HashMap<String, u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessorProfile {
    pub name: String,
    pub elapsed_us: u64,
    pub input_wait_us: u64,
    pub output_wait_us: u64,
    pub input_rows: u64,
    pub output_rows: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceHealth {
    pub current_queries: u64,
    pub memory_tracking_bytes: u64,
    pub mark_cache_bytes: u64,
    pub mark_cache_files: u64,
    pub query_cache_entries: u64,
    pub query_cache_bytes: u64,
    pub active_merges: u64,
    pub temp_files_sort: u64,
    pub temp_files_agg: u64,
    pub temp_files_join: u64,
    pub uptime_seconds: u64,
    pub disk_usage: Vec<DiskInfo>,
    pub table_parts: Vec<TablePartsInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskInfo {
    pub name: String,
    pub path: String,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TablePartsInfo {
    pub database: String,
    pub table: String,
    pub part_count: u64,
    pub total_rows: u64,
    pub bytes_on_disk: u64,
    pub partition_count: u64,
    pub active_mutations: u64,
    pub detached_parts: u64,
    pub last_modification_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_summary_string_values() {
        let header = r#"{"read_rows":"1078975","read_bytes":"26728779","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","result_rows":"3","result_bytes":"4096","elapsed_ns":"62800000","memory_usage":"1048576"}"#;
        let stats = QueryStats::from_summary(header, "test-id".into());
        assert_eq!(stats.query_id, "test-id");
        assert_eq!(stats.read_rows, 1078975);
        assert_eq!(stats.read_bytes, 26728779);
        assert_eq!(stats.result_rows, 3);
        assert_eq!(stats.result_bytes, 4096);
        assert_eq!(stats.elapsed_ns, 62800000);
        assert_eq!(stats.memory_usage, 1048576);
    }

    #[test]
    fn parse_summary_numeric_values() {
        let header = r#"{"read_rows":500,"read_bytes":1024,"result_rows":10,"result_bytes":256,"elapsed_ns":5000000,"memory_usage":-100}"#;
        let stats = QueryStats::from_summary(header, "num-id".into());
        assert_eq!(stats.read_rows, 500);
        assert_eq!(stats.read_bytes, 1024);
        assert_eq!(stats.result_rows, 10);
        assert_eq!(stats.result_bytes, 256);
        assert_eq!(stats.elapsed_ns, 5000000);
        assert_eq!(stats.memory_usage, -100);
    }

    #[test]
    fn parse_summary_missing_fields() {
        let header = r#"{"read_rows":"42"}"#;
        let stats = QueryStats::from_summary(header, "partial".into());
        assert_eq!(stats.read_rows, 42);
        assert_eq!(stats.read_bytes, 0);
        assert_eq!(stats.memory_usage, 0);
    }

    #[test]
    fn parse_summary_invalid_json() {
        let stats = QueryStats::from_summary("not json", "bad".into());
        assert_eq!(stats.read_rows, 0);
        assert_eq!(stats.query_id, "bad");
    }

    #[test]
    fn elapsed_ms_conversion() {
        let stats = QueryStats {
            elapsed_ns: 62_800_000,
            ..Default::default()
        };
        assert!((stats.elapsed_ms() - 62.8).abs() < 0.001);
    }
}
