use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryLogEntry {
    #[serde(default)]
    pub query_id: String,
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
