use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const INDEXING_PROGRESS_BUCKET: &str = "indexing_progress";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CountsSnapshot {
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub nodes: HashMap<String, i64>,
    #[serde(default)]
    pub edges: HashMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodeProgressSnapshot {
    #[serde(default)]
    pub traversal_path: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub branches: HashMap<String, BranchCodeSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BranchCodeSnapshot {
    #[serde(default)]
    pub commit: String,
    #[serde(default)]
    pub indexed_at: String,
    #[serde(default)]
    pub nodes: HashMap<String, i64>,
    #[serde(default)]
    pub edges: HashMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetaSnapshot {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub initial_backfill_done: bool,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub sdlc: SdlcMeta,
    #[serde(default)]
    pub code: CodeMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SdlcMeta {
    #[serde(default)]
    pub last_completed_at: String,
    #[serde(default)]
    pub last_started_at: String,
    #[serde(default)]
    pub last_duration_ms: i64,
    #[serde(default)]
    pub cycle_count: i64,
    #[serde(default)]
    pub last_error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodeMeta {
    #[serde(default)]
    pub projects_indexed: i64,
    #[serde(default)]
    pub projects_total: i64,
    #[serde(default)]
    pub last_indexed_at: String,
}

pub fn counts_key(traversal_path: &str) -> String {
    let tp_dots = traversal_path.trim_end_matches('/').replace('/', ".");
    format!("counts.{tp_dots}")
}

pub fn meta_key(namespace_id: i64) -> String {
    format!("meta.{namespace_id}")
}

pub fn code_key(project_id: i64) -> String {
    format!("code.{project_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_key_format() {
        assert_eq!(counts_key("1/9970/"), "counts.1.9970");
        assert_eq!(counts_key("1/9970/55154808/"), "counts.1.9970.55154808");
    }

    #[test]
    fn meta_key_format() {
        assert_eq!(meta_key(9970), "meta.9970");
    }

    #[test]
    fn code_key_format() {
        assert_eq!(code_key(12345), "code.12345");
    }
}
