/// Project metadata returned by the `/info` endpoint.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProjectInfo {
    pub project_id: i64,
    pub default_branch: String,
}

/// A single entry from the `/changed_paths` NDJSON response.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChangedPath {
    pub path: String,
    pub status: ChangeStatus,
    #[serde(default)]
    pub old_path: String,
    pub new_mode: u32,
    #[serde(default)]
    pub old_mode: u32,
    pub old_blob_id: String,
    pub new_blob_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChangeStatus {
    Deleted,
    Renamed,
    Added,
    Modified,
    Copied,
    TypeChange,
    #[serde(other)]
    Unknown,
}
