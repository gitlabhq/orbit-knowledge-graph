/// Per-project Gitaly connection details and repository metadata returned by Rails.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RepositoryInfo {
    pub project_id: i64,
    pub gitaly_connection_info: GitalyConnectionInfo,
    pub default_branch: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GitalyConnectionInfo {
    pub address: String,
    pub token: Option<String>,
    pub storage: String,
    pub path: String,
}
