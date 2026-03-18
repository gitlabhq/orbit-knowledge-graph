/// Project metadata returned by the `/info` endpoint.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProjectInfo {
    pub project_id: i64,
    pub default_branch: String,
}
