/// Project metadata returned by the `/info` endpoint.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProjectInfo {
    pub project_id: i64,
    pub default_branch: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MergeRequestDiffBatch {
    #[serde(rename = "id")]
    pub merge_request_diff_id: i64,
    pub head_commit_sha: Option<String>,
    pub base_commit_sha: Option<String>,
    pub start_commit_sha: Option<String>,
    pub diffs: Vec<MergeRequestDiffFileEntry>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MergeRequestDiffFileEntry {
    pub old_path: String,
    pub new_path: String,
    pub a_mode: Option<String>,
    pub b_mode: Option<String>,
    pub new_file: bool,
    pub renamed_file: bool,
    pub deleted_file: bool,
    pub generated_file: Option<bool>,
    pub collapsed: bool,
    pub too_large: bool,
    pub diff: String,
}
