//! Builders for GitLab's internal Orbit API paths
//! (`/api/v4/internal/orbit/...`), centralized so a future breaking-change
//! version bump has one function per route to update.

pub(crate) fn project_info(base_url: &str, project_id: i64) -> String {
    format!("{base_url}/api/v4/internal/orbit/project/{project_id}/info")
}

pub(crate) fn cloud_connector_token(base_url: &str) -> String {
    format!("{base_url}/api/v4/internal/orbit/cloud_connector_token")
}

pub(crate) fn project_archive(base_url: &str, project_id: i64) -> String {
    format!("{base_url}/api/v4/internal/orbit/project/{project_id}/repository/archive")
}

pub(crate) fn project_changed_paths(base_url: &str, project_id: i64) -> String {
    format!("{base_url}/api/v4/internal/orbit/project/{project_id}/repository/changed_paths")
}

pub(crate) fn project_list_blobs(base_url: &str, project_id: i64) -> String {
    format!("{base_url}/api/v4/internal/orbit/project/{project_id}/repository/list_blobs")
}

pub(crate) fn merge_request_diff_files(base_url: &str, project_id: i64, diff_id: i64) -> String {
    format!("{base_url}/api/v4/internal/orbit/project/{project_id}/merge_request_diffs/{diff_id}")
}

pub(crate) fn merge_request_diff_raw(base_url: &str, project_id: i64, diff_id: i64) -> String {
    format!(
        "{base_url}/api/v4/internal/orbit/project/{project_id}/merge_request_diffs/{diff_id}/raw_diffs"
    )
}

pub(crate) fn merge_request_raw_diff_by_iid(
    base_url: &str,
    project_id: i64,
    merge_request_iid: i64,
) -> String {
    format!(
        "{base_url}/api/v4/internal/orbit/project/{project_id}/merge_requests/{merge_request_iid}/raw_diffs"
    )
}
