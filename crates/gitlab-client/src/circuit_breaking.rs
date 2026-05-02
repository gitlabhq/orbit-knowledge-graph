use circuit_breaker::CircuitBreaker;

use crate::client::{ByteStream, GitlabClient};
use crate::error::GitlabClientError;
use crate::types::{MergeRequestDiffBatch, ProjectInfo};

pub struct CircuitBreakingGitlabClient {
    client: GitlabClient,
    breaker: CircuitBreaker,
}

impl CircuitBreakingGitlabClient {
    pub fn new(client: GitlabClient, breaker: CircuitBreaker) -> Self {
        Self { client, breaker }
    }

    pub fn client(&self) -> &GitlabClient {
        &self.client
    }

    pub async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || self.client.project_info(project_id),
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || self.client.download_archive(project_id, ref_name),
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || self.client.changed_paths(project_id, from_sha, to_sha),
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn list_blobs(
        &self,
        project_id: i64,
        oids: &[String],
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || self.client.list_blobs(project_id, oids),
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn list_merge_request_diff_files(
        &self,
        project_id: i64,
        diff_id: i64,
        paths: &[String],
    ) -> Result<MergeRequestDiffBatch, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || {
                    self.client
                        .list_merge_request_diff_files(project_id, diff_id, paths)
                },
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn get_merge_request_raw_diff(
        &self,
        project_id: i64,
        diff_id: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || self.client.get_merge_request_raw_diff(project_id, diff_id),
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }

    pub async fn get_merge_request_raw_diff_by_iid(
        &self,
        project_id: i64,
        merge_request_iid: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_with_filter(
                || {
                    self.client
                        .get_merge_request_raw_diff_by_iid(project_id, merge_request_iid)
                },
                GitlabClientError::is_transient,
            )
            .await
            .map_err(GitlabClientError::from_circuit_breaker)
    }
}
