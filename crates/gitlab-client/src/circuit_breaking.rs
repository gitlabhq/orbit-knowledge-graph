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
            .call_transient(|| self.client.project_info(project_id))
            .await
    }

    pub async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_transient(|| self.client.download_archive(project_id, ref_name))
            .await
    }

    pub async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_transient(|| self.client.changed_paths(project_id, from_sha, to_sha))
            .await
    }

    pub async fn list_blobs(
        &self,
        project_id: i64,
        oids: &[String],
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_transient(|| self.client.list_blobs(project_id, oids))
            .await
    }

    pub async fn list_merge_request_diff_files(
        &self,
        project_id: i64,
        diff_id: i64,
        paths: &[String],
    ) -> Result<MergeRequestDiffBatch, GitlabClientError> {
        self.breaker
            .call_transient(|| {
                self.client
                    .list_merge_request_diff_files(project_id, diff_id, paths)
            })
            .await
    }

    pub async fn get_merge_request_raw_diff(
        &self,
        project_id: i64,
        diff_id: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_transient(|| self.client.get_merge_request_raw_diff(project_id, diff_id))
            .await
    }

    pub async fn get_merge_request_raw_diff_by_iid(
        &self,
        project_id: i64,
        merge_request_iid: i64,
    ) -> Result<ByteStream, GitlabClientError> {
        self.breaker
            .call_transient(|| {
                self.client
                    .get_merge_request_raw_diff_by_iid(project_id, merge_request_iid)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use axum::Router;
    use axum::http::StatusCode as AxumStatus;
    use axum::routing::get;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use circuit_breaker::{CircuitBreakerRegistry, CircuitConfig, ServiceName};
    use tokio::net::TcpListener;

    use super::*;
    use orbit_server_config::GitlabClientConfiguration;

    #[derive(Clone, Copy)]
    struct TestService;

    impl ServiceName for TestService {
        fn as_str(&self) -> &'static str {
            "gitlab-test"
        }
    }

    fn breaker() -> CircuitBreaker {
        let mut configs = HashMap::new();
        configs.insert(
            "gitlab-test",
            CircuitConfig {
                failure_threshold: 2,
                window: Duration::from_secs(10),
                cooldown: Duration::from_secs(60),
            },
        );
        CircuitBreakerRegistry::without_observer(configs).circuit_breaker(TestService)
    }

    fn config_for(base_url: String) -> GitlabClientConfiguration {
        GitlabClientConfiguration {
            base_url,
            signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
            resolve_host: None,
        }
    }

    /// The returned counter proves an open circuit stops calling out at all.
    async fn stub_project_info_server(status: AxumStatus) -> (String, Arc<AtomicU32>) {
        let hits = Arc::new(AtomicU32::new(0));
        let hits_handler = hits.clone();
        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{id}/info",
            get(move || {
                let hits = hits_handler.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    status
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), hits)
    }

    #[tokio::test]
    async fn repeated_server_errors_trip_the_circuit_and_reject_further_calls() {
        let (url, hits) = stub_project_info_server(AxumStatus::INTERNAL_SERVER_ERROR).await;
        let client = GitlabClient::new(config_for(url)).unwrap();
        let wrapped = CircuitBreakingGitlabClient::new(client, breaker());

        for _ in 0..2 {
            let err = wrapped.project_info(1).await.unwrap_err();
            assert!(matches!(err, GitlabClientError::ServerError { .. }));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let err = wrapped.project_info(1).await.unwrap_err();
        assert!(matches!(err, GitlabClientError::CircuitOpen { .. }));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "an open circuit must not reach the server"
        );
    }

    #[tokio::test]
    async fn not_found_never_trips_the_circuit() {
        let (url, hits) = stub_project_info_server(AxumStatus::NOT_FOUND).await;
        let client = GitlabClient::new(config_for(url)).unwrap();
        let wrapped = CircuitBreakingGitlabClient::new(client, breaker());

        for _ in 0..5 {
            let err = wrapped.project_info(1).await.unwrap_err();
            assert!(matches!(err, GitlabClientError::NotFound(1)));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn unauthorized_never_trips_the_circuit() {
        let (url, hits) = stub_project_info_server(AxumStatus::UNAUTHORIZED).await;
        let client = GitlabClient::new(config_for(url)).unwrap();
        let wrapped = CircuitBreakingGitlabClient::new(client, breaker());

        for _ in 0..5 {
            let err = wrapped.project_info(1).await.unwrap_err();
            assert!(matches!(err, GitlabClientError::Unauthorized));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn connection_failure_is_transient_and_trips_the_circuit() {
        let client = GitlabClient::new(config_for("http://127.0.0.1:1".into())).unwrap();
        let wrapped = CircuitBreakingGitlabClient::new(client, breaker());

        for _ in 0..2 {
            let err = wrapped.project_info(1).await.unwrap_err();
            assert!(matches!(err, GitlabClientError::Request(_)));
        }

        let err = wrapped.project_info(1).await.unwrap_err();
        assert!(matches!(err, GitlabClientError::CircuitOpen { .. }));
    }
}
