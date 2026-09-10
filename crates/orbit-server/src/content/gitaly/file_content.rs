use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gitlab_client::GitlabClient;
use indexer::modules::code::repository::blob_stream::{BlobStream, ResolvedBlob};
use orbit_server_config::{GitalyProxyConfig, GitalyTransport};
use orbit_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use tracing::{debug, warn};

use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

use crate::content::gitaly::proxy::ProxyBlobService;
use crate::content::metrics;

/// `revision` is the git ref used in `<revision>:<path>` for `list_blobs`;
/// prefers `commit_sha` (immutable) over `branch` (can advance).
#[derive(Debug, Clone)]
pub struct GitalyBlobRequest {
    pub project_id: i64,
    pub revision: String,
    pub file_path: String,
    pub start_byte: Option<i64>,
    pub end_byte: Option<i64>,
}

type FileKey = (i64, String, String); // (project_id, revision, file_path)

struct BlobFetch {
    blobs: Vec<ResolvedBlob>,
    stream_error: Option<String>,
}

impl BlobFetch {
    fn complete(blobs: Vec<ResolvedBlob>) -> Self {
        Self {
            blobs,
            stream_error: None,
        }
    }

    fn outcome(&self) -> &'static str {
        if self.stream_error.is_some() {
            "error"
        } else {
            "ok"
        }
    }
}

#[async_trait]
trait RailsBlobFetcher: Send + Sync {
    async fn fetch(&self, project_id: i64, revisions: &[String]) -> Result<BlobFetch, String>;
}

struct GitlabRailsBlobFetcher(Arc<GitlabClient>);

#[async_trait]
impl RailsBlobFetcher for GitlabRailsBlobFetcher {
    async fn fetch(&self, project_id: i64, revisions: &[String]) -> Result<BlobFetch, String> {
        let stream = self
            .0
            .list_blobs(project_id, revisions)
            .await
            .map_err(|error| error.to_string())?;
        let (blobs, error) = BlobStream::new(stream).drain().await;
        Ok(BlobFetch {
            blobs,
            stream_error: error.map(|error| error.to_string()),
        })
    }
}

/// Requests are grouped by `project_id` and deduplicated by file identity.
/// Multiple definitions in the same file share the fetched content and
/// only receive their byte-range slice.
pub struct GitalyContentService {
    rails: Arc<dyn RailsBlobFetcher>,
    proxy: Option<Arc<ProxyBlobService>>,
    transport: GitalyTransport,
}

impl GitalyContentService {
    pub fn new(client: Arc<GitlabClient>) -> Self {
        Self::with_transport(
            client,
            GitalyTransport::RailsHttp,
            &GitalyProxyConfig::default(),
        )
    }

    pub fn with_transport(
        client: Arc<GitlabClient>,
        transport: GitalyTransport,
        proxy_config: &GitalyProxyConfig,
    ) -> Self {
        let proxy = (transport != GitalyTransport::RailsHttp)
            .then(|| Arc::new(ProxyBlobService::new(Arc::clone(&client), proxy_config)));
        Self {
            rails: Arc::new(GitlabRailsBlobFetcher(client)),
            proxy,
            transport,
        }
    }
}

#[async_trait]
impl ColumnResolver for GitalyContentService {
    async fn resolve_batch(
        &self,
        _lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        let mut timer = metrics::start_resolve(rows.len());

        let requests: Vec<Option<(GitalyBlobRequest, FileKey)>> = rows
            .iter()
            .map(|props| {
                let req = Self::build_request(props)?;
                let key = (req.project_id, req.revision.clone(), req.file_path.clone());
                Some((req, key))
            })
            .collect();

        let mut file_cache: HashMap<FileKey, Option<String>> = HashMap::new();

        let mut by_project: HashMap<i64, Vec<FileKey>> = HashMap::new();
        for (req, key) in requests.iter().flatten() {
            if !file_cache.contains_key(key) {
                file_cache.insert(key.clone(), None);
                by_project
                    .entry(req.project_id)
                    .or_default()
                    .push(key.clone());
            }
        }

        let futures = by_project.iter().map(|(&project_id, keys)| {
            let rails = Arc::clone(&self.rails);
            let proxy = self.proxy.clone();
            let transport = self.transport;
            let revisions: Vec<String> = keys
                .iter()
                .map(|(_, revision, path)| format!("{revision}:{path}"))
                .collect();
            let keys = keys.clone();
            async move {
                metrics::record_gitaly_call();
                let blobs = fetch_project_blobs(
                    &rails,
                    proxy.as_deref(),
                    transport,
                    project_id,
                    &revisions,
                )
                .await;

                let fetched = match blobs {
                    Ok(fetched) => fetched,
                    Err(error) => {
                        warn!(
                            project_id,
                            %error,
                            "list_blobs failed, content will be missing for this project"
                        );
                        return (vec![], true);
                    }
                };
                if let Some(error) = &fetched.stream_error {
                    warn!(project_id, %error, "blob stream ended after partial results");
                }
                let had_error = fetched.stream_error.is_some();
                let results = blobs_to_content(project_id, &keys, fetched.blobs);
                (results, had_error)
            }
        });

        let mut had_errors = false;
        for (blobs, errored) in futures::future::join_all(futures).await {
            had_errors |= errored;
            for (key, text) in blobs {
                file_cache.insert(key, Some(text));
            }
        }

        timer.set_outcome(if had_errors { "error" } else { "gitaly_direct" });

        // Non-UTF-8 blobs were filtered during fetch, so their cache entries
        // remain None and resolve to None here.
        Ok(requests
            .iter()
            .map(|entry| {
                let (req, key) = entry.as_ref()?;
                let content = file_cache.get(key)?.as_deref()?;
                Some(ColumnValue::String(
                    slice_content(content, req.start_byte, req.end_byte).to_string(),
                ))
            })
            .collect())
    }
}

async fn fetch_project_blobs(
    rails: &Arc<dyn RailsBlobFetcher>,
    proxy: Option<&ProxyBlobService>,
    transport: GitalyTransport,
    project_id: i64,
    revisions: &[String],
) -> Result<BlobFetch, String> {
    if transport == GitalyTransport::RailsHttp {
        let result = rails.fetch(project_id, revisions).await;
        let outcome = match &result {
            Ok(fetched) => fetched.outcome(),
            Err(_) => "error",
        };
        metrics::record_gitaly_transport("rails_http", outcome);
        return result;
    }

    let proxy = proxy.expect("proxy service exists for WebSocket transports");
    match proxy.fetch(project_id, revisions).await {
        Ok(blobs) => {
            metrics::record_gitaly_transport("workhorse_ws", "ok");
            Ok(BlobFetch::complete(blobs))
        }
        Err(crate::content::gitaly::proxy::ProxyBlobError::Stream { status, partial }) => {
            metrics::record_gitaly_transport("workhorse_ws", "stream_error");
            Ok(BlobFetch {
                blobs: partial,
                stream_error: Some(status.to_string()),
            })
        }
        Err(error)
            if transport == GitalyTransport::WorkhorseWsWithFallback
                && error.allows_rails_fallback() =>
        {
            let fallback = rails.fetch(project_id, revisions).await;
            let outcome = match &fallback {
                Ok(fetched) if fetched.stream_error.is_none() => "fallback_ok",
                Ok(_) | Err(_) => "fallback_error",
            };
            metrics::record_gitaly_transport("workhorse_ws", outcome);
            fallback
        }
        Err(error) => {
            metrics::record_gitaly_transport("workhorse_ws", error.outcome());
            Err(error.to_string())
        }
    }
}

fn blobs_to_content(
    project_id: i64,
    keys: &[FileKey],
    blobs: Vec<ResolvedBlob>,
) -> Vec<(FileKey, String)> {
    blobs
        .into_iter()
        .zip(keys.iter())
        .filter_map(|(blob, key)| match String::from_utf8(blob.data) {
            Ok(text) => {
                metrics::record_blob_bytes(text.len() as u64);
                Some((key.clone(), text))
            }
            Err(_) => {
                debug!(project_id, path = %key.2, "skipping binary blob");
                None
            }
        })
        .collect()
}

impl GitalyContentService {
    /// Expects `project_id` and either `path` (File) or `file_path`
    /// (Definition). Returns `None` if any required field is missing or byte
    /// ranges are invalid.
    pub fn build_request(props: &HashMap<String, ColumnValue>) -> Option<GitalyBlobRequest> {
        let project_id: i64 = props.get("project_id").and_then(|v| v.coerce())?;

        // Prefer commit_sha (immutable) over branch (can advance).
        let revision: String = props
            .get("commit_sha")
            .and_then(|v| v.coerce::<String>())
            .filter(|s| !s.is_empty())
            .or_else(|| props.get("branch").and_then(|v| v.coerce()))?;

        let file_path: String = props
            .get("file_path")
            .or_else(|| props.get("path"))
            .and_then(|v| v.coerce())?;

        let start_byte: Option<i64> = props.get("start_byte").and_then(|v| v.coerce());
        let end_byte: Option<i64> = props.get("end_byte").and_then(|v| v.coerce());

        match (start_byte, end_byte) {
            (Some(s), Some(e)) if s < 0 || e < 0 || s > e => return None,
            _ => {}
        }

        Some(GitalyBlobRequest {
            project_id,
            revision,
            file_path,
            start_byte,
            end_byte,
        })
    }
}

/// Return the byte-range slice of `content`, or the full string when no
/// range is specified. Returns an empty string if the range is out of
/// bounds or lands on a UTF-8 boundary.
fn slice_content(content: &str, start_byte: Option<i64>, end_byte: Option<i64>) -> &str {
    match (start_byte, end_byte) {
        (Some(s), Some(e)) if s >= 0 && e >= s => {
            let s = s as usize;
            let e = (e as usize).min(content.len());
            if s >= content.len() {
                return "";
            }
            // str::get checks UTF-8 char boundaries and returns None
            // if either index falls inside a multi-byte character.
            content.get(s..e).unwrap_or("")
        }
        _ => content,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gitlab_client::test_support::{FakeWorkhorse, Preauth, StreamPlan, serve};
    use tonic::Code;

    use super::*;

    struct MockRails {
        calls: AtomicUsize,
    }

    struct PartialRails;

    #[async_trait]
    impl RailsBlobFetcher for PartialRails {
        async fn fetch(
            &self,
            _project_id: i64,
            _revisions: &[String],
        ) -> Result<BlobFetch, String> {
            Ok(BlobFetch {
                blobs: vec![ResolvedBlob {
                    oid: "first".to_owned(),
                    data: b"partial content".to_vec(),
                }],
                stream_error: Some("oversized second blob".to_owned()),
            })
        }
    }

    #[async_trait]
    impl RailsBlobFetcher for MockRails {
        async fn fetch(
            &self,
            _project_id: i64,
            _revisions: &[String],
        ) -> Result<BlobFetch, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(BlobFetch::complete(Vec::new()))
        }
    }

    async fn wait_for_rpcs(fake: &FakeWorkhorse, expected: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while fake.rpcs() < expected {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn build_request_from_file_props() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(42));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.project_id, 42);
        assert_eq!(req.revision, "main");
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, None);
        assert_eq!(req.end_byte, None);
    }

    #[test]
    fn build_request_from_definition_props() {
        let props = definition_props(100, 200);

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.file_path, "src/lib.rs");
        assert_eq!(req.start_byte, Some(100));
        assert_eq!(req.end_byte, Some(200));
    }

    #[test]
    fn build_request_none_without_project_id() {
        let mut props = HashMap::new();
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));

        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_prefers_file_path_over_path() {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(1));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("path".into(), ColumnValue::String("old.rs".into()));
        props.insert("file_path".into(), ColumnValue::String("new.rs".into()));

        let req = GitalyContentService::build_request(&props).unwrap();
        assert_eq!(req.file_path, "new.rs");
    }

    #[test]
    fn build_request_rejects_negative_start_byte() {
        let props = definition_props(-1, 200);
        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_rejects_start_after_end() {
        let props = definition_props(200, 100);
        assert!(GitalyContentService::build_request(&props).is_none());
    }

    #[test]
    fn build_request_accepts_equal_start_end() {
        let props = definition_props(100, 100);
        assert!(GitalyContentService::build_request(&props).is_some());
    }

    #[test]
    fn slice_full_when_no_range() {
        assert_eq!(slice_content("hello world", None, None), "hello world");
    }

    #[test]
    fn slice_byte_range() {
        assert_eq!(slice_content("hello world", Some(6), Some(11)), "world");
    }

    #[test]
    fn slice_clamps_end_to_content_len() {
        assert_eq!(slice_content("hi", Some(0), Some(999)), "hi");
    }

    #[test]
    fn slice_empty_when_start_past_end_of_content() {
        assert_eq!(slice_content("hi", Some(100), Some(200)), "");
    }

    #[test]
    fn slice_empty_on_utf8_boundary() {
        // 'é' is 2 bytes (0xC3 0xA9). Slicing at byte 1 lands mid-character.
        assert_eq!(slice_content("é", Some(0), Some(1)), "");
    }

    #[tokio::test]
    async fn rails_stream_errors_keep_decoded_partial_results() {
        let rails: Arc<dyn RailsBlobFetcher> = Arc::new(PartialRails);

        let fetched = fetch_project_blobs(
            &rails,
            None,
            GitalyTransport::RailsHttp,
            42,
            &["HEAD:a".to_owned(), "HEAD:b".to_owned()],
        )
        .await
        .unwrap();

        assert_eq!(fetched.blobs.len(), 1);
        assert_eq!(fetched.blobs[0].data, b"partial content");
        assert!(fetched.stream_error.is_some());
    }

    #[tokio::test]
    async fn proxy_mid_stream_partials_do_not_fallback() {
        let fake = FakeWorkhorse::start(
            Preauth::ok("600"),
            Arc::new(|_| StreamPlan::ServeThenCut {
                count: 2,
                code: Code::Internal,
            }),
        )
        .await;
        let proxy = ProxyBlobService::new(Arc::new(fake.client()), &GitalyProxyConfig::default());
        let concrete_rails = Arc::new(MockRails {
            calls: AtomicUsize::new(0),
        });
        let rails: Arc<dyn RailsBlobFetcher> = concrete_rails.clone();

        let fetched = fetch_project_blobs(
            &rails,
            Some(&proxy),
            GitalyTransport::WorkhorseWsWithFallback,
            42,
            &["HEAD:a".to_owned(), "HEAD:b".to_owned()],
        )
        .await
        .unwrap();

        assert_eq!(fetched.blobs.len(), 1);
        assert!(fetched.stream_error.is_some());
        assert_eq!(concrete_rails.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stream_saturation_falls_back_to_rails() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 500)).await;
        let config = GitalyProxyConfig {
            webserver_max_inflight_streams: 1,
            ..Default::default()
        };
        let proxy = Arc::new(ProxyBlobService::new(Arc::new(fake.client()), &config));
        let active = {
            let proxy = Arc::clone(&proxy);
            tokio::spawn(async move { proxy.fetch(42, &["HEAD:a".to_owned()]).await })
        };
        wait_for_rpcs(&fake, 1).await;
        let concrete_rails = Arc::new(MockRails {
            calls: AtomicUsize::new(0),
        });
        let rails: Arc<dyn RailsBlobFetcher> = concrete_rails.clone();

        fetch_project_blobs(
            &rails,
            Some(&proxy),
            GitalyTransport::WorkhorseWsWithFallback,
            42,
            &["HEAD:b".to_owned()],
        )
        .await
        .unwrap();

        assert_eq!(concrete_rails.calls.load(Ordering::SeqCst), 1);
        active.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn flag_off_uses_negative_cache_before_falling_back() {
        let fake =
            FakeWorkhorse::start(Preauth::reject(reqwest::StatusCode::NOT_FOUND), serve(1, 0))
                .await;
        let proxy = ProxyBlobService::new(Arc::new(fake.client()), &GitalyProxyConfig::default());
        let concrete_rails = Arc::new(MockRails {
            calls: AtomicUsize::new(0),
        });
        let rails: Arc<dyn RailsBlobFetcher> = concrete_rails.clone();

        for _ in 0..2 {
            fetch_project_blobs(
                &rails,
                Some(&proxy),
                GitalyTransport::WorkhorseWsWithFallback,
                42,
                &["HEAD:a".to_owned()],
            )
            .await
            .unwrap();
        }

        assert_eq!(fake.preauth_requests(), 1);
        assert_eq!(concrete_rails.calls.load(Ordering::SeqCst), 2);
    }

    fn definition_props(start: i64, end: i64) -> HashMap<String, ColumnValue> {
        let mut props = HashMap::new();
        props.insert("project_id".into(), ColumnValue::Int64(42));
        props.insert("branch".into(), ColumnValue::String("main".into()));
        props.insert("file_path".into(), ColumnValue::String("src/lib.rs".into()));
        props.insert("start_byte".into(), ColumnValue::Int64(start));
        props.insert("end_byte".into(), ColumnValue::Int64(end));
        props
    }
}
