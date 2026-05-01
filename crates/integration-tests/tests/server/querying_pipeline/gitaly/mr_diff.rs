use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Body;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use gitlab_client::GitlabClient;
use gkg_server::content::gitaly::mr_diff::MergeRequestDiffContentService;
use gkg_utils::arrow::ColumnValue;
use query_engine::shared::content::{ColumnResolver, ResolverContext};
use tokio::net::TcpListener;

type PropertyRow = HashMap<String, ColumnValue>;

struct MockServer {
    client: Arc<GitlabClient>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn extract_query_paths(uri: &axum::http::Uri) -> Vec<String> {
    let full = format!("http://localhost{uri}");
    reqwest::Url::parse(&full)
        .unwrap()
        .query_pairs()
        .filter(|(k, _)| k == "paths[]")
        .map(|(_, v)| v.into_owned())
        .collect()
}

async fn mock_mr_diff_server(
    patch_handler: impl Fn(i64, i64, Vec<String>) -> (StatusCode, String)
    + Send
    + Sync
    + Clone
    + 'static,
    raw_handler: impl Fn(i64, i64) -> (StatusCode, Vec<u8>) + Send + Sync + Clone + 'static,
) -> MockServer {
    let app = Router::new()
        .route(
            "/api/v4/internal/orbit/project/{project_id}/merge_request_diffs/{diff_id}",
            get({
                let h = patch_handler.clone();
                move |Path((pid, did)): Path<(i64, i64)>, req: axum::extract::Request| {
                    let h = h.clone();
                    let paths = extract_query_paths(req.uri());
                    async move {
                        let (status, body) = h(pid, did, paths);
                        (status, body).into_response()
                    }
                }
            }),
        )
        .route(
            "/api/v4/internal/orbit/project/{project_id}/merge_request_diffs/{diff_id}/raw_diffs",
            get({
                let h = raw_handler.clone();
                move |Path((pid, did)): Path<(i64, i64)>| {
                    let h = h.clone();
                    async move {
                        let (status, body) = h(pid, did);
                        (status, Body::from(body)).into_response()
                    }
                }
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = GitlabClient::new(gkg_server_config::GitlabClientConfiguration {
        base_url: format!("http://{addr}"),
        signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
        resolve_host: None,
    })
    .unwrap();

    MockServer {
        client: Arc::new(client),
        handle,
    }
}

fn resolver_ctx() -> ResolverContext {
    ResolverContext::default()
}

fn patch_row(project_id: i64, diff_id: i64, new_path: &str, old_path: &str) -> PropertyRow {
    let mut row = PropertyRow::new();
    row.insert("project_id".into(), ColumnValue::Int64(project_id));
    row.insert("merge_request_diff_id".into(), ColumnValue::Int64(diff_id));
    row.insert("new_path".into(), ColumnValue::String(new_path.into()));
    row.insert("old_path".into(), ColumnValue::String(old_path.into()));
    row
}

fn raw_patch_row(project_id: i64, diff_id: i64) -> PropertyRow {
    let mut row = PropertyRow::new();
    row.insert("project_id".into(), ColumnValue::Int64(project_id));
    row.insert("id".into(), ColumnValue::Int64(diff_id));
    row
}

fn diff_batch_json(diff_id: i64, diffs: &[(&str, &str, &str)]) -> String {
    let entries: Vec<serde_json::Value> = diffs
        .iter()
        .map(|(old, new, diff)| {
            serde_json::json!({
                "old_path": old,
                "new_path": new,
                "a_mode": "100644",
                "b_mode": "100644",
                "new_file": false,
                "renamed_file": false,
                "deleted_file": false,
                "generated_file": false,
                "collapsed": false,
                "too_large": false,
                "diff": diff,
            })
        })
        .collect();

    serde_json::json!({
        "id": diff_id,
        "head_commit_sha": "abc123",
        "diffs": entries,
    })
    .to_string()
}

fn noop_patch(_pid: i64, _did: i64, _paths: Vec<String>) -> (StatusCode, String) {
    panic!("patch endpoint should not be called")
}

fn noop_raw(_pid: i64, _did: i64) -> (StatusCode, Vec<u8>) {
    panic!("raw_diffs endpoint should not be called")
}

#[tokio::test]
async fn patch_resolves_single_file() {
    let mock = mock_mr_diff_server(
        |_pid, diff_id, _paths| {
            let body = diff_batch_json(diff_id, &[("a.rs", "a.rs", "@@ changed")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = patch_row(1, 42, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(
        results,
        vec![Some(ColumnValue::String("@@ changed".into()))]
    );
}

#[tokio::test]
async fn patch_resolves_multiple_files_same_snapshot() {
    let mock = mock_mr_diff_server(
        |_pid, diff_id, _paths| {
            let body = diff_batch_json(
                diff_id,
                &[("a.rs", "a.rs", "@@ a"), ("b.rs", "b.rs", "@@ b")],
            );
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = patch_row(1, 42, "a.rs", "a.rs");
    let r2 = patch_row(1, 42, "b.rs", "b.rs");
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results[0], Some(ColumnValue::String("@@ a".into())));
    assert_eq!(results[1], Some(ColumnValue::String("@@ b".into())));
}

#[tokio::test]
async fn patch_deduplicates_same_file() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let counter = call_count.clone();

    let mock = mock_mr_diff_server(
        move |_pid, diff_id, _paths| {
            counter.fetch_add(1, Ordering::SeqCst);
            let body = diff_batch_json(diff_id, &[("a.rs", "a.rs", "@@ dedup")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = patch_row(1, 42, "a.rs", "a.rs");
    let r2 = patch_row(1, 42, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0], Some(ColumnValue::String("@@ dedup".into())));
}

#[tokio::test]
async fn patch_groups_by_project_and_diff_id() {
    let snapshots_called = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = snapshots_called.clone();

    let mock = mock_mr_diff_server(
        move |pid, did, _paths| {
            tracker.lock().unwrap().push((pid, did));
            let body = diff_batch_json(did, &[("a.rs", "a.rs", "@@ diff")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = patch_row(1, 10, "a.rs", "a.rs");
    let r2 = patch_row(2, 20, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    svc.resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    let mut called = snapshots_called.lock().unwrap().clone();
    called.sort();
    assert_eq!(called, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn patch_sends_paths_query_param() {
    let received_paths = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = received_paths.clone();

    let mock = mock_mr_diff_server(
        move |_pid, diff_id, paths| {
            tracker.lock().unwrap().extend(paths);
            let body = diff_batch_json(
                diff_id,
                &[("a.rs", "a.rs", "@@ a"), ("b.rs", "b.rs", "@@ b")],
            );
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = patch_row(1, 42, "a.rs", "a.rs");
    let r2 = patch_row(1, 42, "b.rs", "b.rs");
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    svc.resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    let mut got = received_paths.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec!["a.rs", "b.rs"]);
}

#[tokio::test]
async fn patch_matches_by_canonical_path_old_path_fallback() {
    let mock = mock_mr_diff_server(
        |_pid, diff_id, _paths| {
            let body = diff_batch_json(diff_id, &[("renamed.rs", "", "@@ renamed")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = patch_row(1, 42, "", "renamed.rs");
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results[0], Some(ColumnValue::String("@@ renamed".into())));
}

#[tokio::test]
async fn patch_missing_project_id_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let mut row = PropertyRow::new();
    row.insert("merge_request_diff_id".into(), ColumnValue::Int64(42));
    row.insert("new_path".into(), ColumnValue::String("a.rs".into()));
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn patch_missing_diff_id_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let mut row = PropertyRow::new();
    row.insert("project_id".into(), ColumnValue::Int64(1));
    row.insert("new_path".into(), ColumnValue::String("a.rs".into()));
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn patch_too_large_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let mut row = patch_row(1, 42, "big.rs", "big.rs");
    row.insert("too_large".into(), ColumnValue::String("true".into()));
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn patch_empty_diff_returns_none() {
    let mock = mock_mr_diff_server(
        |_pid, diff_id, _paths| {
            let body = diff_batch_json(diff_id, &[("a.rs", "a.rs", "")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = patch_row(1, 42, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn patch_gitlab_500_returns_none() {
    let mock = mock_mr_diff_server(
        |_pid, _did, _paths| (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = patch_row(1, 42, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn patch_mixed_valid_and_invalid_rows() {
    let mock = mock_mr_diff_server(
        |_pid, diff_id, _paths| {
            let body = diff_batch_json(diff_id, &[("a.rs", "a.rs", "@@ ok")]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let valid = patch_row(1, 42, "a.rs", "a.rs");
    let invalid = PropertyRow::new();
    let rows: Vec<&PropertyRow> = vec![&valid, &invalid];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results[0], Some(ColumnValue::String("@@ ok".into())));
    assert_eq!(results[1], None);
}

#[tokio::test]
async fn patch_empty_batch() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let rows: Vec<&PropertyRow> = vec![];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn patch_same_path_different_projects() {
    let projects_called = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = projects_called.clone();

    let mock = mock_mr_diff_server(
        move |pid, did, _paths| {
            tracker.lock().unwrap().push(pid);
            let diff = format!("@@ project-{pid}");
            let body = diff_batch_json(did, &[("a.rs", "a.rs", &diff)]);
            (StatusCode::OK, body)
        },
        noop_raw,
    )
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = patch_row(1, 10, "a.rs", "a.rs");
    let r2 = patch_row(2, 20, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    let results = svc
        .resolve_batch("patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_ne!(results[0], results[1]);
    let mut called = projects_called.lock().unwrap().clone();
    called.sort();
    assert_eq!(called, vec![1, 2]);
}

#[tokio::test]
async fn raw_patch_resolves_single() {
    let patch = "diff --git a/f.rs b/f.rs\n--- a/f.rs\n+++ b/f.rs\n@@ -1 +1,2 @@\n+new\n";

    let mock = mock_mr_diff_server(noop_patch, {
        let patch = patch.to_string();
        move |_pid, _did| (StatusCode::OK, patch.clone().into_bytes())
    })
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![Some(ColumnValue::String(patch.into()))]);
}

#[tokio::test]
async fn raw_patch_deduplicates_same_snapshot() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let counter = call_count.clone();

    let mock = mock_mr_diff_server(noop_patch, move |_pid, _did| {
        counter.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, b"@@ shared".to_vec())
    })
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = raw_patch_row(1, 42);
    let r2 = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(results[0], results[1]);
}

#[tokio::test]
async fn raw_patch_different_snapshots() {
    let mock = mock_mr_diff_server(noop_patch, |_pid, did| {
        (StatusCode::OK, format!("@@ diff-{did}").into_bytes())
    })
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let r1 = raw_patch_row(1, 10);
    let r2 = raw_patch_row(1, 20);
    let rows: Vec<&PropertyRow> = vec![&r1, &r2];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results[0], Some(ColumnValue::String("@@ diff-10".into())));
    assert_eq!(results[1], Some(ColumnValue::String("@@ diff-20".into())));
}

#[tokio::test]
async fn raw_patch_gitlab_500_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, |_pid, _did| {
        (StatusCode::INTERNAL_SERVER_ERROR, Vec::new())
    })
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn raw_patch_empty_body_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, |_pid, _did| (StatusCode::OK, Vec::new())).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn raw_patch_exceeds_size_cap_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, |_pid, _did| {
        (StatusCode::OK, vec![b'x'; 5_000_001])
    })
    .await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn raw_patch_invalid_utf8_returns_none() {
    let mock =
        mock_mr_diff_server(noop_patch, |_pid, _did| (StatusCode::OK, vec![0xFF, 0xFE])).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = raw_patch_row(1, 42);
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn raw_patch_missing_id_returns_none() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let mut row = PropertyRow::new();
    row.insert("project_id".into(), ColumnValue::Int64(1));
    let rows: Vec<&PropertyRow> = vec![&row];

    let results = svc
        .resolve_batch("raw_patch", &rows, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![None]);
}

#[tokio::test]
async fn unknown_lookup_returns_error() {
    let mock = mock_mr_diff_server(noop_patch, noop_raw).await;

    let svc = MergeRequestDiffContentService::new(mock.client.clone());
    let row = patch_row(1, 42, "a.rs", "a.rs");
    let rows: Vec<&PropertyRow> = vec![&row];

    let err = svc
        .resolve_batch("bogus", &rows, &resolver_ctx())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("unknown lookup"));
}
