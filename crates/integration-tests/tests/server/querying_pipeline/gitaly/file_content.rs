use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing::post};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use gitaly_protos::proto::ListBlobsResponse;
use gitaly_protos::proto::list_blobs_response::Blob as BlobChunk;
use gitlab_client::GitlabClient;
use orbit_arrow::ColumnValue;
use orbit_server::content::gitaly::file_content::GitalyContentService;
use prost::Message;
use query_engine::pipeline::SecurityContext;
use query_engine::shared::content::{ColumnResolver, ResolverContext};
use tokio::net::TcpListener;

type PropertyRow = HashMap<String, ColumnValue>;

fn resolver_ctx() -> ResolverContext {
    ResolverContext {
        security_context: Some(SecurityContext::new(1, vec!["1/".into()]).unwrap()),
    }
}

fn encode_frame(response: &ListBlobsResponse) -> Vec<u8> {
    let frame = response.encode_to_vec();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(frame.len() as u32).to_be_bytes());
    buf.extend_from_slice(&frame);
    buf
}

fn file_row(project_id: i64, branch: &str, path: &str) -> PropertyRow {
    let mut props = PropertyRow::new();
    props.insert("project_id".into(), ColumnValue::Int64(project_id));
    props.insert("branch".into(), ColumnValue::String(branch.into()));
    props.insert("path".into(), ColumnValue::String(path.into()));
    props
}

fn definition_row(
    project_id: i64,
    branch: &str,
    file_path: &str,
    start_byte: i64,
    end_byte: i64,
) -> PropertyRow {
    let mut props = PropertyRow::new();
    props.insert("project_id".into(), ColumnValue::Int64(project_id));
    props.insert("branch".into(), ColumnValue::String(branch.into()));
    props.insert("file_path".into(), ColumnValue::String(file_path.into()));
    props.insert("start_byte".into(), ColumnValue::Int64(start_byte));
    props.insert("end_byte".into(), ColumnValue::Int64(end_byte));
    props
}

struct MockServer {
    client: Arc<GitlabClient>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn mock_gitlab_server(
    handler: impl Fn(i64, Vec<String>) -> Vec<u8> + Send + Sync + Clone + 'static,
) -> MockServer {
    let app = Router::new().route(
        "/api/v4/internal/orbit/project/{project_id}/repository/list_blobs",
        post(move |Path(project_id): Path<i64>, body: String| {
            let handler = handler.clone();
            async move {
                let parsed: serde_json::Value =
                    serde_json::from_str(&body).expect("mock received invalid JSON from client");
                let revisions: Vec<String> = parsed["revisions"]
                    .as_array()
                    .expect("missing 'revisions' array in request body")
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                let bytes = handler(project_id, revisions);
                (StatusCode::OK, Body::from(bytes)).into_response()
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = GitlabClient::new(orbit_server_config::GitlabClientConfiguration {
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

fn single_blob_response(oid: &str, content: &[u8]) -> Vec<u8> {
    encode_frame(&ListBlobsResponse {
        blobs: vec![BlobChunk {
            oid: oid.into(),
            size: content.len() as i64,
            data: content.to_vec(),
            path: Vec::new(),
        }],
    })
}

fn multi_blob_response(blobs: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (oid, content) in blobs {
        buf.extend(encode_frame(&ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: oid.clone(),
                size: content.len() as i64,
                data: content.clone(),
                path: oid.split_once(':').map_or("", |(_, p)| p).into(),
            }],
        }));
    }
    buf
}

/// Like Gitaly, lists a blob shared by several paths only once.
fn gitaly_like_response(revisions: &[String], content_of: impl Fn(&str) -> Vec<u8>) -> Vec<u8> {
    let mut seen = std::collections::HashSet::new();
    let mut buf = Vec::new();
    for revision in revisions {
        let path = revision.split_once(':').map_or("", |(_, p)| p);
        let content = content_of(path);
        if !seen.insert(content.clone()) {
            continue;
        }
        buf.extend(encode_frame(&ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: format!("oid-{}", seen.len()),
                size: content.len() as i64,
                data: content,
                path: path.as_bytes().to_vec(),
            }],
        }));
    }
    buf
}

fn content_with_duplicates(path: &str) -> Vec<u8> {
    match path {
        "a/same.txt" | "b/same.txt" => b"identical content\n".to_vec(),
        "pkg/__init__.py" | "tests/__init__.py" => Vec::new(),
        other => format!("// content of {other}\n").into_bytes(),
    }
}

fn duplicate_rows() -> Vec<PropertyRow> {
    let mut paths: Vec<String> = (1..=40).map(|i| format!("src/File{i:02}.java")).collect();
    paths.insert(7, "b/same.txt".into());
    paths.insert(3, "a/same.txt".into());
    paths.push("pkg/__init__.py".into());
    paths.insert(20, "tests/__init__.py".into());
    paths.iter().map(|p| file_row(1, "main", p)).collect()
}

fn assert_each_row_has_its_own_content(rows: &[PropertyRow], results: &[Option<ColumnValue>]) {
    assert_eq!(results.len(), rows.len());
    for (row, result) in rows.iter().zip(results) {
        let Some(ColumnValue::String(path)) = row.get("path") else {
            panic!("row without path");
        };
        let expected = String::from_utf8(content_with_duplicates(path)).unwrap();
        assert_eq!(
            result,
            &Some(ColumnValue::String(expected)),
            "wrong content for {path}"
        );
    }
}

#[tokio::test]
async fn resolves_single_file() {
    let mock =
        mock_gitlab_server(|_project_id, _revisions| single_blob_response("abc", b"fn main() {}"))
            .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row = file_row(1, "main", "src/main.rs");
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Some(ColumnValue::String("fn main() {}".into())));
}

#[tokio::test]
async fn resolves_multiple_files_same_project() {
    let mock = mock_gitlab_server(|_project_id, revisions| {
        let blobs: Vec<(String, Vec<u8>)> = revisions
            .iter()
            .map(|rev| {
                let content = format!("// content of {rev}");
                (rev.clone(), content.into_bytes())
            })
            .collect();
        multi_blob_response(&blobs)
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = file_row(1, "main", "src/a.rs");
    let row2 = file_row(1, "main", "src/b.rs");
    let rows: Vec<&PropertyRow> = vec![&row1, &row2];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        Some(ColumnValue::String("// content of main:src/a.rs".into()))
    );
    assert_eq!(
        results[1],
        Some(ColumnValue::String("// content of main:src/b.rs".into()))
    );
}

#[tokio::test]
async fn deduplicates_same_file() {
    let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = call_count.clone();

    let mock = mock_gitlab_server(move |_project_id, revisions| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            revisions.len(),
            1,
            "should deduplicate identical file references"
        );
        single_blob_response("abc", b"shared content")
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = file_row(1, "main", "src/lib.rs");
    let row2 = file_row(1, "main", "src/lib.rs");
    let rows: Vec<&PropertyRow> = vec![&row1, &row2];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], results[1]);
    assert_eq!(
        results[0],
        Some(ColumnValue::String("shared content".into()))
    );
    assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn groups_by_project_id() {
    let projects_called = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = projects_called.clone();

    let mock = mock_gitlab_server(move |project_id, revisions| {
        tracker.lock().unwrap().push(project_id);
        gitaly_like_response(&revisions, |p| p.as_bytes().to_vec())
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = file_row(10, "main", "a.rs");
    let row2 = file_row(20, "main", "b.rs");
    let row3 = file_row(10, "main", "c.rs");
    let rows: Vec<&PropertyRow> = vec![&row1, &row2, &row3];
    let ctx = resolver_ctx();

    let _ = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    let mut called = projects_called.lock().unwrap().clone();
    called.sort();
    assert_eq!(called, vec![10, 20], "should make one call per project");
}

#[tokio::test]
async fn definition_byte_range_slicing() {
    let mock = mock_gitlab_server(|_project_id, _revisions| {
        single_blob_response("abc", b"0123456789abcdef")
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row = definition_row(1, "main", "src/lib.rs", 4, 10);
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Some(ColumnValue::String("456789".into())));
}

#[tokio::test]
async fn multiple_definitions_in_same_file() {
    let mock = mock_gitlab_server(|_project_id, revisions| {
        assert_eq!(revisions.len(), 1, "same file should be deduplicated");
        single_blob_response("abc", b"fn foo() {} fn bar() {}")
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = definition_row(1, "main", "src/lib.rs", 0, 11);
    let row2 = definition_row(1, "main", "src/lib.rs", 12, 23);
    let rows: Vec<&PropertyRow> = vec![&row1, &row2];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Some(ColumnValue::String("fn foo() {}".into())));
    assert_eq!(results[1], Some(ColumnValue::String("fn bar() {}".into())));
}

#[tokio::test]
async fn missing_project_id_returns_none() {
    let mock = mock_gitlab_server(|_project_id, _revisions| {
        panic!("should not be called when no valid rows exist");
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let mut row = PropertyRow::new();
    row.insert("branch".into(), ColumnValue::String("main".into()));
    row.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], None);
}

#[tokio::test]
async fn gitlab_error_returns_none_gracefully() {
    let app = Router::new().route(
        "/api/v4/internal/orbit/project/{project_id}/repository/list_blobs",
        post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Arc::new(
        GitlabClient::new(orbit_server_config::GitlabClientConfiguration {
            base_url: format!("http://{addr}"),
            signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
            resolve_host: None,
        })
        .unwrap(),
    );

    let service = GitalyContentService::new(client);
    let row = file_row(1, "main", "src/lib.rs");
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0], None,
        "failed list_blobs should result in None, not an error"
    );
    handle.abort();
}

#[tokio::test]
async fn binary_blob_returns_none() {
    let mock = mock_gitlab_server(|_project_id, _revisions| {
        single_blob_response("abc", &[0xFF, 0xFE, 0x00, 0x01])
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row = file_row(1, "main", "image.png");
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], None, "binary blobs should resolve to None");
}

#[tokio::test]
async fn empty_batch_returns_empty() {
    let mock = mock_gitlab_server(|_project_id, _revisions| {
        panic!("should not be called for empty batch");
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let rows: Vec<&PropertyRow> = vec![];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn mixed_valid_and_invalid_rows() {
    let mock =
        mock_gitlab_server(|_project_id, _revisions| single_blob_response("abc", b"resolved"))
            .await;

    let service = GitalyContentService::new(mock.client.clone());
    let valid = file_row(1, "main", "src/lib.rs");
    let mut invalid = PropertyRow::new();
    invalid.insert("branch".into(), ColumnValue::String("main".into()));
    let rows: Vec<&PropertyRow> = vec![&valid, &invalid];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Some(ColumnValue::String("resolved".into())));
    assert_eq!(results[1], None, "invalid row should not block valid rows");
}

#[tokio::test]
async fn prefers_commit_sha_over_branch() {
    let mock = mock_gitlab_server(|_project_id, revisions| {
        assert_eq!(revisions.len(), 1);
        assert!(
            revisions[0].starts_with("abc123:"),
            "should use commit_sha not branch, got: {}",
            revisions[0]
        );
        single_blob_response("abc", b"content")
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let mut row = PropertyRow::new();
    row.insert("project_id".into(), ColumnValue::Int64(1));
    row.insert("branch".into(), ColumnValue::String("main".into()));
    row.insert("commit_sha".into(), ColumnValue::String("abc123".into()));
    row.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
    let rows: Vec<&PropertyRow> = vec![&row];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].is_some());
}

#[tokio::test]
async fn same_path_different_projects_fetched_separately() {
    let projects_called = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = projects_called.clone();

    let mock = mock_gitlab_server(move |project_id, _revisions| {
        tracker.lock().unwrap().push(project_id);
        let content = format!("from project {project_id}");
        single_blob_response("abc", content.as_bytes())
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = file_row(1, "main", "src/lib.rs");
    let row2 = file_row(2, "main", "src/lib.rs");
    let rows: Vec<&PropertyRow> = vec![&row1, &row2];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_ne!(
        results[0], results[1],
        "different projects should yield different content"
    );

    let mut called = projects_called.lock().unwrap().clone();
    called.sort();
    assert_eq!(called, vec![1, 2]);
}

#[tokio::test]
async fn files_with_identical_content_each_get_their_own_content() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = calls.clone();
    let mock = mock_gitlab_server(move |_project_id, revisions| {
        tracker.lock().unwrap().push(revisions.len());
        gitaly_like_response(&revisions, content_with_duplicates)
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let rows = duplicate_rows();
    let row_refs: Vec<&PropertyRow> = rows.iter().collect();

    let results = service
        .resolve_batch("blob_content", &row_refs, &resolver_ctx())
        .await
        .unwrap();

    assert_each_row_has_its_own_content(&rows, &results);
    let mut calls = calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(
        calls,
        vec![2, rows.len()],
        "one batch, then one batch of the files whose blob was listed under another path"
    );
}

#[tokio::test]
async fn files_resolve_by_path_when_blobs_arrive_out_of_order() {
    let mock = mock_gitlab_server(|_project_id, revisions| {
        let mut reversed = revisions.clone();
        reversed.reverse();
        gitaly_like_response(&reversed, content_with_duplicates)
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let rows = duplicate_rows();
    let row_refs: Vec<&PropertyRow> = rows.iter().collect();

    let results = service
        .resolve_batch("blob_content", &row_refs, &resolver_ctx())
        .await
        .unwrap();

    assert_each_row_has_its_own_content(&rows, &results);
}

#[tokio::test]
async fn same_path_at_two_revisions_gets_each_revision_content() {
    let mock = mock_gitlab_server(|_project_id, revisions| {
        gitaly_like_response(&revisions, |_| revisions[0].clone().into_bytes())
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let row1 = file_row(1, "sha1", "lib.rs");
    let row2 = file_row(1, "sha2", "lib.rs");

    let results = service
        .resolve_batch("blob_content", &[&row1, &row2], &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(
        results,
        vec![
            Some(ColumnValue::String("sha1:lib.rs".into())),
            Some(ColumnValue::String("sha2:lib.rs".into())),
        ]
    );
}

#[tokio::test]
async fn three_identical_files_take_one_batch_each() {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracker = calls.clone();
    let mock = mock_gitlab_server(move |_project_id, revisions| {
        tracker.lock().unwrap().push(revisions.len());
        gitaly_like_response(&revisions, |_| b"same".to_vec())
    })
    .await;

    let service = GitalyContentService::new(mock.client.clone());
    let rows: Vec<PropertyRow> = ["a.txt", "b.txt", "c.txt"]
        .iter()
        .map(|p| file_row(1, "main", p))
        .collect();
    let row_refs: Vec<&PropertyRow> = rows.iter().collect();

    let results = service
        .resolve_batch("blob_content", &row_refs, &resolver_ctx())
        .await
        .unwrap();

    assert_eq!(results, vec![Some(ColumnValue::String("same".into())); 3]);
    assert_eq!(*calls.lock().unwrap(), vec![3, 2, 1]);
}
