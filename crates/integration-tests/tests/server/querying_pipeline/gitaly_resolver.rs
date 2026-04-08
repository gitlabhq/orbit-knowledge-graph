//! Integration tests for GitalyContentService::resolve_batch with a mock HTTP server.

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
use gkg_server::content::gitaly::GitalyContentService;
use gkg_server::content::{ColumnResolver, ResolverContext};
use gkg_utils::arrow::ColumnValue;
use prost::Message;
use query_engine::compiler::SecurityContext;
use tokio::net::TcpListener;

type PropertyRow = HashMap<String, ColumnValue>;

fn resolver_ctx() -> ResolverContext {
    ResolverContext {
        security_context: SecurityContext::new(1, vec!["1/".into()]).unwrap(),
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

/// Starts a mock HTTP server that serves list_blobs responses.
/// `handler` maps (project_id, revisions) -> response body bytes.
async fn mock_gitlab_server(
    handler: impl Fn(i64, Vec<String>) -> Vec<u8> + Send + Sync + Clone + 'static,
) -> (Arc<GitlabClient>, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/api/v4/internal/orbit/project/{project_id}/repository/list_blobs",
        post(move |Path(project_id): Path<i64>, body: String| {
            let handler = handler.clone();
            async move {
                let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                let revisions: Vec<String> = parsed
                    .get("revisions")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
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

    let client = GitlabClient::new(gkg_server_config::GitlabClientConfiguration {
        base_url: format!("http://{addr}"),
        signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
        resolve_host: None,
    })
    .unwrap();

    (Arc::new(client), handle)
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
                path: Vec::new(),
            }],
        }));
    }
    buf
}

#[tokio::test]
async fn resolves_single_file() {
    let (client, _handle) =
        mock_gitlab_server(|_project_id, _revisions| single_blob_response("abc", b"fn main() {}"))
            .await;

    let service = GitalyContentService::new(client);
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
    let (client, _handle) = mock_gitlab_server(|_project_id, revisions| {
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

    let service = GitalyContentService::new(client);
    let row1 = file_row(1, "main", "src/a.rs");
    let row2 = file_row(1, "main", "src/b.rs");
    let rows: Vec<&PropertyRow> = vec![&row1, &row2];
    let ctx = resolver_ctx();

    let results = service
        .resolve_batch("blob_content", &rows, &ctx)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0].is_some());
    assert!(results[1].is_some());
}

#[tokio::test]
async fn deduplicates_same_file() {
    let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = call_count.clone();

    let (client, _handle) = mock_gitlab_server(move |_project_id, revisions| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Should only receive 1 unique revision even though 2 rows reference the same file
        assert_eq!(
            revisions.len(),
            1,
            "should deduplicate identical file references"
        );
        single_blob_response("abc", b"shared content")
    })
    .await;

    let service = GitalyContentService::new(client);
    let row1 = file_row(1, "main", "src/lib.rs");
    let row2 = file_row(1, "main", "src/lib.rs"); // same file
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

    let (client, _handle) = mock_gitlab_server(move |project_id, _revisions| {
        tracker.lock().unwrap().push(project_id);
        single_blob_response("abc", b"content")
    })
    .await;

    let service = GitalyContentService::new(client);
    let row1 = file_row(10, "main", "a.rs");
    let row2 = file_row(20, "main", "b.rs");
    let row3 = file_row(10, "main", "c.rs"); // same project as row1
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
    let (client, _handle) = mock_gitlab_server(|_project_id, _revisions| {
        single_blob_response("abc", b"0123456789abcdef")
    })
    .await;

    let service = GitalyContentService::new(client);
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
    let (client, _handle) = mock_gitlab_server(|_project_id, revisions| {
        assert_eq!(revisions.len(), 1, "same file should be deduplicated");
        single_blob_response("abc", b"fn foo() {} fn bar() {}")
    })
    .await;

    let service = GitalyContentService::new(client);
    let row1 = definition_row(1, "main", "src/lib.rs", 0, 11); // "fn foo() {}"
    let row2 = definition_row(1, "main", "src/lib.rs", 12, 23); // "fn bar() {}"
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
    let (client, _handle) = mock_gitlab_server(|_project_id, _revisions| {
        panic!("should not be called when no valid rows exist");
    })
    .await;

    let service = GitalyContentService::new(client);
    let mut row = PropertyRow::new();
    row.insert("branch".into(), ColumnValue::String("main".into()));
    row.insert("path".into(), ColumnValue::String("src/lib.rs".into()));
    // no project_id
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
    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Arc::new(
        GitlabClient::new(gkg_server_config::GitlabClientConfiguration {
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
}

#[tokio::test]
async fn binary_blob_returns_none() {
    let (client, _handle) = mock_gitlab_server(|_project_id, _revisions| {
        // Invalid UTF-8 bytes
        single_blob_response("abc", &[0xFF, 0xFE, 0x00, 0x01])
    })
    .await;

    let service = GitalyContentService::new(client);
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
