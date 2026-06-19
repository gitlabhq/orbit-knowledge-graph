//! End-to-end archive path: serve a tar over a mock GitLab archive endpoint,
//! stream it through `gkg_utils::archive::extract_tar_gz` + the production
//! `CodeFilter`, and run `Pipeline::run`. Verifies resolver inputs survive the
//! round trip and resolution works, and that excluded entries are recorded but
//! not materialized or parsed.

use std::path::Path;
use std::sync::{Arc, Mutex};

use code_graph::v2::config::{CodeFilter, detect_language_from_path};
use code_graph::v2::linker::CodeGraph;
use code_graph::v2::linker::graph::GraphNode;
use code_graph::v2::types::EdgeKind;
use code_graph::v2::{
    BatchSink, FileInventoryEntry, GraphConverter, NullSink, Pipeline, PipelineConfig, SinkError,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use gkg_utils::archive::extract_tar_gz;
use std::io::Write;

enum Entry<'a> {
    File(&'a str, &'a [u8]),
}

fn build_archive(entries: &[Entry]) -> Vec<u8> {
    let mut tb = tar::Builder::new(Vec::new());
    for entry in entries {
        match entry {
            Entry::File(path, content) => {
                let mut h = tar::Header::new_gnu();
                h.set_size(content.len() as u64);
                h.set_mode(0o644);
                h.set_cksum();
                tb.append_data(&mut h, path, *content).unwrap();
            }
        }
    }
    let tar_bytes = tb.into_inner().unwrap();
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(&tar_bytes).unwrap();
    enc.finish().unwrap()
}

struct CapturingConverter {
    graphs: Mutex<Vec<CodeGraph>>,
}

struct CapturedPipelineRun {
    graphs: Vec<CodeGraph>,
    files_discovered: usize,
    files_indexed: usize,
    files_parsed: usize,
}

impl GraphConverter for CapturingConverter {
    fn convert(
        &self,
        graph: CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, SinkError> {
        self.graphs.lock().unwrap().push(graph);
        Ok(Vec::new())
    }
}

/// Fetch the archive over HTTP and stream it through the production extraction
/// path. Mirrors `LocalRepositoryCache::extract_archive` end to end.
async fn extract_via_archive_endpoint(
    entries: &[Entry<'_>],
    target: &Path,
) -> Vec<FileInventoryEntry> {
    use axum::Router;
    use axum::body::Body;
    use axum::http::header;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use futures::StreamExt;
    use tokio_util::io::SyncIoBridge;

    let archive_bytes = build_archive(entries);
    let app = Router::new().route(
        "/api/v4/internal/orbit/project/{project_id}/repository/archive",
        get(move || {
            let body = archive_bytes.clone();
            async move {
                (
                    [(header::CONTENT_TYPE, "application/x-gzip")],
                    Body::from(body),
                )
                    .into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("http://{addr}/api/v4/internal/orbit/project/42/repository/archive?ref=main");
    let response = reqwest::get(&url).await.unwrap();
    assert!(response.status().is_success(), "fetch failed: {url}");
    let async_reader = tokio_util::io::StreamReader::new(
        response
            .bytes_stream()
            .map(|r| r.map_err(std::io::Error::other)),
    );
    let target = target.to_path_buf();
    let handle = tokio::runtime::Handle::current();
    let inventory = tokio::task::spawn_blocking(move || {
        let bridge = SyncIoBridge::new_with_handle(async_reader, handle);
        extract_tar_gz(
            bridge,
            &target,
            &mut CodeFilter::new(0, 0, detect_language_from_path),
        )
        .unwrap()
    })
    .await
    .unwrap();
    server.abort();
    inventory
}

async fn run_pipeline(root: &Path, file_inventory: Vec<FileInventoryEntry>) -> CapturedPipelineRun {
    let capturer = Arc::new(CapturingConverter {
        graphs: Mutex::new(Vec::new()),
    });
    let capturer_for_pipeline = capturer.clone();
    let root = root.to_path_buf();
    let result = tokio::task::spawn_blocking(move || {
        let sink: Arc<dyn BatchSink> = Arc::new(NullSink);
        Pipeline::run(
            &root,
            Arc::from(file_inventory),
            PipelineConfig::default(),
            capturer_for_pipeline as Arc<dyn GraphConverter>,
            sink,
        )
    })
    .await
    .unwrap();
    assert!(
        result.errors.is_empty(),
        "pipeline errors: {:#?}",
        result.errors
    );
    CapturedPipelineRun {
        graphs: Arc::try_unwrap(capturer)
            .ok()
            .expect("capturer still has outstanding refs")
            .graphs
            .into_inner()
            .unwrap(),
        files_discovered: result.stats.files_discovered,
        files_indexed: result.stats.files_indexed,
        files_parsed: result.stats.files_parsed,
    }
}

fn has_def(graphs: &[CodeGraph], file: &str, name: &str) -> bool {
    graphs.iter().any(|g| {
        g.graph.node_indices().any(|idx| {
            if let GraphNode::Definition { file_path, id } = &g.graph[idx] {
                file_path.ends_with(file) && g.str(g.defs[id.0 as usize].name) == name
            } else {
                false
            }
        })
    })
}

fn edge_count(graphs: &[CodeGraph], kind: EdgeKind) -> usize {
    graphs
        .iter()
        .map(|g| {
            g.graph
                .raw_edges()
                .iter()
                .filter(|e| e.weight.relationship.edge_kind == kind)
                .count()
        })
        .sum()
}

fn file_language(graphs: &[CodeGraph], path: &str) -> Option<&'static str> {
    graphs
        .iter()
        .flat_map(|g| g.files())
        .find_map(|(_, file)| (file.path == path).then(|| file.language_name()))
}

#[tokio::test]
async fn cargo_workspace_resolves_through_archive_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [
        Entry::File(
            "root/Cargo.toml",
            b"[workspace]\nmembers = [\"crates/lib\", \"crates/app\"]\nresolver = \"2\"\n",
        ),
        Entry::File(
            "root/crates/lib/Cargo.toml",
            b"[package]\nname = \"lib\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        ),
        Entry::File(
            "root/crates/lib/src/lib.rs",
            b"pub fn greet() -> &'static str { \"hi\" }\n",
        ),
        Entry::File(
            "root/crates/app/Cargo.toml",
            b"[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nlib = { path = \"../lib\" }\n",
        ),
        Entry::File("root/crates/app/src/main.rs", b"fn main() { lib::greet(); }\n"),
        Entry::File("root/assets/logo.png", b"\x89PNG"),
        Entry::File("root/dist/build.zip", b"PK"),
    ];
    let file_inventory = extract_via_archive_endpoint(&entries, dir.path()).await;

    assert!(dir.path().join("Cargo.toml").exists());
    assert!(dir.path().join("crates/lib/Cargo.toml").exists());
    assert!(dir.path().join("crates/app/Cargo.toml").exists());
    assert!(!dir.path().join("assets/logo.png").exists());
    assert!(!dir.path().join("dist/build.zip").exists());

    let run = run_pipeline(dir.path(), file_inventory).await;
    assert!(
        has_def(&run.graphs, "crates/lib/src/lib.rs", "greet"),
        "Rust workspace resolver missed lib::greet"
    );
    assert!(
        edge_count(&run.graphs, EdgeKind::Calls) > 0,
        "no Calls edges emitted; cross-crate resolution likely failed"
    );
}

#[tokio::test]
async fn excluded_archive_entries_are_not_materialized_or_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [
        Entry::File("root/src/app.ts", b"export function run() { return 1; }\n"),
        Entry::File("root/assets/logo.png", b"\x89PNG"),
        Entry::File("root/dist/build.zip", b"PK"),
    ];
    let file_inventory = extract_via_archive_endpoint(&entries, dir.path()).await;
    let inventory_paths: Vec<_> = file_inventory.iter().map(|e| e.path.as_str()).collect();
    assert!(inventory_paths.contains(&"src/app.ts"));
    assert!(inventory_paths.contains(&"assets/logo.png"));
    assert!(inventory_paths.contains(&"dist/build.zip"));

    assert!(dir.path().join("src/app.ts").exists());
    assert!(!dir.path().join("assets/logo.png").exists());
    assert!(!dir.path().join("dist/build.zip").exists());

    let run = run_pipeline(dir.path(), file_inventory).await;
    assert_eq!(run.files_discovered, 3);
    assert_eq!(run.files_indexed, 3);
    assert_eq!(run.files_parsed, 1);
    assert_eq!(
        file_language(&run.graphs, "assets/logo.png"),
        Some("unknown")
    );
    assert!(
        has_def(&run.graphs, "src/app.ts", "run"),
        "materialized source file should still be parsed"
    );
}

#[tokio::test]
async fn js_tsconfig_alias_resolves_through_archive_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [
        Entry::File(
            "root/package.json",
            b"{\"name\":\"frontend\",\"version\":\"0.0.0\"}\n",
        ),
        Entry::File(
            "root/tsconfig.json",
            b"{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"src/*\"]}}}\n",
        ),
        Entry::File(
            "root/src/utils.ts",
            b"export function helper() { return 42; }\n",
        ),
        Entry::File(
            "root/src/main.ts",
            b"import { helper } from '@/utils';\nexport function run() { return helper(); }\n",
        ),
        Entry::File("root/static/banner.gif", b"GIF89a"),
        Entry::File("root/fonts/Inter.woff2", b""),
    ];
    let file_inventory = extract_via_archive_endpoint(&entries, dir.path()).await;

    assert!(dir.path().join("package.json").exists());
    assert!(dir.path().join("tsconfig.json").exists());
    assert!(!dir.path().join("static/banner.gif").exists());
    assert!(!dir.path().join("fonts/Inter.woff2").exists());

    let run = run_pipeline(dir.path(), file_inventory).await;
    assert!(
        has_def(&run.graphs, "src/utils.ts", "helper"),
        "JS resolver missed utils::helper"
    );
    assert!(
        edge_count(&run.graphs, EdgeKind::Imports) > 0,
        "no Imports edges emitted; tsconfig alias likely failed to resolve"
    );
}
