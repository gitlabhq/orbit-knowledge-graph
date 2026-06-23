//! Corpus smoke test.
//!
//! Runs every query in `fixtures/queries/corpus/` through the **same pipeline
//! stages the webserver runs** (`QueryPipelineService::run_query`): Security ->
//! PathResolution -> Compilation -> ClickHouseExecutor -> Extraction ->
//! Authorization -> Redaction -> Hydration -> Output, against a real ClickHouse
//! seeded with the data-correctness fixture.
//!
//! The only substitution is the Authorization stage: the real one performs the
//! GitLab-Rails authorization handshake over the gRPC stream, which has no
//! in-memory equivalent, so it is replaced by an authorize-everything stage
//! (the same thing every other server integration test mocks). Every other
//! stage is the production stage, in the production order.
//!
//! This asserts the queries run end to end (compiled SQL + hydration plan are
//! valid against the current schema), not result correctness. `expect: error`
//! entries are deliberate invalid controls and must fail.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::{DummyClaims, GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology};
use gkg_server::auth::Claims;
use gkg_server::pipeline::{
    ClickHouseExecutor, HydrationStage, PathResolutionStage, RedactionStage, SecurityStage,
};
use gkg_server::redaction::ResourceAuthorization;
use integration_testkit::load_seed;
use ontology::Ontology;
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use query_engine::shared::content::{ColumnResolver, ColumnResolverRegistry};
use query_engine::shared::{
    AuthorizationOutput, CompilationStage, ExtractionOutput, ExtractionStage, OutputStage,
};
use serde::Deserialize;

const CORPUS_DIR: &str = concat!(env!("FIXTURES_DIR"), "/queries/corpus");
const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const DOC_MARKDOWN_DIRS: &[&str] = &[
    "docs/source",
    "docs/design-documents",
    "skills/orbit",
    "crates/integration-testkit",
];
const DOC_QUERY_MARKER: &str = "orbit-query";

#[derive(Deserialize)]
struct Entry {
    query: String,
    #[serde(default)]
    expect: Option<String>,
}

struct SmokeCase {
    key: String,
    query: String,
    expects_error: bool,
}

/// Stand-in for the real `AuthorizationStage`, which authorizes resources via
/// the GitLab-Rails handshake over the gRPC stream. Authorizes everything so
/// the rest of the pipeline (redaction, hydration) runs on whatever rows the
/// query returned.
struct AuthorizeAllStage;

impl PipelineStage for AuthorizeAllStage {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx
            .phases
            .get::<ExtractionOutput>()
            .ok_or_else(|| PipelineError::custom("ExtractionOutput not found in phases"))?;
        let authorizations = input
            .query_result
            .resource_checks()
            .iter()
            .map(|c| ResourceAuthorization {
                resource_type: c.resource_type.clone(),
                authorized: c
                    .ids
                    .iter()
                    .map(|id| (*id, true))
                    .collect::<HashMap<i64, bool>>(),
            })
            .collect();
        Ok(AuthorizationOutput {
            query_result: input.query_result.clone(),
            authorizations,
        })
    }
}

/// Resolves any content lookup to a mock value so hydration of gitaly-backed
/// virtual columns doesn't need a real Gitaly.
struct MockColumnResolver;

#[async_trait::async_trait]
impl ColumnResolver for MockColumnResolver {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&query_engine::shared::content::PropertyRow],
        _ctx: &query_engine::shared::content::ResolverContext,
    ) -> Result<Vec<Option<gkg_utils::arrow::ColumnValue>>, PipelineError> {
        Ok(rows
            .iter()
            .map(|_| {
                Some(gkg_utils::arrow::ColumnValue::String(format!(
                    "mock:{lookup}"
                )))
            })
            .collect())
    }
}

/// Replace the corpus' run-time placeholders with dummy values so the query
/// compiles and executes. Correctness is irrelevant here.
fn resolve_placeholders(query: &str) -> serde_json::Result<String> {
    // `{{TOKEN}}` -> 1. Works for both bare-numeric (`[{{ID}}]` -> `[1]`) and
    // quoted-string (`"{{PATH}}"` -> `"1"`) placeholders.
    let token_re = regex::Regex::new(r"\{\{[^}]+\}\}").unwrap();
    let s = token_re.replace_all(query, "1");

    let mut value: serde_json::Value = serde_json::from_str(&s)?;
    // A query has either `nodes` (array) or a singular `node`; resolve both.
    if let Some(nodes) = value.get_mut("nodes").and_then(|n| n.as_array_mut()) {
        for node in nodes.iter_mut() {
            resolve_sample_node_ids(node);
        }
    }
    if let Some(node) = value.get_mut("node") {
        resolve_sample_node_ids(node);
    }
    serde_json::to_string(&value)
}

fn resolve_sample_node_ids(node: &mut serde_json::Value) {
    let Some(obj) = node.as_object_mut() else {
        return;
    };
    let count = match obj.get("node_ids").and_then(|v| v.as_str()) {
        Some("$sample") => 1,
        Some(s) => match s.strip_prefix("$sample:") {
            Some(n) => n.parse().unwrap_or(1),
            None => return,
        },
        None => return,
    };
    let ids: Vec<i64> = (1..=count).collect();
    obj.insert("node_ids".to_string(), serde_json::json!(ids));
}

fn load_corpus() -> Vec<SmokeCase> {
    let mut out = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(CORPUS_DIR)
        .unwrap_or_else(|e| panic!("read corpus dir {CORPUS_DIR}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().is_some_and(|x| x == "yaml")
                // raw_sql_ab.yaml is a raw-SQL A/B suite, not DSL — not compilable here.
                && p.file_name().is_some_and(|n| n != "raw_sql_ab.yaml")
        })
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no corpus YAML files found in {CORPUS_DIR}"
    );

    for path in files {
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path).expect("read corpus file");
        let entries: BTreeMap<String, Entry> =
            serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("parse {file}: {e}"));
        for (key, entry) in entries {
            let expects_error = entry.expect.as_deref() == Some("error");
            out.push(SmokeCase {
                key: format!("{file}::{key}"),
                query: entry.query,
                expects_error,
            });
        }
    }
    out
}

fn load_doc_queries() -> (Vec<SmokeCase>, Vec<String>) {
    let mut cases = Vec::new();
    let mut failures = Vec::new();

    for path in markdown_paths() {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read Markdown file {}: {e}", path.display()));
        collect_doc_queries(&path, &text, &mut cases, &mut failures);
    }

    cases.sort_by(|a, b| a.key.cmp(&b.key));
    (cases, failures)
}

fn markdown_paths() -> Vec<PathBuf> {
    let root = Path::new(REPO_ROOT);
    let mut paths = Vec::new();

    for rel in DOC_MARKDOWN_DIRS {
        collect_markdown_paths(&root.join(rel), &mut paths);
    }

    let root_entries = std::fs::read_dir(root)
        .unwrap_or_else(|e| panic!("read repository root {}: {e}", root.display()));
    for entry in root_entries {
        let path = entry.expect("read repository root entry").path();
        if path.extension().is_some_and(|ext| ext == "md") {
            paths.push(path);
        }
    }

    paths.sort();
    paths
}

fn collect_markdown_paths(dir: &Path, paths: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read docs dir {}: {e}", dir.display()));

    for entry in entries {
        let path = entry.expect("read docs dir entry").path();
        if path.is_dir() {
            collect_markdown_paths(&path, paths);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            paths.push(path);
        }
    }
}

fn collect_doc_queries(
    path: &Path,
    text: &str,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    let mut fence: Option<OpenFence> = None;
    let path_label = relative_path(path);

    for (idx, line) in text.lines().enumerate() {
        let line_number = idx + 1;
        if let Some(open) = fence.as_mut() {
            if let Some((ch, len, _)) = fence_info(line)
                && ch == open.ch
                && len >= open.len
            {
                let closed = fence.take().expect("open fence");
                if closed.track {
                    handle_doc_fence(&path_label, closed, cases, failures);
                }
                continue;
            }
            open.body.push_str(line);
            open.body.push('\n');
        } else if let Some((ch, len, info)) = fence_info(line) {
            fence = Some(OpenFence {
                ch,
                len,
                info: info.to_string(),
                start_line: line_number,
                track: should_track_fence(info),
                body: String::new(),
            });
        }
    }

    if let Some(open) = fence.filter(|open| open.track) {
        failures.push(format!(
            "{}:{}: unclosed Markdown code fence",
            path_label, open.start_line
        ));
    }
}

struct OpenFence {
    ch: char,
    len: usize,
    info: String,
    start_line: usize,
    track: bool,
    body: String,
}

fn fence_info(line: &str) -> Option<(char, usize, &str)> {
    let trimmed = line.trim_start();
    let ch = trimmed.chars().next()?;
    if ch != '`' && ch != '~' {
        return None;
    }

    let len = trimmed.chars().take_while(|c| *c == ch).count();
    if len < 3 {
        return None;
    }

    Some((ch, len, trimmed[len..].trim()))
}

fn should_track_fence(info: &str) -> bool {
    let tokens: Vec<_> = info
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect();
    if tokens.iter().any(|token| token == DOC_QUERY_MARKER) {
        return true;
    }

    matches!(
        tokens.first().map(String::as_str),
        Some("json" | "rust" | "bash" | "sh" | "shell")
    )
}

fn handle_doc_fence(
    path_label: &str,
    fence: OpenFence,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    let tokens: Vec<_> = fence
        .info
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect();
    let first_token = tokens.first().map(String::as_str);
    let marked = tokens.iter().any(|token| token == DOC_QUERY_MARKER);

    if marked {
        match normalize_marked_doc_queries(&fence) {
            Ok(queries) => {
                for (idx, query) in queries.into_iter().enumerate() {
                    cases.push(SmokeCase {
                        key: doc_case_key(path_label, fence.start_line, idx),
                        query,
                        expects_error: false,
                    });
                }
            }
            Err(e) => failures.push(format!("{}:{}: {e}", path_label, fence.start_line)),
        }
        return;
    }

    match first_token {
        Some("json") => {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&fence.body)
                && orbit_query_value(&value).is_some()
            {
                failures.push(format!(
                    "{}:{}: Orbit query JSON fence must use `json {DOC_QUERY_MARKER}`",
                    path_label, fence.start_line
                ));
            }
        }
        Some("bash" | "sh" | "shell") if fence.body.contains("\"query_type\"") => {
            failures.push(format!(
                "{}:{}: shell fence contains Orbit query JSON; move the query body to a `json {DOC_QUERY_MARKER}` fence",
                path_label, fence.start_line
            ));
        }
        _ => {}
    }
}

fn doc_case_key(path_label: &str, line: usize, idx: usize) -> String {
    if idx == 0 {
        format!("{path_label}:{line}")
    } else {
        format!("{path_label}:{line}#{idx}")
    }
}

fn normalize_marked_doc_queries(fence: &OpenFence) -> Result<Vec<String>, String> {
    let first_token = fence.info.split_whitespace().next();
    match first_token {
        Some("json") => normalize_doc_query(&fence.body).map(|query| vec![query]),
        Some("rust") => normalize_rust_doc_queries(&fence.body),
        _ => Err(format!(
            "`{DOC_QUERY_MARKER}` fences must use `json` or `rust` as the first info token"
        )),
    }
}

fn normalize_doc_query(body: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid Orbit query JSON: {e}"))?;
    let query = orbit_query_value(&value)
        .ok_or_else(|| "marked fence does not contain an Orbit query".to_string())?;
    serde_json::to_string(query).map_err(|e| format!("serialize Orbit query JSON: {e}"))
}

fn normalize_rust_doc_queries(body: &str) -> Result<Vec<String>, String> {
    let mut queries = Vec::new();
    for literal in rust_raw_strings(body) {
        if literal.contains("\"query_type\"") {
            queries.push(normalize_doc_query(literal)?);
        }
    }
    if queries.is_empty() {
        return Err("marked Rust fence does not contain an Orbit query raw string".to_string());
    }
    Ok(queries)
}

fn rust_raw_strings(body: &str) -> Vec<&str> {
    let bytes = body.as_bytes();
    let mut literals = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'r' {
            i += 1;
            continue;
        }

        let mut quote = i + 1;
        while quote < bytes.len() && bytes[quote] == b'#' {
            quote += 1;
        }
        if quote >= bytes.len() || bytes[quote] != b'"' {
            i += 1;
            continue;
        }

        let hashes = quote - i - 1;
        let content_start = quote + 1;
        let mut end = content_start;
        while end < bytes.len() {
            if bytes[end] == b'"'
                && end + 1 + hashes <= bytes.len()
                && bytes[end + 1..end + 1 + hashes]
                    .iter()
                    .all(|byte| *byte == b'#')
            {
                literals.push(&body[content_start..end]);
                i = end + 1 + hashes;
                break;
            }
            end += 1;
        }

        if end >= bytes.len() {
            break;
        }
    }

    literals
}

fn orbit_query_value(value: &serde_json::Value) -> Option<&serde_json::Value> {
    if is_query_shape(value) {
        return Some(value);
    }

    let query = value.get("query")?;
    is_query_shape(query).then_some(query)
}

fn is_query_shape(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    if [
        "format_version",
        "edges",
        "rows",
        "columns",
        "group_columns",
    ]
    .iter()
    .any(|key| obj.contains_key(*key))
    {
        return false;
    }

    match obj.get("query_type").and_then(serde_json::Value::as_str) {
        Some("traversal") => obj.contains_key("node") || obj.contains_key("nodes"),
        Some("aggregation") => obj.contains_key("aggregations"),
        Some("path_finding") => obj.contains_key("path"),
        Some("neighbors") => obj.contains_key("neighbors"),
        _ => false,
    }
}

fn relative_path(path: &Path) -> String {
    path.strip_prefix(Path::new(REPO_ROOT))
        .unwrap_or(path)
        .display()
        .to_string()
}

/// Run one query through the production pipeline stage sequence (authz mocked).
async fn run_pipeline(
    db: &TestContext,
    json: &str,
    ontology: &Arc<Ontology>,
    claims: &Claims,
) -> Result<(), PipelineError> {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::new(db.create_client()));
    server_extensions.insert(claims.clone());
    let mut registry = ColumnResolverRegistry::new();
    registry.register("gitaly", Arc::new(MockColumnResolver));
    server_extensions.insert(registry);

    let mut ctx = QueryPipelineContext {
        query_json: json.to_string(),
        compiled: None,
        ontology: Arc::clone(ontology),
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    };
    let mut obs = NoOpObserver;

    PipelineRunner::start(&mut ctx, &mut obs)
        .then(&SecurityStage)
        .await?
        .then(&PathResolutionStage)
        .await?
        .then(&CompilationStage)
        .await?
        .then(&ClickHouseExecutor)
        .await?
        .then(&ExtractionStage)
        .await?
        .then(&AuthorizeAllStage)
        .await?
        .then(&RedactionStage)
        .await?
        .then(&HydrationStage)
        .await?
        .then(&OutputStage)
        .await?
        .finish()
        .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))?;
    Ok(())
}

#[tokio::test]
async fn corpus_smoke() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_all().await;

    let ontology = Arc::new(load_ontology());
    // Admin claims -> Owner over org root, so access-gated entities are visible
    // and the real SQL runs (not `WHERE false`).
    let claims = Claims::dummy();

    let corpus = load_corpus();
    let corpus_total = corpus.len();
    let (doc_queries, mut failures) = load_doc_queries();
    let doc_total = doc_queries.len();
    let total = corpus_total + doc_total;
    let mut smoke_cases = corpus;
    smoke_cases.extend(doc_queries);

    for case in smoke_cases {
        let json = match resolve_placeholders(&case.query) {
            Ok(j) => j,
            Err(e) => {
                if !case.expects_error {
                    failures.push(format!("{}: invalid query JSON: {e}", case.key));
                }
                continue;
            }
        };

        let outcome = run_pipeline(&ctx, &json, &ontology, &claims).await;

        match (case.expects_error, outcome) {
            (false, Err(e)) => failures.push(format!("{}: {e:?}", case.key)),
            (true, Ok(())) => failures.push(format!(
                "{}: expected a pipeline error (expect: error), but it ran",
                case.key
            )),
            _ => {}
        }
    }

    assert!(
        failures.is_empty(),
        "{}/{} corpus queries did not run as expected:\n  {}",
        failures.len(),
        total,
        failures.join("\n  ")
    );
    eprintln!(
        "corpus_smoke: {corpus_total} corpus queries and {doc_total} docs queries ran clean through the full pipeline"
    );
}
