//! Corpus smoke test.
//!
//! Runs every GQL query in `fixtures/queries/corpus/` through the **same pipeline
//! stages the webserver runs** (`QueryPipelineService::run_query`): Security ->
//! Compilation -> ClickHouseExecutor -> Extraction ->
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
use std::sync::{Arc, LazyLock};

use crate::common::{
    DummyClaims, GRAPH_SCHEMA_SQL, TestContext, derive_clickhouse_data_model, load_ontology,
};
use compiler::Frontend;
use comrak::nodes::{NodeCodeBlock, NodeValue};
use comrak::{Arena, Options, parse_document};
use integration_testkit::load_seed;
use orbit_server::auth::Claims;
use orbit_server::pipeline::{ClickHouseExecutor, HydrationStage, RedactionStage, SecurityStage};
use orbit_server::redaction::ResourceAuthorization;
use query_engine::formatters::GraphResponse;
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use query_engine::shared::PipelineOutput;
use query_engine::shared::content::{ColumnResolver, ColumnResolverRegistry};
use query_engine::shared::{
    AuthorizationOutput, CompilationStage, ExtractionOutput, ExtractionStage, OutputStage,
};
use serde::Deserialize;

const CORPUS_DIR: &str = concat!(env!("FIXTURES_DIR"), "/queries/corpus");
const NAMED_QUERIES_DIR: &str = env!("NAMED_QUERIES_DIR");
const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const DOC_MARKDOWN_DIRS: &[&str] = &[
    "docs/docs-gitlab-com",
    "docs/pages/engineering/testing",
    "docs/pages/engineering/architecture",
    "docs/pages/engineering/adr",
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
    frontend: Frontend,
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
        let mut input = ctx
            .phases
            .remove::<ExtractionOutput>()
            .ok_or_else(|| PipelineError::custom("ExtractionOutput not found in phases"))?;
        HydrationStage::resolve_owners(ctx, &mut input.query_result).await?;
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
            query_result: input.query_result,
            authorizations,
        })
    }
}

const PIPELINE_STAGES: [&str; 8] = [
    "security",
    "compilation",
    "execution",
    "extraction",
    "authorization",
    "redaction",
    "hydration",
    "output",
];

struct Traced<'a, S>(&'static str, &'a S);

#[derive(Default)]
struct StageLog(Vec<&'static str>);

impl<S: PipelineStage> PipelineStage for Traced<'_, S> {
    type Input = S::Input;
    type Output = S::Output;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let output = self.1.execute(ctx, obs).await?;
        ctx.phases
            .get_or_insert_default::<StageLog>()
            .0
            .push(self.0);
        Ok(output)
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
    ) -> Result<Vec<Option<orbit_arrow::ColumnValue>>, PipelineError> {
        Ok(rows
            .iter()
            .map(|_| Some(orbit_arrow::ColumnValue::String(format!("mock:{lookup}"))))
            .collect())
    }
}

fn resolve_placeholders(query: &str) -> String {
    static TOKEN: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\{\{[^}]+\}\}").unwrap());
    static SAMPLE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i)(\w+)\.id\s+IN\s+\$sample(?::(\d+))?").unwrap());
    let without_tokens = TOKEN.replace_all(query, "1");
    SAMPLE
        .replace_all(&without_tokens, |caps: &regex::Captures<'_>| {
            let count: i64 = caps.get(2).map_or(1, |n| n.as_str().parse().unwrap_or(1));
            let ids: Vec<String> = (1..=count).map(|id| id.to_string()).collect();
            format!("{}.id IN [{}]", &caps[1], ids.join(", "))
        })
        .into_owned()
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
            orbit_utils::yaml::from_str(&text).unwrap_or_else(|e| panic!("parse {file}: {e}"));
        for (key, entry) in entries {
            let expects_error = entry.expect.as_deref() == Some("error");
            out.push(SmokeCase {
                key: format!("{file}::{key}"),
                query: resolve_placeholders(&entry.query),
                expects_error,
                frontend: Frontend::Gql,
            });
        }
    }
    out
}

fn load_named_queries() -> Vec<SmokeCase> {
    let queries =
        named_queries::NamedQueries::load_from_dir(std::path::Path::new(NAMED_QUERIES_DIR))
            .unwrap_or_else(|e| panic!("load named queries from {NAMED_QUERIES_DIR}: {e}"));

    let values = named_queries::BindingValues { current_user_id: 1 };
    let spellings = [(named_queries::Language::Gql, Frontend::Gql)];
    queries
        .iter()
        .flat_map(|query| {
            spellings.map(|(language, frontend)| SmokeCase {
                key: format!("named_query::{}::{language:?}", query.name),
                query: query
                    .render(language, &values, &query.example_parameters())
                    .unwrap_or_else(|e| panic!("render named query `{}`: {e}", query.name)),
                expects_error: false,
                frontend,
            })
        })
        .collect()
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
    let arena = Arena::new();
    let root = parse_document(&arena, text, &Options::default());
    let path_label = relative_path(path);

    for node in root.descendants() {
        let ast = node.data();
        if let NodeValue::CodeBlock(block) = &ast.value {
            handle_doc_code_block(
                &path_label,
                ast.sourcepos.start.line,
                block,
                cases,
                failures,
            );
        }
    }
}

fn handle_doc_code_block(
    path_label: &str,
    line: usize,
    block: &NodeCodeBlock,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    if !block.fenced {
        return;
    }

    let tokens: Vec<_> = block
        .info
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect();
    let first_token = tokens.first().map(String::as_str);
    let marked = tokens.iter().any(|token| token == DOC_QUERY_MARKER);

    if !block.closed
        && (marked || matches!(first_token, Some("json" | "gql" | "bash" | "sh" | "shell")))
    {
        failures.push(format!("{path_label}:{line}: unclosed Markdown code fence"));
        return;
    }

    if marked {
        match marked_doc_query(first_token, &block.literal) {
            Ok((query, frontend)) => {
                cases.push(SmokeCase {
                    key: format!("{path_label}:{line}"),
                    query,
                    expects_error: false,
                    frontend,
                });
            }
            Err(e) => failures.push(format!("{path_label}:{line}: {e}")),
        }
        return;
    }

    match first_token {
        Some("json") => {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&block.literal)
                && orbit_query_value(&value).is_some()
            {
                failures.push(format!(
                    "{path_label}:{line}: the JSON Query DSL was removed; rewrite this query as a `gql {DOC_QUERY_MARKER}` fence"
                ));
            }
        }
        Some("bash" | "sh" | "shell") if block.literal.contains("\"query_type\"") => {
            failures.push(format!(
                "{path_label}:{line}: shell fence contains a JSON Query DSL query, which was removed; use GQL"
            ));
        }
        _ => {}
    }
}

fn marked_doc_query(first_token: Option<&str>, body: &str) -> Result<(String, Frontend), String> {
    let frontend = first_token
        .and_then(|token| token.parse::<Frontend>().ok())
        .ok_or_else(|| {
            format!(
                "`{DOC_QUERY_MARKER}` fences must name a query language ({}) as the first info token",
                Frontend::names().join(", ")
            )
        })?;
    if body.trim().is_empty() {
        return Err("marked fence does not contain an Orbit query".to_string());
    }
    Ok((body.trim().to_string(), frontend))
}

fn orbit_query_value(value: &serde_json::Value) -> Option<&serde_json::Value> {
    let query = value.get("query").unwrap_or(value);
    if is_query_output(query) {
        return None;
    }

    query
        .as_object()
        .is_some_and(|obj| obj.contains_key("query_type"))
        .then_some(query)
}

fn is_query_output(value: &serde_json::Value) -> bool {
    if serde_json::from_value::<GraphResponse>(value.clone()).is_ok() {
        return true;
    }

    value.as_object().is_some_and(|obj| {
        obj.contains_key("result")
            || obj.contains_key("format_version")
            || obj.contains_key("format_name")
    })
}

fn relative_path(path: &Path) -> String {
    path.strip_prefix(Path::new(REPO_ROOT))
        .unwrap_or(path)
        .display()
        .to_string()
}

async fn run_pipeline(
    db: &TestContext,
    query: &str,
    frontend: Frontend,
    (data_model, ontology): (
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    claims: &Claims,
) -> (Vec<&'static str>, Result<PipelineOutput, PipelineError>) {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::new(db.create_client()));
    server_extensions.insert(Arc::clone(data_model));
    server_extensions.insert(claims.clone());
    let mut registry = ColumnResolverRegistry::new();
    registry.register("gitaly", Arc::new(MockColumnResolver));
    server_extensions.insert(registry);

    let mut ctx = QueryPipelineContext {
        frontend,
        query: query.to_string(),
        compiled: None,
        ontology: Arc::clone(ontology),
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    };
    let output = run_stages(&mut ctx).await;
    let stages = ctx.phases.remove::<StageLog>().unwrap_or_default().0;
    (stages, output)
}

async fn run_stages(ctx: &mut QueryPipelineContext) -> Result<PipelineOutput, PipelineError> {
    let [
        security,
        compilation,
        execution,
        extraction,
        authorization,
        redaction,
        hydration,
        output,
    ] = PIPELINE_STAGES;
    let executor = ClickHouseExecutor {
        migration_version: *orbit_migrations::version::SCHEMA_VERSION,
    };
    let mut obs = NoOpObserver;
    PipelineRunner::start(ctx, &mut obs)
        .then(&Traced(security, &SecurityStage))
        .await?
        .then(&Traced(compilation, &CompilationStage))
        .await?
        .then(&Traced(execution, &executor))
        .await?
        .then(&Traced(extraction, &ExtractionStage))
        .await?
        .then(&Traced(authorization, &AuthorizeAllStage))
        .await?
        .then(&Traced(redaction, &RedactionStage))
        .await?
        .then(&Traced(hydration, &HydrationStage))
        .await?
        .then(&Traced(output, &OutputStage))
        .await?
        .finish()
        .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))
}

#[tokio::test]
async fn corpus_smoke() {
    orbit_server_config::query::init(orbit_server_config::AppConfig::embedded_defaults().query);
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_if_needed().await;

    let ontology = load_ontology();
    let data_model = derive_clickhouse_data_model(&ontology);
    // Admin claims -> Owner over org root, so access-gated entities are visible
    // and the real SQL runs (not `WHERE false`).
    let claims = Claims::dummy();

    let corpus = load_corpus();
    let corpus_total = corpus.len();
    let named_queries = load_named_queries();
    let named_total = named_queries.len();
    let (doc_queries, mut failures) = load_doc_queries();
    let doc_total = doc_queries.len();
    let total = corpus_total + named_total + doc_total;
    let mut smoke_cases = corpus;
    smoke_cases.extend(named_queries);
    smoke_cases.extend(doc_queries);

    for case in smoke_cases {
        let (stages, outcome) = run_pipeline(
            &ctx,
            &case.query,
            case.frontend,
            (&data_model, &ontology),
            &claims,
        )
        .await;

        let ran = stages.join(" > ");
        match (case.expects_error, outcome) {
            (false, Ok(_)) if stages != PIPELINE_STAGES => {
                failures.push(format!("{}: ran only {ran}", case.key))
            }
            (false, Ok(output)) => eprintln!(
                "corpus_smoke: {} ({:?} {}): {ran}; {} rows, {} redacted, {} ClickHouse queries",
                case.key,
                case.frontend,
                output.query_type,
                output.row_count,
                output.redacted_count,
                output.execution_log.len()
            ),
            (false, Err(e)) => failures.push(format!("{}: {e:?} after [{ran}]", case.key)),
            (true, Ok(_)) => failures.push(format!(
                "{}: expected a pipeline error (expect: error), but it ran",
                case.key
            )),
            (true, Err(e)) => eprintln!(
                "corpus_smoke: {} rejected as expected after [{ran}]: {e}",
                case.key
            ),
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
        "corpus_smoke: {corpus_total} corpus queries, {named_total} named queries, and {doc_total} docs queries ran clean through the full pipeline"
    );
}
