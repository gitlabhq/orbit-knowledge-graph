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

#[derive(Deserialize)]
struct Entry {
    query: String,
    #[serde(default)]
    expect: Option<String>,
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

fn load_corpus() -> Vec<(String, Entry)> {
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
            out.push((format!("{file}::{key}"), entry));
        }
    }
    out
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
        query: query_engine::compiler::QueryInput::Json(json.to_string()),
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
    let total = corpus.len();
    let mut failures: Vec<String> = Vec::new();

    for (key, entry) in corpus {
        let expects_error = entry.expect.as_deref() == Some("error");

        let json = match resolve_placeholders(&entry.query) {
            Ok(j) => j,
            Err(e) => {
                if !expects_error {
                    failures.push(format!("{key}: invalid query JSON: {e}"));
                }
                continue;
            }
        };

        let outcome = run_pipeline(&ctx, &json, &ontology, &claims).await;

        match (expects_error, outcome) {
            (false, Err(e)) => failures.push(format!("{key}: {e:?}")),
            (true, Ok(())) => failures.push(format!(
                "{key}: expected a pipeline error (expect: error), but it ran"
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
    eprintln!("corpus_smoke: {total} queries ran clean through the full pipeline");
}
