//! Runs every corpus, named, and Markdown query through the webserver pipeline stages.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use crate::common::{
    DummyClaims, GRAPH_SCHEMA_SQL, TestContext, derive_clickhouse_data_model, load_ontology,
};
use compiler::{Frontend, parse_input};
use comrak::nodes::{NodeCodeBlock, NodeValue};
use comrak::{Arena, Options, parse_document};
use integration_testkit::load_seed;
use ontology::introspection::SchemaResponse;
use orbit_server::auth::Claims;
use orbit_server::pipeline::{
    ClickHouseExecutor, HydrationStage, RedactionStage, RoutingOutput, RoutingStage, SecurityStage,
};
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
const DOC_SKIPPED_PATHS: &[&str] = &["docs-locale", "CHANGELOG.md"];
const DOC_QUERY_MARKER: &str = "orbit-query";
const DOC_QUERY_FENCE: &str = "opencypher";

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

impl SmokeCase {
    fn runs(key: String, query: String, frontend: Frontend) -> Self {
        Self {
            key,
            query,
            expects_error: false,
            frontend,
        }
    }
}

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
            .remove::<ExtractionOutput>()
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
            query_result: input.query_result,
            authorizations,
        })
    }
}

const PIPELINE_STAGES: [&str; 9] = [
    "routing",
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
    let spellings = [
        (named_queries::Language::Json, Frontend::JsonDsl),
        (named_queries::Language::Gql, Frontend::Gql),
    ];
    queries
        .iter()
        .flat_map(|query| {
            spellings.map(|(language, frontend)| {
                SmokeCase::runs(
                    format!("named_query::{}::{language:?}", query.name),
                    query
                        .render_language(language, &values, &query.example_parameters())
                        .unwrap_or_else(|e| panic!("render named query `{}`: {e}", query.name)),
                    frontend,
                )
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
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.md",
        ])
        .output()
        .unwrap_or_else(|e| panic!("run git ls-files in {}: {e}", root.display()));
    assert!(
        output.status.success(),
        "git ls-files in {} failed: {}",
        root.display(),
        String::from_utf8_lossy(&output.stderr)
    );

    let mut paths: Vec<PathBuf> = String::from_utf8(output.stdout)
        .expect("git ls-files prints UTF-8 paths")
        .split('\0')
        .filter(|rel| !rel.is_empty())
        .filter(|rel| {
            !DOC_SKIPPED_PATHS.iter().any(|skipped| {
                rel.strip_prefix(skipped)
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            })
        })
        .map(|rel| root.join(rel))
        .filter(|path| path.is_file())
        .collect();

    assert!(
        !paths.is_empty(),
        "no Markdown files found in {}",
        root.display()
    );
    paths.sort();
    paths.dedup();
    paths
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

    collect_html_doc_queries(&path_label, text, cases, failures);
}

fn collect_html_doc_queries(
    path_label: &str,
    text: &str,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    static HTML_CODE_START: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r#"<code class="language-([\w-]+)">"#).unwrap());
    static HTML_CODE_BODY: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^([^<]*)</code>").unwrap());

    for start in HTML_CODE_START.captures_iter(text) {
        let tag = start.get(0).unwrap();
        let line = text[..tag.start()].matches('\n').count() + 1;
        let language = start[1].to_ascii_lowercase();
        match HTML_CODE_BODY.captures(&text[tag.end()..]) {
            Some(body) => push_language_doc_query(
                path_label,
                line,
                &language,
                &unescape_html(&body[1]),
                cases,
                failures,
            ),
            None if is_query_language(&language) => failures.push(format!(
                "{path_label}:{line}: cannot read the HTML query code; write `<` as `&lt;`"
            )),
            None => {}
        }
    }
}

fn is_query_language(language: &str) -> bool {
    matches!(language, DOC_QUERY_FENCE | "gql" | "cypher")
}

fn unescape_html(text: &str) -> String {
    text.replace("&#10;", "\n")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn push_language_doc_query(
    path_label: &str,
    line: usize,
    language: &str,
    body: &str,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    match (language, body.trim()) {
        (DOC_QUERY_FENCE, "") => failures.push(format!(
            "{path_label}:{line}: `{DOC_QUERY_FENCE}` code is empty"
        )),
        (DOC_QUERY_FENCE, query) => cases.push(SmokeCase::runs(
            format!("{path_label}:{line}"),
            query.to_string(),
            Frontend::Gql,
        )),
        ("gql" | "cypher", _) => failures.push(format!(
            "{path_label}:{line}: use `{DOC_QUERY_FENCE}`, not `{language}`, for Orbit query code"
        )),
        _ => {}
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
        && (marked
            || matches!(first_token, Some("json" | "bash" | "sh" | "shell"))
            || first_token.is_some_and(is_query_language))
    {
        failures.push(format!("{path_label}:{line}: unclosed Markdown code fence"));
        return;
    }

    match (first_token, marked) {
        (Some(language), false) if is_query_language(language) => {
            push_language_doc_query(path_label, line, language, &block.literal, cases, failures)
        }
        (Some("json"), true) => match normalize_doc_query(&block.literal) {
            Ok(query) => cases.push(SmokeCase::runs(
                format!("{path_label}:{line}"),
                query,
                Frontend::JsonDsl,
            )),
            Err(e) => failures.push(format!("{path_label}:{line}: {e}")),
        },
        (_, true) => failures.push(format!(
            "{path_label}:{line}: `{DOC_QUERY_MARKER}` marks only `json` fences; a `{DOC_QUERY_FENCE}` fence always runs"
        )),
        (Some("json"), false) => {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&block.literal)
                && orbit_query_value(&value).is_some()
            {
                failures.push(format!(
                    "{path_label}:{line}: Orbit query JSON fence must use `json {DOC_QUERY_MARKER}`"
                ));
            }
        }
        (Some("bash" | "sh" | "shell"), false) => {
            if block.literal.contains("\"query_type\"") {
                failures.push(format!(
                    "{path_label}:{line}: shell fence contains Orbit query JSON; move the query body to a `json {DOC_QUERY_MARKER}` fence"
                ));
            }
            push_shell_doc_queries(path_label, line, &block.literal, cases, failures);
        }
        _ => {}
    }
}

fn push_shell_doc_queries(
    path_label: &str,
    line: usize,
    body: &str,
    cases: &mut Vec<SmokeCase>,
    failures: &mut Vec<String>,
) {
    const QUERY_KEYWORDS: &str = "MATCH|CALL";
    static COMMAND: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\borbit\s+query\b(.*)").unwrap());
    static QUERY_WORD: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(&format!(r"(?i)\b({QUERY_KEYWORDS})\b")).unwrap());
    static QUERY_START: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(&format!(r"(?i)^\s*({QUERY_KEYWORDS})\b")).unwrap());
    static QUOTED: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r#"'([^']*)'|"([^"]*)""#).unwrap());
    static PLACEHOLDER: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i)<[a-z][\w./-]*>").unwrap());

    for (offset, command_line) in shell_command_lines(body) {
        let Some(args) = COMMAND
            .captures(&command_line)
            .map(|caps| caps.get(1).unwrap())
        else {
            continue;
        };
        let args = args.as_str();
        if !QUERY_WORD.is_match(args) || PLACEHOLDER.is_match(args) {
            continue;
        }
        let key = format!("{path_label}:{}", line + 1 + offset);
        let query = QUOTED
            .captures_iter(args)
            .filter_map(|caps| caps.get(1).or_else(|| caps.get(2)))
            .map(|m| m.as_str())
            .find(|quoted| QUERY_START.is_match(quoted));
        match query {
            Some(query) => cases.push(SmokeCase::runs(key, query.to_string(), Frontend::Gql)),
            None => failures.push(format!(
                "{key}: cannot read the query on this `orbit query` line; put the whole query in one pair of quotes"
            )),
        }
    }
}

fn shell_command_lines(body: &str) -> Vec<(usize, String)> {
    let mut commands: Vec<(usize, String)> = Vec::new();
    let mut continued = false;
    for (offset, text) in body.lines().enumerate() {
        let (text, continues) = match text.strip_suffix('\\') {
            Some(head) => (head, true),
            None => (text, false),
        };
        match commands.last_mut() {
            Some((_, command)) if continued => command.push_str(text),
            _ => commands.push((offset, text.to_string())),
        }
        continued = continues;
    }
    commands
}

fn normalize_doc_query(body: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid Orbit query JSON: {e}"))?;
    let query = orbit_query_value(&value)
        .ok_or_else(|| "marked fence does not contain an Orbit query".to_string())?;
    let json =
        serde_json::to_string(query).map_err(|e| format!("serialize Orbit query JSON: {e}"))?;
    parse_input(&json).map_err(|e| format!("invalid Orbit query JSON: {e}"))?;
    Ok(json)
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
    json: &str,
    frontend: Frontend,
    (data_model, ontology): (
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    claims: &Claims,
) -> (Vec<&'static str>, Result<SmokeOutput, PipelineError>) {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::new(db.create_client()));
    server_extensions.insert(Arc::clone(data_model));
    server_extensions.insert(claims.clone());
    let mut registry = ColumnResolverRegistry::new();
    registry.register("gitaly", Arc::new(MockColumnResolver));
    server_extensions.insert(registry);

    let mut ctx = QueryPipelineContext {
        frontend,
        query_json: json.to_string(),
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

enum SmokeOutput {
    Query(Box<PipelineOutput>),
    Schema(SchemaResponse),
}

fn expected_stages(output: &SmokeOutput) -> &'static [&'static str] {
    match output {
        SmokeOutput::Query(_) => &PIPELINE_STAGES,
        SmokeOutput::Schema(_) => &PIPELINE_STAGES[..1],
    }
}

async fn run_stages(ctx: &mut QueryPipelineContext) -> Result<SmokeOutput, PipelineError> {
    let [
        routing,
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
    let route = PipelineRunner::start(ctx, &mut obs)
        .then(&Traced(routing, &RoutingStage))
        .await?
        .finish()
        .ok_or_else(|| PipelineError::custom("RoutingStage did not produce RoutingOutput"))?;
    if let RoutingOutput::Schema(response) = route {
        return Ok(SmokeOutput::Schema(response));
    }

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
        .map(|output| SmokeOutput::Query(Box::new(output)))
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
            (false, Ok(ref output)) if stages != expected_stages(output) => failures.push(format!(
                "{}: ran {ran}; expected {}",
                case.key,
                expected_stages(output).join(" > ")
            )),
            (false, Ok(SmokeOutput::Schema(response))) => eprintln!(
                "corpus_smoke: {} ({:?} schema): {ran}; {} domains, {} edges",
                case.key,
                case.frontend,
                response.domains.len(),
                response.edges.len()
            ),
            (false, Ok(SmokeOutput::Query(output))) => eprintln!(
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
        "corpus_smoke: {corpus_total} corpus queries, {named_total} named queries, and {doc_total} docs queries ran as expected"
    );
}
