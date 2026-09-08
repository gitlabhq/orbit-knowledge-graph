//! YAML-driven query pipeline scenarios. Parallels `scenario/` for indexer
//! tests but exercises the full query path: compile, execute, redact,
//! hydrate, paginate, format, assert.

mod format;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use query_engine::compiler::{
    AccessLevel, AuthorizedPath, CompiledQueryContext, QueryLanguage, SecurityContext,
    compile_query,
};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::content::ColumnResolverRegistry;
use query_engine::shared::{PipelineOutput, RedactionOutput};

use crate::context::TestContext;
use crate::mock_redaction::MockRedactionService;
use crate::visitor::{NodeExt, Requirement, ResponseView};
use crate::{SeededColumnResolver, collect_subtest_results, load_ontology};

pub use format::{NodeExpect, QueryExpect, QueryScenario, RedactionConfig, SecurityOverride};

use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;

pub async fn run_dir(ctx: &TestContext, root: &str) {
    let root = Path::new(root);
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no query scenario files found under {}",
        root.display()
    );

    if let Ok(filter) = std::env::var("SCENARIO_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            files.retain(|f| scenario_name(root, f).contains(filter));
            assert!(
                !files.is_empty(),
                "SCENARIO_FILTER='{filter}' matched no query scenarios under {}",
                root.display()
            );
        }
    }

    let concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let ctx = Arc::new(ctx.clone());

    let mut handles = Vec::new();
    for file in files {
        let name = scenario_name(root, &file);
        let semaphore = Arc::clone(&semaphore);
        let ctx = Arc::clone(&ctx);
        let task_name = name.clone();
        let handle = tokio::task::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            run_scenario(&ctx, &file, &task_name).await;
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
}

async fn run_scenario(ctx: &TestContext, file: &Path, name: &str) {
    let raw = std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
    let scenario: QueryScenario = orbit_utils::yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("{name}: invalid scenario: {e}"));

    let security = build_security(&scenario.security);
    let redaction = build_redaction(&scenario.redaction);

    for (frontend_key, query_str) in &scenario.query {
        let Some(language) = QueryLanguage::from_name(frontend_key) else {
            eprintln!("    {name}: skipping unknown query language '{frontend_key}'");
            continue;
        };
        let label = format!("{name} [{frontend_key}]");
        run_frontend(
            ctx,
            language,
            query_str,
            &security,
            &redaction,
            &scenario.expect,
            &label,
        )
        .await;
    }
}

async fn run_frontend(
    ctx: &TestContext,
    language: QueryLanguage,
    query: &str,
    security: &SecurityContext,
    redaction: &MockRedactionService,
    expect: &QueryExpect,
    label: &str,
) {
    let ontology = Arc::new(load_ontology());

    let compiled = match compile_query(query, language, &ontology, security) {
        Ok(c) => {
            assert!(
                expect.compile_error.is_none(),
                "{label}: expected compile error but got success"
            );
            Arc::new(c)
        }
        Err(e) => match &expect.compile_error {
            Some(format::CompileErrorExpect::Flag(true)) => return,
            Some(format::CompileErrorExpect::Substring(sub)) => {
                assert!(
                    e.to_string().contains(sub.as_str()),
                    "{label}: compile error '{e}' does not contain '{sub}'"
                );
                return;
            }
            _ => panic!("{label}: unexpected compile error: {e}"),
        },
    };

    let resp = execute_pipeline(ctx, &compiled, &ontology, security, redaction).await;

    let response: query_engine::formatters::GraphResponse =
        serde_json::from_value(resp).expect("response should deserialize");
    let view = ResponseView::for_query(&compiled.input, response);

    apply_expect(&view, expect, label);
}

async fn execute_pipeline(
    ctx: &TestContext,
    compiled: &Arc<CompiledQueryContext>,
    ontology: &Arc<ontology::Ontology>,
    security: &SecurityContext,
    redaction: &MockRedactionService,
) -> serde_json::Value {
    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let checks = result.resource_checks();
    let authorizations = redaction.check(&checks);
    let redacted_count = result.apply_authorizations(&authorizations);

    let resolver = Arc::new(SeededColumnResolver::from_seed_file());
    let mut resolver_registry = ColumnResolverRegistry::new();
    resolver_registry.register("gitaly", resolver as _);

    let client = Arc::new(ctx.create_client());
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    server_extensions.insert(resolver_registry);

    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::clone(compiled)),
        ontology: Arc::clone(ontology),
        security_context: Some(security.clone()),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(RedactionOutput {
        query_result: result,
        redacted_count,
    });
    let mut obs = NoOpObserver;

    let hydration_output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await
        .expect("hydration should succeed");

    let mut query_result = hydration_output.query_result;
    let pagination = Some(query_engine::shared::paginate(
        &mut query_result,
        &compiled.input,
    ));

    let output = PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydration_output.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(compiled),
        query_result,
        result_context: hydration_output.result_context,
        execution_log: vec![],
        pagination,
    };

    GraphFormatter.format(&output)
}

fn apply_expect(view: &ResponseView, expect: &QueryExpect, label: &str) {
    for req_name in &expect.skip_requirements {
        if let Some(req) = parse_requirement(req_name) {
            view.skip_requirement(req);
        }
    }

    if let Some(n) = expect.node_count {
        view.assert_node_count(n);
    }
    for (entity, ids) in &expect.node_order {
        view.assert_node_order(entity, ids);
    }
    for (entity, ids) in &expect.node_ids {
        view.assert_node_ids(entity, ids);
    }
    for node in &expect.nodes {
        let found = view.find_node(&node.entity, node.id);
        assert!(
            found.is_some(),
            "{label}: node {}/{} not found",
            node.entity,
            node.id
        );
        let found = found.unwrap();
        for (prop, expected) in &node.properties {
            match expected {
                serde_json::Value::String(s) => found.assert_str(prop, s),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        found.assert_i64(prop, i);
                    }
                }
                serde_json::Value::Bool(b) => {
                    assert_eq!(
                        found.prop_bool(prop),
                        Some(*b),
                        "{label}: {}/{}.{prop} expected {b}",
                        node.entity,
                        node.id,
                    );
                }
                serde_json::Value::Null => {
                    assert!(
                        !found.has_prop(prop),
                        "{label}: {}/{}.{prop} expected null but has value",
                        node.entity,
                        node.id,
                    );
                }
                _ => panic!("{label}: unsupported property value type for {prop}"),
            }
        }
    }
    for (entity, ids) in &expect.node_absent {
        for id in ids {
            view.assert_node_absent(entity, *id);
        }
    }
    for (kind, tuples) in &expect.edge_set {
        let pairs: Vec<(i64, i64)> = tuples.iter().map(|[a, b]| (*a, *b)).collect();
        view.assert_edge_set(kind, &pairs);
    }
    for (kind, count) in &expect.edge_count {
        view.assert_edge_count(kind, *count);
    }
    if expect.referential_integrity {
        view.assert_referential_integrity();
    }
    if let Some(expected) = expect.has_more {
        let pagination = view.response.pagination.as_ref();
        assert!(
            pagination.is_some(),
            "{label}: expected pagination but none found"
        );
        assert_eq!(
            pagination.unwrap().has_more,
            expected,
            "{label}: has_more mismatch"
        );
    }
}

fn build_security(overrides: &Option<SecurityOverride>) -> SecurityContext {
    let Some(ov) = overrides else {
        return SecurityContext::new(1, vec!["1/".into()]).unwrap();
    };
    let org = ov.org_id.unwrap_or(1);
    let paths: Vec<String> = ov.paths.clone().unwrap_or_else(|| vec!["1/".into()]);
    let access = ov.access_level.unwrap_or(AccessLevel::Reporter as u32);
    let authorized: Vec<AuthorizedPath> = paths
        .iter()
        .map(|p| AuthorizedPath::new(p.as_str(), access))
        .collect();

    let mut ctx = SecurityContext::new_with_roles(org, authorized).unwrap();
    if let Some(true) = ov.admin {
        ctx = ctx.with_role(true, Some(AccessLevel::Owner as u32));
    }
    ctx
}

fn build_redaction(config: &Option<RedactionConfig>) -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    let Some(config) = config else {
        // Default: allow all seeded entities.
        svc.allow("user", &[1, 2, 3, 4, 5, 6, 7]);
        svc.allow("group", &[100, 101, 102, 200, 300, 900]);
        svc.allow("project", &[1000, 1001, 1002, 1003, 1004, 1010, 9000]);
        svc.allow("merge_request", &[2000, 2001, 2002, 2003, 2004, 2005, 9100]);
        svc.allow("note", &[3000, 3001, 3002, 3003]);
        svc.allow("work_item", &[4000, 4001, 4002, 4003, 4010]);
        svc.allow("milestone", &[6000, 6001]);
        svc.allow("label", &[7000, 7001, 7002]);
        return svc;
    };
    for (resource, ids) in &config.allow {
        svc.allow(resource, ids);
    }
    for (resource, ids) in &config.deny {
        svc.deny(resource, ids);
    }
    svc
}

fn parse_requirement(name: &str) -> Option<Requirement> {
    match name {
        "node_ids" => Some(Requirement::NodeIds),
        "node_count" => Some(Requirement::NodeCount),
        "order_by" => Some(Requirement::OrderBy),
        "cursor" => Some(Requirement::Cursor),
        "aggregation" => Some(Requirement::Aggregation),
        "aggregation_sort" => Some(Requirement::AggregationSort),
        "neighbors" => Some(Requirement::Neighbors),
        "path_finding" => Some(Requirement::PathFinding),
        _ => None,
    }
}

fn discover(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => panic!("failed to read {}: {e}", dir.display()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            discover(&path, files);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            files.push(path);
        }
    }
}

fn scenario_name(root: &Path, file: &Path) -> String {
    let relative = file
        .strip_prefix(root)
        .expect("file is under root")
        .with_extension("");
    let prefix = root
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("{prefix}/{}", relative.display())
}
