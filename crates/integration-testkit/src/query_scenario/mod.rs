//! YAML-driven query pipeline scenarios. Parallels `scenario/` for indexer
//! tests but exercises the full query path: compile, execute, redact,
//! hydrate, paginate, format, assert.

mod format;

use std::path::Path;
use std::sync::Arc;

use query_engine::compiler::{
    AccessLevel, AuthorizedPath, CompiledQueryContext, Frontend, SecurityContext, compile,
};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::content::ColumnResolverRegistry;
use query_engine::shared::{PipelineOutput, RedactionOutput};

use crate::context::TestContext;
use crate::mock_redaction::MockRedactionService;
use crate::scenario::{self, Seed};
use crate::visitor::{NodeExt, Requirement, ResponseView};
use crate::{SeededColumnResolver, collect_subtest_results, load_ontology};

pub use format::{
    PresetOr, QueryExpect, QueryScenario, RedactionConfig, ScenarioConfig, SecurityOverride,
};

use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;

/// Parse all YAML scenario files under `root` without executing them.
/// Catches syntax errors and serde mismatches in `cargo nextest --lib`
/// before the integration stage spins up Docker.
pub fn validate_parse(root: &str) {
    let root = Path::new(root);
    let mut files = Vec::new();
    scenario::discover(root, &mut files);
    assert!(!files.is_empty(), "no files found under {}", root.display());
    for file in &files {
        let raw =
            std::fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let _: QueryScenario =
            orbit_utils::yaml::from_str(&raw).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
    }
}

/// Load a named seed preset from `presets/seed.yaml` and apply it.
pub async fn load_yaml_seed(ctx: &TestContext, presets_dir: &str, name: &str) {
    let path = Path::new(presets_dir).join("seed.yaml");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed presets at {}: {e}", path.display()));
    let map: std::collections::BTreeMap<String, Seed> =
        orbit_utils::yaml::from_str(&raw).unwrap_or_else(|e| panic!("invalid seed presets: {e}"));
    let seed = map
        .get(name)
        .unwrap_or_else(|| panic!("unknown seed preset '{name}'"));
    let columns = crate::scenario::seed::fetch_table_columns(ctx).await;
    crate::scenario::seed::apply_seed(ctx, seed, &Default::default(), &columns, name).await;
}

pub async fn run_dir(ctx: &TestContext, root: &str, presets: &str) {
    let root = Path::new(root);
    let presets: Arc<Path> = Arc::from(Path::new(presets));
    let mut files = Vec::new();
    scenario::discover(root, &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no query scenario files found under {}",
        root.display()
    );

    if let Ok(filter) = std::env::var("SCENARIO_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            files.retain(|f| scenario::scenario_name(root, f).contains(filter));
            assert!(
                !files.is_empty(),
                "SCENARIO_FILTER='{filter}' matched no query scenarios under {}",
                root.display()
            );
        }
    }

    scenario::assert_distinct_database_names(root, &files);

    let concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let ctx = Arc::new(ctx.clone());

    let mut handles = Vec::new();
    for file in files {
        let name = scenario::scenario_name(root, &file);
        let semaphore = Arc::clone(&semaphore);
        let ctx = Arc::clone(&ctx);
        let presets = Arc::clone(&presets);
        let task_name = name.clone();
        let handle = tokio::task::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            run_scenario(&ctx, &file, &task_name, &presets).await;
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
}

async fn run_scenario(ctx: &TestContext, file: &Path, name: &str, presets: &Path) {
    let raw = std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
    let scenario: QueryScenario = orbit_utils::yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("{name}: invalid scenario: {e}"));

    let cfg = &scenario.config;

    let needs_fork = !cfg.extra_seed.is_empty();
    let ctx = if needs_fork {
        let db_name = scenario::database_name(name);
        let forked = ctx.fork(&db_name).await;
        let columns = crate::scenario::seed::fetch_table_columns(&forked).await;
        crate::scenario::seed::apply_seed(
            &forked,
            &cfg.extra_seed,
            &Default::default(),
            &columns,
            name,
        )
        .await;
        forked.optimize_all().await;
        forked
    } else {
        ctx.clone()
    };

    let security_override = resolve_preset("security", &cfg.security, presets, name);
    let redaction_with_default = cfg
        .redaction
        .clone()
        .unwrap_or(PresetOr::Preset("allow_all".into()));
    let redaction_config =
        resolve_preset("redaction", &Some(redaction_with_default), presets, name);
    let security = build_security(&security_override);
    let redaction = build_redaction(&redaction_config);

    for (frontend_key, query_str) in &scenario.query {
        let Some(frontend) = Frontend::from_name(frontend_key) else {
            eprintln!("    {name}: skipping unknown query language '{frontend_key}'");
            continue;
        };
        let label = format!("{name} [{frontend_key}]");
        run_frontend(
            &ctx,
            frontend,
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
    frontend: Frontend,
    query: &str,
    security: &SecurityContext,
    redaction: &MockRedactionService,
    expect: &QueryExpect,
    label: &str,
) {
    let ontology = Arc::new(load_ontology());

    let compiled = match compile(query, frontend, &ontology, security) {
        Ok(c) => {
            let expects_error = matches!(
                expect.compile_error,
                Some(format::CompileErrorExpect::Flag(true))
                    | Some(format::CompileErrorExpect::Substring(_))
            );
            assert!(
                !expects_error,
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
    if let Some(n) = expect.node_count.or_else(|| expect.derived_node_count()) {
        view.assert_node_count(n);
    }
    for (entity, ne) in &expect.nodes {
        if let Some(order) = &ne.order {
            view.assert_node_order(entity, order);
        } else if let Some(ids) = &ne.ids {
            view.assert_node_ids(entity, ids);
        }
        if let Some(count) = ne.count {
            assert_eq!(
                view.nodes_of_type(entity).len(),
                count,
                "{label}: {entity} count mismatch"
            );
        }
        for row in &ne.rows {
            let id = row
                .get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("{label}: node {entity} row missing integer 'id'"));
            let found = view
                .find_node(entity, id)
                .unwrap_or_else(|| panic!("{label}: node {entity}/{id} not found"));
            for (prop, expected) in row {
                if prop == "id" {
                    continue;
                }
                assert_property(found, prop, expected, entity, id, label);
            }
        }
        if let Some(absent) = &ne.absent {
            for id in absent {
                view.assert_node_absent(entity, *id);
            }
        }
        for (field, expected) in &ne.filters {
            let expected = expected.clone();
            let field_name = field.clone();
            view.assert_filter(entity, field, move |n| {
                n.prop(&field_name) == Some(&expected)
            });
        }
    }
    for (kind, tuples) in &expect.edges {
        let pairs: Vec<(i64, i64)> = tuples.iter().map(|[a, b]| (*a, *b)).collect();
        view.assert_edge_set(kind, &pairs);
    }
    for (kind, tuples) in &expect.edge_exists {
        for [from_id, to_id] in tuples {
            let edge = view
                .response
                .edges
                .iter()
                .find(|e| e.from_id == *from_id && e.to_id == *to_id && e.edge_type == *kind);
            let edge = edge
                .unwrap_or_else(|| panic!("{label}: expected edge {from_id} --{kind}--> {to_id}"));
            view.assert_edge_exists(&edge.from, *from_id, &edge.to, *to_id, kind);
        }
    }
    for (kind, tuples) in &expect.edge_absent {
        for [from_id, to_id] in tuples {
            let found = view
                .response
                .edges
                .iter()
                .any(|e| e.from_id == *from_id && e.to_id == *to_id && e.edge_type == *kind);
            assert!(
                !found,
                "{label}: unexpected edge {from_id} --{kind}--> {to_id}"
            );
        }
    }
    for (kind, count) in &expect.edge_count {
        view.assert_edge_count(kind, *count);
    }
    for (group_key, ge) in &expect.groups {
        let ids: Vec<i64> = ge.rows.iter().map(|gr| gr.id).collect();
        if !ids.is_empty() {
            let entity = &ge.rows[0].entity;
            view.assert_group_node_ids(group_key, entity, &ids);
        }
        for gr in &ge.rows {
            for (col, expected) in &gr.values {
                match expected {
                    serde_json::Value::Number(n) if n.is_i64() => {
                        view.assert_group_row_value_i64(
                            group_key,
                            &gr.entity,
                            gr.id,
                            col,
                            n.as_i64().unwrap(),
                        );
                    }
                    serde_json::Value::Number(n) if n.is_f64() => {
                        view.assert_group_row_value_f64(
                            group_key,
                            &gr.entity,
                            gr.id,
                            col,
                            n.as_f64().unwrap(),
                        );
                    }
                    serde_json::Value::String(s) => {
                        view.assert_group_row_value_str(group_key, &gr.entity, gr.id, col, s);
                    }
                    _ => panic!("{label}: unsupported group value type for {col}"),
                }
            }
            for (prop, expected) in &gr.properties {
                match expected {
                    serde_json::Value::String(s) => {
                        view.assert_group_node_property_str(group_key, &gr.entity, gr.id, prop, s);
                    }
                    _ => panic!("{label}: unsupported group property type for {prop}"),
                }
            }
        }
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

fn assert_property(
    node: &dyn NodeExt,
    prop: &str,
    expected: &serde_json::Value,
    entity: &str,
    id: i64,
    label: &str,
) {
    match expected {
        serde_json::Value::String(s) => node.assert_str(prop, s),
        serde_json::Value::Number(n) if n.is_i64() => {
            node.assert_i64(prop, n.as_i64().unwrap());
        }
        serde_json::Value::Bool(b) => {
            assert_eq!(
                node.prop_bool(prop),
                Some(*b),
                "{label}: {entity}/{id}.{prop} expected {b}",
            );
        }
        serde_json::Value::Null => {
            assert!(
                !node.has_prop(prop),
                "{label}: {entity}/{id}.{prop} expected null but has value",
            );
        }
        _ => panic!("{label}: unsupported property value type for {prop}"),
    }
}

fn resolve_preset<T: Clone + serde::de::DeserializeOwned>(
    kind: &str,
    spec: &Option<PresetOr<T>>,
    presets: &Path,
    scenario: &str,
) -> Option<T> {
    match spec {
        None => None,
        Some(PresetOr::Inline(v)) => Some(v.clone()),
        Some(PresetOr::Preset(name)) => {
            let path = presets.join(format!("{kind}.yaml"));
            let raw = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{scenario}: preset file {}: {e}", path.display()));
            let map: std::collections::BTreeMap<String, T> = orbit_utils::yaml::from_str(&raw)
                .unwrap_or_else(|e| panic!("{scenario}: invalid {kind} presets: {e}"));
            Some(
                map.get(name.as_str())
                    .unwrap_or_else(|| panic!("{scenario}: unknown {kind} preset '{name}'"))
                    .clone(),
            )
        }
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
    if let Some(config) = config {
        for (resource, ids) in &config.allow {
            svc.allow(resource, ids);
        }
        for (resource, ids) in &config.deny {
            svc.deny(resource, ids);
        }
    }
    svc
}

fn parse_requirement(name: &str) -> Option<Requirement> {
    if let Some(field) = name.strip_prefix("filter:") {
        return Some(Requirement::Filter {
            field: field.to_string(),
        });
    }
    if let Some(edge_type) = name.strip_prefix("relationship:") {
        return Some(Requirement::Relationship {
            edge_type: edge_type.to_string(),
        });
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_and_gql_keys_map_to_their_frontends() {
        assert_eq!(Frontend::from_name("json"), Some(Frontend::JsonDsl));
        assert_eq!(Frontend::from_name("gql"), Some(Frontend::Gql));
        assert_eq!(Frontend::from_name("sql"), None);
    }

    #[test]
    fn both_frontends_compile_the_same_query_identically() {
        let ontology = load_ontology();
        let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let json = compile(r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"columns":["username"]}],"limit":5}"#, Frontend::JsonDsl, &ontology, &security)
        .unwrap();
        let gql = compile(
            "MATCH (u:User {id: 1}) RETURN u.username LIMIT 5",
            Frontend::Gql,
            &ontology,
            &security,
        )
        .unwrap();
        assert_eq!(json.base.sql, gql.base.sql);
        assert_eq!(json.base.params, gql.base.params);
        assert_eq!(json.query_type, gql.query_type);
        assert_eq!(json.hydration, gql.hydration);
    }

    #[test]
    fn gql_frontend_rejects_writes() {
        let error = compile(
            "CREATE (u:User)",
            Frontend::Gql,
            &load_ontology(),
            &SecurityContext::new(1, vec!["1/".into()]).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("Orbit query syntax"), "{error}");
    }
}
