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
    PathEdgeExpect, PresetOr, QueryExpect, QueryScenario, RedactionConfig, ScenarioConfig,
    SecurityOverride,
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
        let Ok(frontend) = frontend_key.parse::<Frontend>() else {
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
            Some(format::CompileErrorExpect::Flag(true)) => {
                let msg = e.to_string();
                for banned in &expect.compile_error_not_contains {
                    assert!(
                        !msg.contains(banned.as_str()),
                        "{label}: compile error must not contain '{banned}'\nerror: {msg}"
                    );
                }
                return;
            }
            Some(format::CompileErrorExpect::Substring(sub)) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(sub.as_str()),
                    "{label}: compile error '{msg}' does not contain '{sub}'"
                );
                for banned in &expect.compile_error_not_contains {
                    assert!(
                        !msg.contains(banned.as_str()),
                        "{label}: compile error must not contain '{banned}'\nerror: {msg}"
                    );
                }
                return;
            }
            _ => panic!("{label}: unexpected compile error: {e}"),
        },
    };

    let sql = compiled.base.render();
    for fragment in &expect.sql_contains {
        assert!(
            sql.contains(fragment.as_str()),
            "{label}: SQL does not contain '{fragment}'\nSQL: {sql}"
        );
    }
    for fragment in &expect.sql_not_contains {
        assert!(
            !sql.contains(fragment.as_str()),
            "{label}: SQL should not contain '{fragment}'\nSQL: {sql}"
        );
    }

    if expect.compile_only {
        return;
    }

    if !expect.pages.is_empty() {
        expect.validate_pages_exclusive(label);
        run_pages(
            ctx, frontend, query, &ontology, security, redaction, expect, label,
        )
        .await;
        return;
    }

    let resp = execute_pipeline(ctx, &compiled, &ontology, security, redaction).await;

    let response: query_engine::formatters::GraphResponse =
        serde_json::from_value(resp).expect("response should deserialize");
    let view = ResponseView::for_query(&compiled.input, response);

    apply_expect(&view, expect, label);
}

fn with_after(frontend: Frontend, base_query: &str, token: &str) -> String {
    match frontend {
        Frontend::JsonDsl => {
            let mut query: serde_json::Value =
                serde_json::from_str(base_query).expect("query must be valid JSON");
            query["cursor"]["after"] = serde_json::Value::String(token.to_owned());
            query.to_string()
        }
        Frontend::Gql => format!("{base_query} AFTER '{token}'"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pages(
    ctx: &TestContext,
    frontend: Frontend,
    base_query: &str,
    ontology: &Arc<ontology::Ontology>,
    security: &SecurityContext,
    redaction: &MockRedactionService,
    expect: &QueryExpect,
    label: &str,
) {
    let mut query_str = base_query.trim_end().to_owned();

    let mut collected_ids: std::collections::BTreeMap<String, Vec<i64>> =
        std::collections::BTreeMap::new();
    let mut collected_group_ids: std::collections::BTreeMap<String, Vec<i64>> =
        std::collections::BTreeMap::new();
    let mut collected_edges: Vec<(String, i64, i64)> = Vec::new();
    let mut page_count = 0;

    for (i, page_expect) in expect.pages.iter().enumerate() {
        let page_label = format!("{label} page {}", i + 1);

        let compiled = Arc::new(
            compile(&query_str, frontend, ontology, security)
                .unwrap_or_else(|e| panic!("{page_label}: compile failed: {e}")),
        );

        let resp = execute_pipeline(ctx, &compiled, ontology, security, redaction).await;
        let response: query_engine::formatters::GraphResponse =
            serde_json::from_value(resp).expect("response should deserialize");

        let next_cursor = response
            .pagination
            .as_ref()
            .and_then(|p| p.next_cursor.clone());

        // Collect IDs and edges for all_pages assertions.
        if expect.all_pages.is_some() {
            for node in &response.nodes {
                collected_ids
                    .entry(node.entity_type.clone())
                    .or_default()
                    .push(node.id);
            }
            for edge in &response.edges {
                collected_edges.push((edge.edge_type.clone(), edge.from_id, edge.to_id));
            }
        }
        page_count += 1;

        let view = ResponseView::for_query(&compiled.input, response);

        if let Some(ap) = &expect.all_pages {
            for key in ap.group_node_ids.keys() {
                if let Some((group_key, entity)) = key.split_once(':') {
                    let ids = view.group_node_ids_ordered(group_key, entity);
                    collected_group_ids
                        .entry(key.clone())
                        .or_default()
                        .extend(ids);
                }
            }
        }

        apply_expect(&view, page_expect, &page_label);
        if page_expect.node_count.is_none() {
            view.assert_node_count(view.node_count());
        }

        match next_cursor {
            Some(cursor) => query_str = with_after(frontend, base_query.trim_end(), &cursor),
            None => {
                assert_eq!(
                    i + 1,
                    expect.pages.len(),
                    "{page_label}: no next_cursor but more pages expected"
                );
            }
        }
    }

    if let Some(ap) = &expect.all_pages {
        if let Some(expected_pages) = ap.page_count {
            assert_eq!(page_count, expected_pages, "{label}: page count mismatch");
        }
        if ap.no_duplicate_ids {
            for (entity, ids) in &collected_ids {
                let mut seen = std::collections::HashSet::new();
                for id in ids {
                    assert!(
                        seen.insert(*id),
                        "{label}: {entity}/{id} appeared on multiple pages"
                    );
                }
            }
        }
        for (entity, expected_ids) in &ap.node_ids {
            let mut actual: Vec<i64> = collected_ids.get(entity).cloned().unwrap_or_default();
            actual.sort();
            actual.dedup();
            assert_eq!(
                actual, *expected_ids,
                "{label}: collected {entity} IDs mismatch"
            );
        }
        for (key, expected_ids) in &ap.group_node_ids {
            let mut actual = collected_group_ids.get(key).cloned().unwrap_or_default();
            actual.sort();
            actual.dedup();
            assert_eq!(
                actual, *expected_ids,
                "{label}: collected group {key} IDs mismatch"
            );
        }
        if let Some(expected) = ap.edge_count {
            collected_edges.sort();
            collected_edges.dedup();
            assert_eq!(
                collected_edges.len(),
                expected,
                "{label}: collected edge count mismatch"
            );
        }
    }
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
        for prop in &ne.prop_present {
            for node in view.nodes_of_type(entity) {
                assert!(
                    node.has_prop(prop),
                    "{label}: {entity}/{} missing property '{prop}'",
                    node.id
                );
            }
        }
        if !ne.prop_absent.is_empty() && !view.nodes_of_type(entity).is_empty() {
            let present = ne.prop_present.clone();
            let absent = ne.prop_absent.clone();
            for prop in &absent {
                let present = present.clone();
                let absent_field = prop.clone();
                view.assert_filter(entity, prop, move |n| {
                    let has_positive =
                        present.iter().any(|p| n.has_prop(p)) || !n.properties.is_empty();
                    has_positive && !n.has_prop(&absent_field)
                });
            }
        } else {
            for prop in &ne.prop_absent {
                for node in view.nodes_of_type(entity) {
                    assert!(
                        !node.has_prop(prop),
                        "{label}: {entity}/{} should not have property '{prop}'",
                        node.id
                    );
                }
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
                eval_filter_predicate(n, &field_name, &expected)
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
    if let Some(n) = expect.total_edge_count {
        assert_eq!(
            view.response.edges.len(),
            n,
            "{label}: total edge count mismatch"
        );
    }
    for (group_key, ge) in &expect.groups {
        if let (Some(entity), Some(order)) = (&ge.entity, &ge.order) {
            view.assert_group_node_order(group_key, entity, order);
        } else if let (Some(entity), Some(ids)) = (&ge.entity, &ge.ids) {
            view.assert_group_node_ids(group_key, entity, ids);
        }
        if let Some(count) = ge.count {
            view.assert_group_node_count(group_key, count);
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
        for ga in &ge.absent {
            view.assert_group_node_absent(group_key, &ga.entity, ga.id);
        }
    }
    if expect.empty_aggregation {
        view.assert_empty_aggregation();
    }
    if let Some(n) = expect.path_count {
        let pids = view.path_ids();
        assert_eq!(pids.len(), n, "{label}: path count mismatch");
    }
    if !expect.path_destinations.is_empty() {
        let pids = view.path_ids();
        let mut actual: std::collections::BTreeMap<String, Vec<i64>> =
            std::collections::BTreeMap::new();
        for &pid in pids.iter() {
            if let Some(last) = view.path(pid).last() {
                actual.entry(last.to.clone()).or_default().push(last.to_id);
            }
        }
        for vals in actual.values_mut() {
            vals.sort();
            vals.dedup();
        }
        for (entity, expected_ids) in &expect.path_destinations {
            let mut expected = expected_ids.clone();
            expected.sort();
            let got = actual.get(entity).cloned().unwrap_or_default();
            assert_eq!(
                got, expected,
                "{label}: path destinations for {entity} mismatch"
            );
        }
    }
    if !expect.path_edges.is_empty() {
        for (pi, path_exp) in expect.path_edges.iter().enumerate() {
            for (ei, edge_exp) in path_exp.iter().enumerate() {
                assert!(
                    edge_exp.has_assertions(),
                    "{label}: path_edges[{pi}][{ei}] has no assertions (all fields omitted)"
                );
            }
        }
        let pids = view.path_ids();
        assert_eq!(
            pids.len(),
            expect.path_edges.len(),
            "{label}: path_edges count ({}) != actual path count ({})",
            expect.path_edges.len(),
            pids.len()
        );
        // Sort both actual paths and expected paths by destination ID
        // so the zip is deterministic regardless of path_ids() ordering.
        let mut actual_paths: Vec<_> = pids
            .iter()
            .map(|&pid| {
                let edges = view.path(pid);
                let dest_id = edges.last().map_or(0, |e| e.to_id);
                (dest_id, edges)
            })
            .collect();
        actual_paths.sort_by_key(|(dest, _)| *dest);
        let mut expected_indexed: Vec<_> = expect
            .path_edges
            .iter()
            .enumerate()
            .map(|(i, edges)| {
                let dest_id = edges.last().and_then(|e| e.to_id).unwrap_or(0);
                (dest_id, i, edges)
            })
            .collect();
        expected_indexed.sort_by_key(|(dest, _, _)| *dest);
        for (i, ((_, actual), (_, _, expected_edges))) in
            actual_paths.iter().zip(&expected_indexed).enumerate()
        {
            assert_eq!(
                actual.len(),
                expected_edges.len(),
                "{label}: path {i} edge count mismatch"
            );
            for (j, (edge, exp)) in actual.iter().zip(*expected_edges).enumerate() {
                if let Some(ref from) = exp.from {
                    assert_eq!(&edge.from, from, "{label}: path {i} edge {j} from entity");
                }
                if let Some(from_id) = exp.from_id {
                    assert_eq!(edge.from_id, from_id, "{label}: path {i} edge {j} from_id");
                }
                if let Some(ref t) = exp.edge_type {
                    assert_eq!(&edge.edge_type, t, "{label}: path {i} edge {j} type");
                }
                if let Some(ref to) = exp.to {
                    assert_eq!(&edge.to, to, "{label}: path {i} edge {j} to entity");
                }
                if let Some(to_id) = exp.to_id {
                    assert_eq!(edge.to_id, to_id, "{label}: path {i} edge {j} to_id");
                }
                if let Some(step) = exp.step {
                    assert_eq!(
                        edge.step,
                        Some(step),
                        "{label}: path {i} edge {j} step"
                    );
                }
            }
        }
    }
    for banned in &expect.path_endpoint_absent {
        for edge in &view.response.edges {
            if edge.path_id.is_some() {
                assert_ne!(
                    &edge.to, banned,
                    "{label}: path edge must not target {banned} (got {}->{} via {})",
                    edge.from_id, edge.to_id, edge.edge_type
                );
            }
        }
    }
    if expect.referential_integrity {
        view.assert_referential_integrity();
    }
    if let Some(n) = expect.row_count {
        view.assert_row_count(n);
    }
    for (i, row) in expect.row_values.iter().enumerate() {
        for (col, expected) in row {
            match expected {
                serde_json::Value::Number(n) if n.is_i64() => {
                    view.assert_row_value_i64(i, col, n.as_i64().unwrap());
                }
                serde_json::Value::String(s) => {
                    view.assert_row_value_str(i, col, s);
                }
                _ => panic!("{label}: unsupported row_values type for {col}"),
            }
        }
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

fn eval_filter_predicate(
    n: &query_engine::formatters::GraphNode,
    field: &str,
    expected: &serde_json::Value,
) -> bool {
    match expected {
        serde_json::Value::Object(m) if m.contains_key("starts_with") => n
            .prop_str(field)
            .is_some_and(|v| v.starts_with(m["starts_with"].as_str().unwrap())),
        serde_json::Value::Object(m) if m.contains_key("contains") => n
            .prop_str(field)
            .is_some_and(|v| v.contains(m["contains"].as_str().unwrap())),
        serde_json::Value::Object(m) if m.contains_key("ends_with") => n
            .prop_str(field)
            .is_some_and(|v| v.ends_with(m["ends_with"].as_str().unwrap())),
        serde_json::Value::Object(m) if m.contains_key("in") => {
            let vals = m["in"].as_array().unwrap();
            n.prop(field).is_some_and(|p| vals.contains(p))
        }
        serde_json::Value::Object(m)
            if m.get("is_null") == Some(&serde_json::Value::Bool(true)) =>
        {
            // is_null as a filter predicate is inherently trivial (passes for
            // blank nodes). Use prop_absent on the entity level instead for
            // non-trivial absence checks. This arm exists for queries where
            // other filter assertions on the same entity make the overall
            // predicate non-trivial.
            !n.has_prop(field)
        }
        serde_json::Value::Object(m)
            if m.get("is_not_null") == Some(&serde_json::Value::Bool(true)) =>
        {
            n.has_prop(field)
        }
        serde_json::Value::Object(m) if m.contains_key("gte") => {
            let threshold = &m["gte"];
            if let Some(t) = threshold.as_i64() {
                n.prop_i64(field)
                    .or_else(|| n.prop_str(field).and_then(|s| s.parse().ok()))
                    .is_some_and(|v| v >= t)
            } else {
                let t = threshold.as_str().unwrap();
                n.prop_str(field).is_some_and(|v| v >= t)
            }
        }
        serde_json::Value::Object(m) if m.contains_key("lte") => {
            let t = m["lte"].as_str().unwrap();
            n.prop_str(field).is_some_and(|v| v <= t)
        }
        serde_json::Value::Object(m) if m.contains_key("lt") => {
            let t = m["lt"].as_str().unwrap();
            n.prop_str(field).is_some_and(|v| v < t)
        }
        serde_json::Value::Object(m) if m.contains_key("eq") => {
            let val = &m["eq"];
            n.prop(field) == Some(val)
                || match val {
                    serde_json::Value::Number(num) => {
                        n.prop_str(field).is_some_and(|s| s == num.to_string())
                    }
                    serde_json::Value::String(s) => n.prop_str(field) == Some(s.as_str()),
                    _ => false,
                }
        }
        // Compound filter: all sub-predicates must pass
        serde_json::Value::Object(m) if m.len() > 1 => m.iter().all(|(op, val)| {
            let sub =
                serde_json::Value::Object(std::iter::once((op.clone(), val.clone())).collect());
            eval_filter_predicate(n, field, &sub)
        }),
        serde_json::Value::Object(m) if m.len() == 1 => {
            let op = m.keys().next().unwrap();
            panic!("unsupported filter operator '{op}' on field '{field}'")
        }
        _ => n.prop(field) == Some(expected),
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
    let authorized: Vec<AuthorizedPath> = if let Some(ap) = &ov.authorized_paths {
        assert!(
            ov.paths.is_none() && ov.access_level.is_none(),
            "specify either paths/access_level or authorized_paths, not both"
        );
        ap.iter()
            .map(|a| AuthorizedPath::new(a.path.as_str(), a.access_level))
            .collect()
    } else {
        let paths: Vec<String> = ov.paths.clone().unwrap_or_else(|| vec!["1/".into()]);
        let access = ov.access_level.unwrap_or(AccessLevel::Reporter as u32);
        paths
            .iter()
            .map(|p| AuthorizedPath::new(p.as_str(), access))
            .collect()
    };

    let mut ctx = SecurityContext::new_with_roles(org, authorized).unwrap();
    if let Some(true) = ov.admin {
        ctx = ctx.with_role(true, Some(AccessLevel::Owner as u32));
    }
    if !ov.scope_prefixes.is_empty() {
        let prefixes: std::collections::HashMap<
            String,
            orbit_utils::traversal_path::TraversalPath,
        > = ov
            .scope_prefixes
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().into()))
            .collect();
        ctx = ctx.with_scope_prefixes(prefixes);
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
        assert_eq!("json".parse(), Ok(Frontend::JsonDsl));
        assert_eq!("gql".parse(), Ok(Frontend::Gql));
        assert!("sql".parse::<Frontend>().is_err());
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
