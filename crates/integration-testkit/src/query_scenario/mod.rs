//! YAML-driven query pipeline scenarios. Parallels `scenario/` for indexer
//! tests but exercises the full query path: compile, execute, redact,
//! hydrate, paginate, format, assert.

mod duckdb;
mod format;
mod pagination;
#[cfg(test)]
mod regression;
mod response;

use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::Path;
use std::sync::Arc;

use format::ExpectedIndex;
use futures::FutureExt;
use pagination::{PageCollection, next_page, with_after, without_cursor};
use query_engine::compiler::{
    AccessLevel, AuthorizedPath, CompiledQueryContext, Frontend, Input, ResultContext,
    SecurityContext,
};
use query_engine::formatters::{GqlFormatter, GraphFormatter, ResultFormatter};
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineStage, QueryPipelineContext, TypeMap,
};
use query_engine::shared::content::ColumnResolverRegistry;
use query_engine::shared::{CompilationStage, PipelineOutput, RedactionOutput};
use response::{
    assert_compilation, assert_cursor, assert_output, assert_repeated_response, repeat_count,
    validate_response,
};

use crate::context::TestContext;
use crate::mock_redaction::MockRedactionService;
use crate::scenario::{self, Seed};
use crate::visitor::{NodeExt, Requirement, ResponseView};
use crate::{
    SeededColumnResolver, collect_subtest_results, derive_clickhouse_data_model, load_ontology,
};

pub use duckdb::run_duckdb_dir;
pub use format::{
    PathEdgeExpect, PresetOr, QueryExpect, QueryScenario, QueryStep, RedactionConfig,
    ScenarioConfig, SecurityOverride,
};

use orbit_server::pipeline::{HydrationStage, RoutingOutput, RoutingStage};
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
        scenario::read_cases::<QueryScenario>(file);
    }
}

fn read_scenarios(root: &Path, file: &Path) -> Vec<(String, QueryScenario)> {
    let name = scenario::scenario_name(root, file);
    scenario::read_cases(file)
        .into_iter()
        .map(|(case, scenario)| {
            let name = case.map_or_else(|| name.clone(), |case| format!("{name}/{case}"));
            (name, scenario)
        })
        .collect()
}

/// Load a named seed preset from `presets/seed.yaml` and apply it.
pub async fn load_yaml_seed(ctx: &TestContext, presets_dir: &str, name: &str) {
    let seed = seed_preset(Path::new(presets_dir), name);
    let columns = crate::scenario::seed::fetch_table_columns(ctx).await;
    crate::scenario::seed::apply_seed(ctx, &seed, &Default::default(), &columns, name).await;
}

fn seed_preset(presets_dir: &Path, name: &str) -> Seed {
    let path = presets_dir.join("seed.yaml");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed presets at {}: {e}", path.display()));
    let mut map: std::collections::BTreeMap<String, Seed> =
        orbit_utils::yaml::from_str(&raw).unwrap_or_else(|e| panic!("invalid seed presets: {e}"));
    map.remove(name)
        .unwrap_or_else(|| panic!("unknown seed preset '{name}'"))
}

pub async fn run_query(
    ctx: &TestContext,
    frontend: Frontend,
    query: &str,
    security: &SecurityContext,
    redaction: &MockRedactionService,
) -> ResponseView {
    let ontology = load_ontology();
    let data_model = derive_clickhouse_data_model(&ontology);
    let compiled = compile_like_server(query, frontend, (&data_model, &ontology), security)
        .await
        .expect("query should compile");
    let output = execute_output(
        ctx,
        frontend,
        &compiled,
        (&data_model, &ontology),
        security,
        redaction,
    )
    .await;
    assert_cursor(&output, "run_query");
    let response = validate_response(&compiled.input, GraphFormatter.format(&output), "run_query");
    ResponseView::for_query(&compiled.input, response)
}

pub async fn run_dir(ctx: &TestContext, root: &str, presets: &str) {
    let root = Path::new(root);
    let presets: Arc<Path> = Arc::from(Path::new(presets));
    let mut files = Vec::new();
    scenario::discover(root, &mut files);
    files.sort();
    let mut scenarios: Vec<_> = files
        .iter()
        .flat_map(|file| read_scenarios(root, file))
        .collect();
    assert!(
        !scenarios.is_empty(),
        "no query scenario files found under {}",
        root.display()
    );

    if let Ok(filter) = std::env::var("SCENARIO_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            scenarios.retain(|(name, _)| name.contains(filter));
            assert!(
                !scenarios.is_empty(),
                "SCENARIO_FILTER='{filter}' matched no query scenarios under {}",
                root.display()
            );
        }
    }

    scenario::assert_distinct_database_names(scenarios.iter().map(|(name, _)| name.clone()));

    let concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let ctx = Arc::new(ctx.clone());

    let mut handles = Vec::new();
    for (name, scenario) in scenarios {
        let semaphore = Arc::clone(&semaphore);
        let ctx = Arc::clone(&ctx);
        let presets = Arc::clone(&presets);
        let task_name = name.clone();
        let handle = tokio::task::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            run_scenario(&ctx, scenario, &task_name, &presets).await;
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
}

async fn run_scenario(ctx: &TestContext, scenario: QueryScenario, name: &str, presets: &Path) {
    let (config, steps) = scenario.into_steps();
    let database = (!config.extra_seed.is_empty()
        || config.ontology_overlay.is_some()
        || steps.iter().any(|step| !step.seed.is_empty()))
    .then(|| scenario::database_name(name));
    let outcome = AssertUnwindSafe(run_case(
        ctx,
        &config,
        &steps,
        name,
        presets,
        database.as_deref(),
    ))
    .catch_unwind()
    .await;
    let cleanup = if let Some(database) = database {
        AssertUnwindSafe(ctx.execute(&format!("DROP DATABASE IF EXISTS `{database}` SYNC")))
            .catch_unwind()
            .await
    } else {
        Ok(())
    };
    if let Err(failure) = outcome.and(cleanup) {
        resume_unwind(failure);
    }
}

async fn run_case(
    ctx: &TestContext,
    cfg: &ScenarioConfig,
    steps: &[QueryStep],
    name: &str,
    presets: &Path,
    database: Option<&str>,
) {
    let ctx = if let Some(database) = database {
        let forked = ctx.fork(database).await;
        if let Some(overlay) = &cfg.ontology_overlay {
            let ontology = crate::load_ontology_overlay(overlay);
            let schema = orbit_migrations::schema::GraphSchema::from_ontology(&ontology).unwrap();
            let credentials = orbit_migrations::schema::DictionaryCredentials {
                database: forked.config.database.clone(),
                user: forked.config.username.clone(),
                password: forked.config.password.clone(),
            };
            orbit_migrations::execute::create_all_versioned_tables(
                &forked.create_client(),
                &schema,
                &credentials,
                &crate::TABLE_PREFIX,
            )
            .await
            .unwrap();
        }
        let columns = crate::scenario::seed::fetch_table_columns(&forked).await;
        let mut settings = Default::default();
        if cfg.unmerged_seed {
            for table in cfg.extra_seed.keys() {
                let table = crate::scenario::seed::prefix_graph_table(table);
                assert!(
                    columns.contains_key(&table),
                    "{name}: unknown table '{table}'"
                );
                forked.execute(&format!("SYSTEM STOP MERGES {table}")).await;
            }
            settings = std::collections::BTreeMap::from([(
                "optimize_on_insert".to_string(),
                serde_json::json!(0),
            )]);
        }
        if cfg.unmerged_seed {
            for (table, rows) in &cfg.extra_seed {
                for row in rows {
                    let seed = Seed::from([(table.clone(), vec![row.clone()])]);
                    crate::scenario::seed::apply_seed(&forked, &seed, &settings, &columns, name)
                        .await;
                }
            }
        } else {
            crate::scenario::seed::apply_seed(&forked, &cfg.extra_seed, &settings, &columns, name)
                .await;
            forked.optimize_all().await;
        }
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
    let ontology = cfg
        .ontology_overlay
        .as_ref()
        .map_or_else(load_ontology, |name| {
            Arc::new(
                crate::load_ontology_overlay(name).with_schema_version_prefix(&crate::TABLE_PREFIX),
            )
        });
    let data_model = derive_clickhouse_data_model(&ontology);

    for (index, step) in steps.iter().enumerate() {
        if !step.seed.is_empty() {
            let columns = crate::scenario::seed::fetch_table_columns(&ctx).await;
            crate::scenario::seed::apply_seed(
                &ctx,
                &step.seed,
                &Default::default(),
                &columns,
                name,
            )
            .await;
            ctx.optimize_all().await;
        }
        let name = &step_label(name, index, steps.len());
        for (frontend_key, query_str) in &step.query {
            let frontend = frontend_key
                .parse::<Frontend>()
                .unwrap_or_else(|_| panic!("{name}: unknown query language '{frontend_key}'"));
            run_frontend(
                (&ctx, &data_model, &ontology),
                (frontend, frontend_key),
                query_str,
                &security,
                &redaction,
                &step.expect,
                name,
            )
            .await;
        }
    }
}

pub(super) fn step_label(name: &str, index: usize, steps: usize) -> String {
    if steps > 1 {
        format!("{name} step {}", index + 1)
    } else {
        name.to_string()
    }
}

async fn run_frontend(
    (ctx, data_model, ontology): (
        &TestContext,
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    (frontend, frontend_key): (Frontend, &str),
    query: &str,
    security: &SecurityContext,
    redaction: &MockRedactionService,
    expect: &QueryExpect,
    name: &str,
) {
    let label = &format!("{name} [{frontend_key}]");
    expect.validate_pages_exclusive(label);

    let compiled = compile_like_server(query, frontend, (data_model, ontology), security).await;
    let Some(compiled) = assert_compilation(compiled, expect, frontend_key, label) else {
        return;
    };

    if expect.compile_only {
        return;
    }

    if !expect.indexes_used.is_empty() {
        let plan = ctx.explain_plan(&compiled.base).await;
        assert_indexes_used(&plan, &expect.indexes_used, label);
    }

    if !expect.pages.is_empty() {
        run_pages(
            ctx,
            frontend,
            query,
            (data_model, ontology),
            security,
            redaction,
            expect,
            label,
        )
        .await;
        return;
    }

    let output = execute_output(
        ctx,
        frontend,
        &compiled,
        (data_model, ontology),
        security,
        redaction,
    )
    .await;
    let baseline = assert_output(&output, frontend, expect, label);
    let count = repeat_count(expect, label);
    for run in 2..=count {
        let compiled = compile_like_server(query, frontend, (data_model, ontology), security).await;
        let compiled = assert_compilation(compiled, expect, frontend_key, label)
            .expect("repeated query should compile");
        if !expect.indexes_used.is_empty() {
            let plan = ctx.explain_plan(&compiled.base).await;
            assert_indexes_used(&plan, &expect.indexes_used, label);
        }
        let output = execute_output(
            ctx,
            frontend,
            &compiled,
            (data_model, ontology),
            security,
            redaction,
        )
        .await;
        let rerun = assert_output(&output, frontend, expect, label);
        assert_repeated_response(&baseline, &rerun, run, label);
    }
}

fn assert_indexes_used(plan: &serde_json::Value, expected: &[ExpectedIndex], label: &str) {
    for ExpectedIndex { table, index } in expected {
        let scan = read_from_merge_tree(plan, table).unwrap_or_else(|| {
            panic!("{label}: no ReadFromMergeTree step for table {table}\nplan: {plan:#}")
        });
        let entry = skip_index(scan, index).unwrap_or_else(|| {
            panic!("{label}: skip index {index} not applied on {table}\nplan: {plan:#}")
        });
        let initial = entry["Initial Granules"].as_u64().unwrap_or(0);
        let selected = entry["Selected Granules"].as_u64().unwrap_or(initial);
        assert!(
            selected < initial,
            "{label}: skip index {index} on {table} selected {selected} of {initial} granules\nplan: {plan:#}"
        );
    }
}

fn unversioned_table(description: &str) -> &str {
    let table = description.rsplit('.').next().unwrap_or(description);
    match table
        .strip_prefix('v')
        .and_then(|rest| rest.split_once('_'))
    {
        Some((version, unversioned)) if version.chars().all(|c| c.is_ascii_digit()) => unversioned,
        _ => table,
    }
}

fn read_from_merge_tree<'a>(
    value: &'a serde_json::Value,
    table: &str,
) -> Option<&'a serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => {
            let is_scan = map.get("Node Type").and_then(serde_json::Value::as_str)
                == Some("ReadFromMergeTree");
            let scans_table = map
                .get("Description")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|description| unversioned_table(description) == table);
            if is_scan && scans_table {
                return Some(value);
            }
            map.values().find_map(|v| read_from_merge_tree(v, table))
        }
        serde_json::Value::Array(items) => {
            items.iter().find_map(|v| read_from_merge_tree(v, table))
        }
        _ => None,
    }
}

fn skip_index<'a>(value: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("Type").and_then(serde_json::Value::as_str) == Some("Skip")
                && map.get("Name").and_then(serde_json::Value::as_str) == Some(name)
            {
                return Some(value);
            }
            map.values().find_map(|v| skip_index(v, name))
        }
        serde_json::Value::Array(items) => items.iter().find_map(|v| skip_index(v, name)),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pages(
    ctx: &TestContext,
    frontend: Frontend,
    base_query: &str,
    (data_model, ontology): (
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    security: &SecurityContext,
    redaction: &MockRedactionService,
    expect: &QueryExpect,
    label: &str,
) {
    let mut query_str = base_query.trim_end().to_owned();

    let mut collected = PageCollection::default();

    for (i, page_expect) in expect.pages.iter().enumerate() {
        let page_label = format!("{label} page {}", i + 1);

        let compiled = compile_like_server(&query_str, frontend, (data_model, ontology), security)
            .await
            .unwrap_or_else(|e| panic!("{page_label}: compile failed: {e}"));

        let output = execute_output(
            ctx,
            frontend,
            &compiled,
            (data_model, ontology),
            security,
            redaction,
        )
        .await;
        let response = assert_output(&output, frontend, page_expect, &page_label);
        let response = serde_json::from_value(response).expect("response should deserialize");
        collected.push(&response);
        if let Some(cursor) = next_page(&response, i, expect.pages.len(), &page_label) {
            query_str = with_after(frontend, base_query, &cursor);
        }
    }

    if let Some(all_pages) = &expect.all_pages {
        collected.assert_expect(all_pages, label);
        if all_pages.matches_unpaginated {
            let query = without_cursor(frontend, base_query);
            let compiled = compile_like_server(&query, frontend, (data_model, ontology), security)
                .await
                .unwrap_or_else(|error| panic!("{label}: unpaginated compile failed: {error}"));
            let output = execute_output(
                ctx,
                frontend,
                &compiled,
                (data_model, ontology),
                security,
                redaction,
            )
            .await;
            assert_cursor(&output, label);
            let response =
                validate_response(&compiled.input, GraphFormatter.format(&output), label);
            collected.assert_matches(&response, label);
        }
    }
}

async fn compile_like_server(
    query: &str,
    frontend: Frontend,
    (data_model, ontology): (
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    security: &SecurityContext,
) -> Result<Arc<CompiledQueryContext>, PipelineError> {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::clone(data_model));
    let mut pipeline_ctx = QueryPipelineContext {
        frontend,
        query_json: query.to_owned(),
        compiled: None,
        ontology: Arc::clone(ontology),
        security_context: Some(security.clone()),
        server_extensions,
        phases: TypeMap::default(),
    };
    let mut obs = NoOpObserver;
    if let RoutingOutput::Schema(_) = RoutingStage.execute(&mut pipeline_ctx, &mut obs).await? {
        return Err(PipelineError::custom("schema calls are not graph queries"));
    }
    CompilationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await?;
    pipeline_ctx
        .compiled
        .ok_or_else(|| PipelineError::custom("CompilationStage produced no query"))
}

async fn execute_output(
    ctx: &TestContext,
    frontend: Frontend,
    compiled: &Arc<CompiledQueryContext>,
    (data_model, ontology): (
        &Arc<query_data_model::ClickHouseDataModel>,
        &Arc<ontology::Ontology>,
    ),
    security: &SecurityContext,
    redaction: &MockRedactionService,
) -> PipelineOutput {
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
    server_extensions.insert(Arc::clone(data_model));
    server_extensions.insert(resolver_registry);

    let mut pipeline_ctx = QueryPipelineContext {
        frontend,
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

    pipeline_output(
        compiled,
        hydration_output.query_result,
        hydration_output.redacted_count,
        hydration_output.result_context,
        compiled.base.sql.clone(),
    )
}

fn pipeline_output(
    compiled: &Arc<CompiledQueryContext>,
    mut query_result: QueryResult,
    redacted_count: usize,
    result_context: ResultContext,
    sql: String,
) -> PipelineOutput {
    let pagination = Some(query_engine::shared::paginate(
        &mut query_result,
        &compiled.input,
        &compiled.pagination,
    ));
    PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![sql],
        compiled: Arc::clone(compiled),
        query_result,
        result_context,
        execution_log: vec![],
        pagination,
    }
}

fn assert_response(input: &Input, response: serde_json::Value, expect: &QueryExpect, label: &str) {
    expect.validate_response_type(input.query_type, label);
    let response = validate_response(input, response, label);
    apply_expect(&ResponseView::for_query(input, response), expect, label);
}

fn assert_gql_table(output: &PipelineOutput, expect: &QueryExpect, label: &str) {
    if expect.gql_columns.is_empty() && expect.gql_column_names.is_empty() {
        return;
    }
    let (header, rows) = GqlFormatter.table(output);
    if !expect.gql_column_names.is_empty() {
        assert_eq!(
            header, expect.gql_column_names,
            "{label}: gql column names mismatch"
        );
    }
    for (name, values) in &expect.gql_columns {
        let index = header
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("{label}: gql table has no column {name}: {header:?}"));
        let actual: Vec<&str> = rows.iter().map(|row| row[index].as_str()).collect();
        assert_eq!(actual, *values, "{label}: gql column {name} mismatch");
    }
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
            let prop_count = row.keys().filter(|k| *k != "id").count();
            assert!(
                prop_count > 0,
                "{label}: node {entity}/{id} row has no property assertions (only 'id')"
            );
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
            if ne.ids.as_ref().is_some_and(Vec::is_empty) {
                view.assert_filter_has_no_matches(entity, field);
                continue;
            }
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
    for (kind, edges) in &expect.edge_exists {
        for expected in edges {
            let edge = view
                .response
                .edges
                .iter()
                .find(|edge| edge.edge_type == *kind && expected.matches(edge));
            let edge = edge.unwrap_or_else(|| panic!("{label}: expected {kind} edge {expected:?}"));
            view.assert_edge_exists(&edge.from, edge.from_id, &edge.to, edge.to_id, kind);
        }
    }
    for (kind, edges) in &expect.edge_absent {
        for expected in edges {
            let found = view
                .response
                .edges
                .iter()
                .any(|edge| edge.edge_type == *kind && expected.matches(edge));
            assert!(!found, "{label}: unexpected {kind} edge {expected:?}");
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
    for (name, node_dot_prop) in &expect.group_columns {
        let (node, property) = node_dot_prop.split_once('.').unwrap_or_else(|| {
            panic!("{label}: group_columns value must be 'node.property', got '{node_dot_prop}'")
        });
        view.assert_group_column(name, node, property);
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
                let dest_id = edges.last().and_then(|e| e.to_id).unwrap_or_else(|| {
                    assert!(
                        expect.path_edges.len() == 1,
                        "{label}: path_edges[{i}] must set to_id on its last edge \
                         when more than one path is expected"
                    );
                    0
                });
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
                    assert_eq!(edge.step, Some(step), "{label}: path {i} edge {j} step");
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
    let has_edge_assertions = !expect.edges.is_empty()
        || !expect.edge_exists.is_empty()
        || !expect.edge_absent.is_empty()
        || !expect.edge_count.is_empty();
    if has_edge_assertions && !view.response.edges.is_empty() {
        view.assert_all_edge_types_covered();
    }
}

fn eval_filter_predicate(
    n: &query_engine::formatters::GraphNode,
    field: &str,
    expected: &serde_json::Value,
) -> bool {
    let serde_json::Value::Object(operators) = expected else {
        return n.properties.get(field) == Some(expected);
    };
    assert!(!operators.is_empty(), "empty filter assertion on '{field}'");
    operators
        .iter()
        .all(|(operator, value)| match operator.as_str() {
            "starts_with" => n
                .prop_str(field)
                .is_some_and(|text| text.starts_with(value.as_str().unwrap())),
            "contains" => n
                .prop_str(field)
                .is_some_and(|text| text.contains(value.as_str().unwrap())),
            "ends_with" => n
                .prop_str(field)
                .is_some_and(|text| text.ends_with(value.as_str().unwrap())),
            "in" => n
                .prop(field)
                .is_some_and(|actual| value.as_array().unwrap().contains(actual)),
            "is_null" if value == &serde_json::Value::Bool(true) => {
                n.properties.get(field) == Some(&serde_json::Value::Null)
            }
            "is_not_null" if value == &serde_json::Value::Bool(true) => n.has_prop(field),
            "gte" | "lte" | "lt" | "gt" => {
                let ordering = if let Some(threshold) = value.as_i64() {
                    n.prop_i64(field)
                        .or_else(|| n.prop_str(field).and_then(|text| text.parse().ok()))
                        .map(|actual| actual.cmp(&threshold))
                } else {
                    n.prop_str(field)
                        .map(|actual| actual.cmp(value.as_str().unwrap()))
                };
                ordering.is_some_and(|ordering| match operator.as_str() {
                    "gte" => !ordering.is_lt(),
                    "lte" => !ordering.is_gt(),
                    "lt" => ordering.is_lt(),
                    _ => ordering.is_gt(),
                })
            }
            "eq" => {
                n.properties.get(field) == Some(value)
                    || value.as_number().is_some_and(|number| {
                        n.prop_str(field)
                            .is_some_and(|text| text == number.to_string())
                    })
            }
            _ => panic!("unsupported filter operator '{operator}' on field '{field}'"),
        })
}

fn try_expand_repeat(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    let pattern = obj.get("repeat")?.as_str()?;
    let count = obj.get("count")?.as_u64()? as usize;
    let suffix = obj.get("suffix").and_then(|v| v.as_str()).unwrap_or("");
    Some(format!("{}{suffix}", pattern.repeat(count)))
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
        serde_json::Value::Object(m) if m.contains_key("repeat") => {
            let expanded = try_expand_repeat(m).unwrap_or_else(|| {
                panic!("{label}: {entity}/{id}.{prop}: invalid repeat object, expected {{repeat: \"str\", count: N}}")
            });
            node.assert_str(prop, &expanded);
        }
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
            node.assert_prop(prop, &serde_json::Value::Null);
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
    use query_engine::compiler::compile;

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
    fn gql_syntax_errors_report_position_without_echoing_source() {
        let ontology = load_ontology();
        let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        for (raw, position) in [
            ("CREATE (u:User)", "line 1, column 1"),
            (
                "\nMATCH (u:User {username: 'private-query-literal'}) RETURN u LMIT 1",
                "line 2, column",
            ),
        ] {
            let error = compile(raw, Frontend::Gql, &ontology, &security).unwrap_err();
            let message = error.to_string();
            assert!(error.is_client_safe(), "{message}");
            assert!(message.contains("Orbit query syntax"), "{message}");
            assert!(message.contains(position), "{message}");
            assert!(!message.contains("private-query-literal"), "{message}");
        }
    }
}
