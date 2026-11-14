//! Entities whose identities are strings, read from ClickHouse: the query, the hydration read
//! and the graph output all carry their text keys.

use std::sync::Arc;

use integration_testkit::{TestContext, graph_schema, t, versioned_data_model};
use query_engine::compiler;
use query_engine::compiler::orbit_authorization::Key;
use query_engine::compiler::{
    HydrationCompileOptions, HydrationRequest, compile, compile_hydration,
};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::shared::PipelineOutput;
use query_engine::shared::hydration::parse_hydration_batches;
use query_engine::types::QueryResult;

fn schema_sql(fragments: &query_data_model::testkit::Fragments) -> Vec<String> {
    let prefix = integration_testkit::TABLE_PREFIX.as_str();
    graph_schema(fragments)
        .tables
        .iter()
        .map(|table| format!("{};", table.to_create_sql(prefix)))
        .collect()
}

/// A neighbors query over entities identified by `slug` and `key`, joined by an edge table with
/// string ID columns: the query runs, its neighbors hydrate by their keys, and the graph shows them.
#[tokio::test]
async fn graph_formatter_named_identity_e2e() {
    use compiler::orbit_authorization::{Caller, Grants, ScopeSet};
    use query_engine::compiler::HydrationPlan;
    use query_engine::shared::hydration as helpers;

    let fragments =
        integration_testkit::authorization_variants::platform(&["acme", "acme.threads"])
            .fragments()
            .and_then(|fragments| fragments.deploy("server"))
            .unwrap();
    let sql = schema_sql(&fragments);
    let statements: Vec<&str> = sql.iter().map(String::as_str).collect();
    let ctx = TestContext::new(&statements).await;
    for insert in [
        format!(
            "INSERT INTO {} (slug, body, traversal_path) VALUES ('s1', 'hello', '1/2/')",
            t("acme_post")
        ),
        format!(
            "INSERT INTO {} (key, title, traversal_path) VALUES ('t1', 'Launch', '1/2/'), ('t2', 'Other', '1/3/')",
            t("acme_thread")
        ),
        format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) \
             VALUES ('1/2/', 's1', 'Post', 'REPLIES_TO', 't1', 'Thread')",
            t("acme_thread_edge")
        ),
    ] {
        ctx.execute(&insert).await;
    }
    let catalog = versioned_data_model(fragments);
    let grants = Grants::new(Caller::new("user:1", false, ()))
        .with("space", "read", ScopeSet::Subtrees(vec!["1/2/".into()]))
        .with("team", "read", ScopeSet::Keys(Vec::new()))
        .with("everyone", "read", ScopeSet::All);
    let limits = query_data_model::testkit::limits();

    let compiled = Arc::new(
        compile(
            "MATCH (p:Post)--(n) WHERE p.slug IN ['s1'] RETURN n",
            &catalog,
            &grants,
            limits,
        )
        .unwrap(),
    );
    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    let neighbors: Vec<(String, Key)> = result
        .rows()
        .iter()
        .filter_map(|row| row.neighbor_node())
        .map(|node| (node.entity_type.clone(), node.id.clone()))
        .collect();
    assert_eq!(neighbors, [("Thread".to_string(), Key::Text("t1".into()))]);

    let HydrationPlan::Dynamic(specs) = &compiled.hydration else {
        panic!("neighbors hydrate dynamically: {:?}", compiled.hydration);
    };
    let static_nodes: Vec<_> = compiled.base.result_context.nodes().cloned().collect();
    let refs = helpers::extract_dynamic_refs(catalog.model(), &result, &static_nodes).unwrap();
    let ref_paths =
        helpers::collect_dynamic_ref_paths(catalog.model(), &result, &static_nodes).unwrap();
    let arms = helpers::hydrate_dynamic(catalog.model(), limits, specs, &refs, &grants, &ref_paths)
        .unwrap();
    let request = HydrationRequest {
        arms,
        options: HydrationCompileOptions {
            dynamic: true,
            path_segment_budget: Some(limits.hydration_path_segments),
        },
    };
    let hydration = compile_hydration(request, &catalog, &grants, limits).unwrap();
    assert!(hydration.sql.contains(".key"), "{}", hydration.sql);
    let rows = ctx.query_parameterized(&hydration).await;
    let properties = parse_hydration_batches(&rows, catalog.model()).unwrap();
    helpers::merge_dynamic_properties(catalog.model(), &mut result, &properties).unwrap();

    let output = PipelineOutput {
        row_count: result.authorized_count(),
        redacted_count: 0,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        result_context: compiled.base.result_context.clone(),
        query_result: result,
        execution_log: vec![],
        pagination: None,
    };
    let graph = GraphFormatter.format(&output);
    let thread = graph["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["type"] == "Thread")
        .unwrap_or_else(|| panic!("{graph}"));
    assert_eq!(thread["id"], "t1");
    assert_eq!(thread["title"], "Launch");
}
