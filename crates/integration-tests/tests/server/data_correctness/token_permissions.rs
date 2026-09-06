use super::helpers::*;
use query_engine::compiler::TokenScope;
use std::collections::HashMap;

fn token_context() -> SecurityContext {
    let mut security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    security.token_scopes = Some(HashMap::from([
        ("User".into(), TokenScope::All),
        (
            "Group".into(),
            TokenScope::Namespaces(vec!["1/100/".into()].into()),
        ),
        (
            "Project".into(),
            TokenScope::Namespaces(vec!["1/100/1000/".into()].into()),
        ),
        (
            "MergeRequest".into(),
            TokenScope::Namespaces(vec!["1/100/1000/".into()].into()),
        ),
    ]));
    security
}

pub(super) async fn exact_leaf_and_empty_grants(ctx: &TestContext) {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","id_range":{"start":1,"end":10000}}],"limit":20}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    response.assert_node_count(1);
    response.assert_node_ids("Project", &[1000]);
    let mut security = token_context();
    security.token_scopes = Some(HashMap::new());
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    response.assert_node_count(0);
}

pub(super) async fn aggregate_filters_unreturned_input(ctx: &TestContext) {
    let json = r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"mr","entity":"MergeRequest"}],"relationships":[{"type":"AUTHORED","from":"u","to":"mr"}],"group_by":["u"],"aggregations":[{"count":"u","as":"total"}]}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    response.assert_group_node_ids("u", "User", &[1]);
    response.assert_group_row_value_i64("u", "User", 1, "total", 2);
    let mut security = token_context();
    security
        .token_scopes
        .as_mut()
        .unwrap()
        .remove("MergeRequest");
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    response.assert_empty_aggregation();
}

pub(super) async fn dynamic_neighbors_filter_each_endpoint(ctx: &TestContext) {
    let response = run_query_with_security(ctx,
        r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[100]}],"neighbors":{"direction":"outgoing","rel_types":["CONTAINS"]}}"#,
        &allow_all(), token_context()).await;
    response.assert_node_count(2);
    response.assert_node_ids("Group", &[100]);
    response.assert_node_ids("Project", &[1000]);
    response.assert_edge_set("CONTAINS", &[(100, 1000)]);
}

pub(super) async fn path_requires_intermediate_permission(ctx: &TestContext) {
    let json = r#"{"query_type":"path_finding","nodes":[{"id":"start","entity":"User","node_ids":[1]},{"id":"end","entity":"Project","node_ids":[1000]}],"path":{"type":"shortest","from":"start","to":"end","max_depth":3,"rel_types":["MEMBER_OF","CONTAINS"]}}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    assert_eq!(response.path_ids().len(), 1);
    response.assert_node_count(3);
    let mut security = token_context();
    security.token_scopes.as_mut().unwrap().remove("Group");
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    assert!(response.path_ids().is_empty());
    response.assert_node_count(0);
}

pub(super) async fn denied_page_omits_cursor(ctx: &TestContext) {
    let response = run_query_with_security(
        ctx,
        r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","id_range":{"start":1,"end":10000}}],"cursor":{"page_size":2}}"#,
        &MockRedactionService::new(),
        token_context(),
    )
    .await;
    response.assert_node_count(0);
    let pagination = response.response.pagination.as_ref().unwrap();
    assert!(!pagination.has_more);
    assert!(pagination.next_cursor.is_none());
}

pub(super) async fn pinned_edge_permissions_bound_ownership_reads(ctx: &TestContext) {
    use integration_testkit::t;

    let definition = t("gl_definition");
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES \
         ('1/100/1000/', 1000, 'main', 13000, 'File', 'DEFINES', 12000, 'Definition'), \
         ('1/100/1000/', 1000, 'main', 13000, 'File', 'DEFINES', 12001, 'Definition'), \
         ('1/100/1000/', 1000, 'main', 13000, 'File', 'DEFINES', 12100, 'Definition')",
        t("gl_code_edge")
    )).await;
    let ontology = load_ontology();
    let entities = ["File", "Definition"];
    let edge_granularity = ontology
        .edge_table_config(ontology.edge_table_for_relationship("DEFINES"))
        .unwrap()
        .storage
        .index_granularity
        .unwrap();
    let endpoint_checks = 2;
    let ownership_read_budget = endpoint_checks
        * entities
            .iter()
            .map(|entity| {
                let node = ontology.get_node(entity).unwrap();
                let node_granularity = node.storage.settings["index_granularity"]
                    .parse::<u64>()
                    .unwrap();
                u64::from(edge_granularity) + node_granularity
            })
            .sum::<u64>();
    let classic = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let mut restricted = classic.clone();
    restricted.token_scopes = Some(HashMap::from_iter(entities.map(|entity| {
        (
            entity.into(),
            TokenScope::Namespaces(vec!["1/100/1000/".into()].into()),
        )
    })));
    let queries = [
        r#"{"query_type":"neighbors","nodes":[{"id":"f","entity":"File","node_ids":[13000]}],"neighbors":{"direction":"outgoing","rel_types":["DEFINES"]}}"#,
        r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[13000]},{"id":"d","entity":"Definition"}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}]}"#,
    ];
    for (start, unrelated_rows) in [(1_000_000, 10_000), (1_010_000, 100_000)] {
        ctx.execute(&format!(
            "INSERT INTO {definition} (id, traversal_path, project_id, branch) \
             SELECT number + {start}, '1/100/1000/', 1000, 'main' FROM numbers({unrelated_rows})"
        ))
        .await;
        ctx.optimize_all().await;
        for json in queries {
            let mut rows_read = Vec::new();
            for security in [&classic, &restricted] {
                let compiled = compile(json, &ontology, security).unwrap();
                let (batches, summary) = ctx
                    .create_client()
                    .query(&compiled.base.render())
                    .with_setting("use_query_cache", "0")
                    .with_setting("max_threads", "1")
                    .fetch_arrow_with_summary()
                    .await
                    .unwrap();
                let summary = summary.unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    if security.token_scopes.is_some() {
                        2
                    } else {
                        3
                    }
                );
                rows_read.push(summary.read_rows().unwrap());
            }
            eprintln!(
                "unrelated={unrelated_rows} classic_rows={} token_rows={}",
                rows_read[0], rows_read[1]
            );
            assert!(
                rows_read[1] <= rows_read[0] + ownership_read_budget,
                "authorization reads exceeded the {ownership_read_budget}-row lookup budget"
            );
            let response =
                run_query_with_security(ctx, json, &allow_all(), restricted.clone()).await;
            response.assert_node_count(3);
            response.assert_node_ids("Definition", &[12000, 12001]);
            response.assert_edge_set("DEFINES", &[(13000, 12000), (13000, 12001)]);
        }
    }
}
