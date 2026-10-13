use super::setup::{admin_ctx, embedded_ontology, test_ctx};
use compiler::{
    AuthorizedPath, ColumnSelection, Frontend, HydrationCompileOptions, HydrationPlan, Input,
    InputNode, QueryType, compile, compile_input,
};
use orbit_utils::traversal_path::TraversalPath;

#[test]
fn valid_column_in_order_by() {
    let query = r#"MATCH (u:User {id: 1})
        RETURN u{.username}
        ORDER BY u.username
        LIMIT 10"#;
    assert!(compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_order_by() {
    let err = compile(
        r#"MATCH (u:User {id: 1})
            RETURN u{.username}
            ORDER BY u.nonexistent_column
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn valid_column_in_filter() {
    let query = r#"MATCH (u:User {username: 'admin'})
        RETURN u{.username}
        LIMIT 10"#;
    assert!(compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_filter() {
    let err = compile(
        r#"MATCH (u:User {nonexistent_column: 'value'})
        RETURN u{.username}
        LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("nonexistent_column"));
}

#[test]
fn valid_column_in_aggregation() {
    assert!(
        compile(
            "MATCH (p:Project {id: 1}) RETURN count(p.name) AS name_count LIMIT 10",
            Frontend::Gql,
            &embedded_ontology(),
            &test_ctx(),
        )
        .is_ok()
    );
}

#[test]
fn invalid_column_in_aggregation() {
    let err = compile(
        "MATCH (p:Project {id: 1}) RETURN sum(p.invalid_property) AS total LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn invalid_entity_type_names_the_label() {
    let err = compile(
        r#"MATCH (n:NonexistentType {id: 1})
            RETURN n{.name}
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unknown entity 'NonexistentType'"),
        "got: {msg}"
    );
}

#[test]
fn invalid_filter_key_lists_valid_candidates() {
    let err = compile(
        r#"MATCH (u:User {project_full_path: 'x'})
            RETURN u{.username}
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("project_full_path"), "got: {msg}");
    assert!(msg.contains("Valid fields:"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
}

#[test]
fn invalid_group_by_property_lists_valid_fields() {
    let err = compile(
        r#"MATCH (p:Project {id: 1})
            RETURN p.reviewer_count, count(p) AS c
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("reviewer_count"), "got: {msg}");
    assert!(msg.contains("does not exist"), "got: {msg}");
    assert!(msg.contains("Valid fields"), "got: {msg}");
    assert!(msg.contains("name"), "got: {msg}");
}

#[test]
fn bare_string_group_by_unknown_node_names_the_reference() {
    let err = compile(
        "MATCH (p:Project {id: 1}) RETURN name, count(p) AS c LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("undefined node \"name\""), "got: {msg}");
}

#[test]
fn invalid_column_lists_valid_candidates() {
    let err = compile(
        r#"MATCH (u:User)
            RETURN u{.bogus_col}
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("bogus_col"), "got: {msg}");
    assert!(msg.contains("Valid fields:"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
}

#[test]
fn invalid_relationship_type_names_the_type() {
    let err = compile(
        r#"MATCH (u:User {id: 1})-[:BOGUS_REL]->(n:Note)
            RETURN u, n
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unknown relationship type \"BOGUS_REL\""),
        "got: {msg}"
    );
}

#[test]
fn full_pipeline() {
    let query = r#"MATCH (n:Note {confidential: true})<-[:AUTHORED]-(u:User)
        RETURN n{.confidential}, u{.username}
        ORDER BY n.created_at DESC
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    // AUTHORED is FK-elided via author_id — no edge table scan.
    assert!(rendered.contains("gl_note"));
    assert!(rendered.contains("gl_user"));
    assert!(rendered.contains("LIMIT 26"));
}

#[test]
fn package_built_by_pipeline_traversal() {
    let query = r#"MATCH (pkg:Package {package_type: 'npm'})-[:BUILT_BY]->(pl:Pipeline)
        RETURN pkg{.name, .version, .package_type}, pl{.id, .status}
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("gl_package"));
    assert!(rendered.contains("gl_ci_edge"));
    assert!(rendered.contains("'BUILT_BY'"));
    assert!(rendered.contains("(_orbit_edge_0.source_kind = 'Package')"));
    assert!(rendered.contains("(_orbit_edge_0.target_kind = 'Pipeline')"));
    assert!(rendered.contains("LIMIT 26"));
}

#[test]
fn basic_search_query() {
    let query = r#"MATCH (u:User)
        WHERE u.username = 'admin'
        RETURN u{.username}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains(" FINAL"),
        "search should use FINAL for dedup"
    );
    assert!(
        rendered.contains("_deleted"),
        "search should filter deleted rows"
    );
    assert!(rendered.contains("username"));
    assert!(rendered.contains("LIMIT 11"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn complex_search_query() {
    let query = r#"MATCH (u:User)
        WHERE u.username STARTS WITH 'admin' AND u.state IN ['active', 'blocked'] AND u.created_at >= '2024-01-01'
        RETURN u{.username, .state, .created_at}
        ORDER BY u.created_at DESC
        LIMIT 50"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    // Uses ClickHouse `IN [...]` array syntax which sqlparser can't parse.
    let rendered = result.base.render();

    assert!(rendered.contains(" FINAL"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains("username"));
    assert!(rendered.contains("state"));
    assert!(rendered.contains("created_at"));
    assert!(rendered.contains("ORDER BY"));
    assert!(rendered.contains("DESC"));
    assert!(rendered.contains("LIMIT 51"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn search_with_specific_columns() {
    let query = r#"MATCH (u:User {id: 1})
        RETURN u{.username, .state}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(rendered.contains("u_username"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn search_with_wildcard_columns() {
    let query = r#"MATCH (u:User {id: 1})
        RETURN properties(u)
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn traversal_with_columns() {
    let query = r#"MATCH (u:User {id: 1})-[:CONTAINS]->(p:Project)
        RETURN u{.username}, p{.name}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(rendered.contains("_orbit_p_id"));
    assert!(rendered.contains("_orbit_p_type"));
}

#[test]
fn aggregation_includes_mandatory_columns_for_group_by_node() {
    let query = "MATCH (u:User {id: 1})-[:AUTHORED]->(mr:MergeRequest) RETURN u{.username}, count(mr) AS mr_count LIMIT 10";

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(!rendered.contains("_orbit_mr_id"));
    assert!(!rendered.contains("_orbit_mr_type"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn path_finding_uses_orbit_path_not_node_columns() {
    let query = "MATCH p = ANY SHORTEST (start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200}) RETURN p";

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_path"));
    assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
}

#[test]
fn result_context_populated() {
    let query = r#"MATCH (u:User {id: 1})-[:CONTAINS]->(p:Project)
        RETURN u{.username}, p{.name}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_orbit_u_id");
    assert_eq!(user.type_column, "_orbit_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_orbit_p_id");
    assert_eq!(project.type_column, "_orbit_p_type");

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(rendered.contains("_orbit_p_id"));
    assert!(rendered.contains("_orbit_p_type"));
}

#[test]
fn single_hop_does_not_generate_recursive_cte() {
    let query = r#"MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note)
        RETURN u{.username}, n{.confidential}
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        !rendered.contains("WITH RECURSIVE"),
        "single hop should not generate recursive CTE"
    );
}

#[test]
fn multi_hop_aggregation() {
    let query = "MATCH (u:User {id: 1})-[:MEMBER_OF*1..2]->(p:Project) RETURN u{.username}, count(p) AS project_count LIMIT 10";

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("UNION ALL"));
    assert!(rendered.contains("_orbit_edge_0"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
}

#[test]
fn definition_uses_project_id_for_redaction() {
    let query = r#"MATCH (d:Definition {id: 1})
        RETURN d{.name, .project_id}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_d_id"));
    assert!(rendered.contains("_orbit_d_type"));
    assert!(
        rendered.contains("d.project_id") && rendered.contains("_orbit_d_id"),
        "Definition should use project_id for redaction"
    );
}

#[test]
fn project_still_uses_id_for_redaction() {
    let query = r#"MATCH (p:Project {id: 1})
        RETURN p{.name}
        LIMIT 10"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_p_id"));
    assert!(
        rendered.contains("p.id AS _orbit_p_id"),
        "Project should use id for redaction"
    );
}

#[test]
fn cursor_pagination_validation() {
    use compiler::QueryError;
    use compiler::passes::cursor::encode;

    let ontology = embedded_ontology();
    let ctx = test_ctx();

    let query = "MATCH (u:User {id: 1}) RETURN u{.username} PAGE 20";
    let result = compile(query, Frontend::Gql, &ontology, &ctx);
    assert!(result.is_ok(), "valid cursor should compile: {result:?}");
    let rendered = result.unwrap().base.render();
    assert!(
        rendered.contains("LIMIT 21"),
        "cursor fetches page_size + 1 probe row: {rendered}"
    );
    assert!(
        rendered.contains("_orbit_cursor_0"),
        "cursor queries select hidden key readback columns: {rendered}"
    );

    let hash = compile(query, Frontend::Gql, &ontology, &ctx)
        .unwrap()
        .pagination
        .query_hash;
    let after = |token: &str| format!("{query} AFTER '{token}'");
    let result = compile(
        &after(&encode(hash, &[Some("7".into())])),
        Frontend::Gql,
        &ontology,
        &ctx,
    );
    assert!(
        result.is_ok(),
        "after token from same query should compile: {result:?}"
    );
    let rendered = result.unwrap().base.render();
    assert!(
        rendered.contains("u.id >"),
        "after token should lower to a seek predicate: {rendered}"
    );

    let err = compile(
        &after(&encode(hash ^ 1, &[Some("7".into())])),
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "token minted for a different query should be a pagination error, got: {err}"
    );

    let err = compile(&after("not-base64!"), Frontend::Gql, &ontology, &ctx).unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "malformed token should be a pagination error, got: {err}"
    );

    let err = compile(
        r#"MATCH (u:User {id: 1})
            RETURN u
            PAGE 0"#,
        Frontend::Gql,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size = 0 should fail");

    let err = compile(
        r#"MATCH (u:User {id: 1})
            RETURN u
            PAGE 1001"#,
        Frontend::Gql,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size above 1000 should fail");

    let result = compile(
        r#"MATCH (u:User {id: 1})
            RETURN u"#,
        Frontend::Gql,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "no cursor should compile fine");
    let result = result.unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("LIMIT 31"),
        "default limit 30 fetches one probe row: {rendered}"
    );
    assert!(
        !result.base.sql.contains("use_query_cache"),
        "queries no longer force the query cache: {}",
        result.base.sql
    );
}

#[test]
fn render_traversal_inlines_all_params() {
    let rendered = compile(
        r#"MATCH (mr:MergeRequest {state: 'opened'})<-[:AUTHORED]-(u:User)
            RETURN mr, u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(
        rendered.contains("'opened'") || rendered.contains("'state:opened'"),
        "rendered SQL should contain the state filter value"
    );
    assert!(rendered.contains("'AUTHORED'"));
}

#[test]
fn render_in_filter_inlines_array() {
    let rendered = compile(
        r#"MATCH (u:User)
            WHERE u.user_type IN ['project_bot', 'service_account']
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("'project_bot'") && rendered.contains("'service_account'"));
}

#[test]
fn render_node_ids_inlines_array() {
    let rendered = compile(
        r#"MATCH (u:User)
            WHERE u.id IN [100, 200, 300]
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("100") && rendered.contains("200") && rendered.contains("300"));
}

#[test]
fn debug_json_round_trip() {
    let compiled = compile(
        r#"MATCH (mr:MergeRequest {state: 'opened'})<-[:AUTHORED]-(u:User)
            RETURN mr, u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();

    let rendered = compiled.base.render();
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": rendered,
        "hydration": serde_json::json!([]),
    });
    let parsed: serde_json::Value = serde_json::from_str(&debug_json.to_string()).unwrap();
    assert!(
        parsed["base"].as_str().unwrap().contains("{p"),
        "base should have placeholders"
    );
    assert!(
        !parsed["base_rendered"].as_str().unwrap().contains("{p"),
        "rendered should not"
    );
    assert!(parsed["hydration"].is_array());
}

#[test]
fn hydration_query_type_generates_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".into(),
                entity: Some("Note".into()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "noteable_type".into(),
                ])),
                node_ids: vec![1, 2, 3],
                ..InputNode::default()
            },
            InputNode {
                id: "hydrate".into(),
                entity: Some("Project".into()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                node_ids: vec![10, 20],
                ..InputNode::default()
            },
        ],
        limit: 10,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    // Hydration SQL uses ClickHouse array literals (`IN [1,2,3]`) which
    // sqlparser doesn't support yet, so we check the raw SQL string.
    let raw = &result.base.render();

    assert!(raw.contains("UNION ALL"));
    assert!(raw.contains("toJSONString"));
    assert!(raw.contains("gl_note"));
    assert!(raw.contains("gl_project"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn hydration_widens_paths_to_segment_budget() {
    let deep = |count: usize| -> Vec<TraversalPath> {
        (0..count)
            .map(|i| TraversalPath::new_unchecked(format!("1/{i:0>40}/{:0>40}/", i + 10000)))
            .collect()
    };
    let node = |entity: &str, paths: Vec<TraversalPath>| InputNode {
        id: "hydrate".into(),
        entity: Some(entity.into()),
        columns: Some(ColumnSelection::List(vec!["id".into()])),
        node_ids: vec![1],
        traversal_paths: paths,
        ..InputNode::default()
    };
    let compile_hydration = |nodes: Vec<InputNode>, budget: Option<usize>| {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes,
            limit: 10,
            ..Input::default()
        };
        compile_input(
            input,
            HydrationCompileOptions {
                dynamic: true,
                path_segment_budget: budget,
            },
            &embedded_ontology(),
            &test_ctx(),
        )
        .unwrap()
    };
    let bound_paths = |result: &compiler::CompiledQueryContext| -> Vec<TraversalPath> {
        result
            .base
            .params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .flat_map(|items| items.iter().filter_map(|v| v.as_str()))
            .map(TraversalPath::new_unchecked)
            .collect()
    };

    let exact = deep(500);
    let result = compile_hydration(
        vec![node("Note", exact.clone()), node("Project", exact.clone())],
        Some(2000),
    );
    let array_params = result
        .base
        .params
        .values()
        .filter(|p| matches!(p.value, serde_json::Value::Array(_)))
        .count();
    assert_eq!(array_params, 1, "arms share one path array param");
    let mut kept = bound_paths(&result);
    kept.sort_unstable();
    let mut expected = exact;
    expected.sort_unstable();
    assert_eq!(kept, expected, "under budget keeps exact leaf paths");

    let over = deep(900);
    let result = compile_hydration(vec![node("Note", over.clone())], Some(2000));
    let widened = bound_paths(&result);
    assert!(widened.iter().all(|w| !over.contains(w)));
    for path in &over {
        assert!(
            widened
                .iter()
                .any(|w| path.as_str().starts_with(w.as_str())),
            "{path} lost its ancestor prefix"
        );
    }

    let result = compile_hydration(vec![node("Note", over.clone())], None);
    assert_eq!(
        bound_paths(&result).len(),
        over.len(),
        "no budget, no widening"
    );
}

#[test]
fn hydration_single_entity_no_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
            node_ids: vec![42],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();

    assert!(!rendered.contains("UNION ALL"));
    assert!(rendered.contains("toJSONString"));
    assert!(rendered.contains("gl_user"));
}

#[test]
fn hydration_uses_parameterized_ids() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
                "created_at".into(),
            ])),
            node_ids: vec![7777, 8888, 9999],
            ..InputNode::default()
        }],
        limit: 3,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let parameterized = &result.base.sql;

    assert!(
        parameterized.contains("Array(Int64)"),
        "IDs should be parameterized"
    );
    assert!(
        !parameterized.contains("7777"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(rendered.contains("7777") && rendered.contains("8888") && rendered.contains("9999"));
}

#[test]
fn hydration_skips_security_context() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();

    assert!(
        !rendered.contains("arrayExists"),
        "hydration should skip security filters"
    );
    assert!(
        !rendered.contains("startsWith"),
        "hydration should not have startsWith"
    );
}

#[test]
fn hydration_id_only_columns_produces_map_with_id() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("map(") && rendered.contains("'id'"),
        "PK should be included in map when requested"
    );
}

#[test]
fn hydration_empty_columns_produces_empty_json() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec![])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("map("),
        "empty props should use literal '{{}}', not map()"
    );
}

#[test]
fn hydration_id_column_included_in_map() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "username".into(),
                "state".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("'username'") && rendered.contains("'state'"));
    let map_section = rendered
        .split("map(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or("");
    assert!(
        map_section.contains("'id'"),
        "map should contain 'id' key when requested"
    );
}

#[test]
fn like_rejects_short_contains_pattern() {
    let err = compile(
        r#"MATCH (u:User)
            WHERE u.username CONTAINS 'ab'
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_single_char_starts_with() {
    let err = compile(
        r#"MATCH (u:User)
            WHERE u.username STARTS WITH 'a'
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_empty_ends_with() {
    let err = compile(
        r#"MATCH (u:User)
            WHERE u.username ENDS WITH ''
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_contains_on_email() {
    let err = compile(
        r#"MATCH (u:User)
            WHERE u.email CONTAINS 'example'
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("string operators"),
        "expected like_allowed rejection, got: {err}"
    );
}

#[test]
fn like_rejects_starts_with_on_email() {
    let err = compile(
        r#"MATCH (u:User)
            WHERE u.email STARTS WITH 'alice'
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("string operators"),
        "expected like_allowed rejection, got: {err}"
    );
}

#[test]
fn like_equality_on_email_compiles_for_admin() {
    // `like_allowed: false` blocks LIKE operators but not equality. Admin context
    // is used because User.email is also gated by `admin_only`, which the
    // RestrictPass enforces ahead of like_allowed.
    assert!(
        compile(
            r#"MATCH (u:User {email: 'alice@example.com'})
                RETURN u
                LIMIT 10"#,
            Frontend::Gql,
            &embedded_ontology(),
            &admin_ctx(),
        )
        .is_ok()
    );
}

#[test]
fn equality_on_email_rejected_for_non_admin() {
    let err = compile(
        r#"MATCH (u:User {email: 'alice@example.com'})
            RETURN u
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("email") && msg.contains("administrator"),
        "expected admin-only rejection on User.email, got: {msg}"
    );
}

#[test]
fn filterable_allows_traversal_path_starts_with_inside_scope() {
    compile(
        r#"MATCH (g:Group)
            WHERE g.traversal_path STARTS WITH '1/100/'
            RETURN g
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path starts_with inside JWT scope should compile");
}

#[test]
fn filterable_allows_traversal_path_root_starts_with_inside_scope() {
    compile(
        r#"MATCH (g:Group)
            WHERE g.traversal_path STARTS WITH '1/'
            RETURN g
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path root starts_with inside JWT scope should compile");
}

#[test]
fn filterable_allows_traversal_path_equality_inside_scope() {
    compile(
        r#"MATCH (p:Project {traversal_path: '1/100/1000/'})
            RETURN p
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path equality inside JWT scope should compile");
}

#[test]
fn filterable_rejects_traversal_path_outside_scope() {
    let err = compile(
        r#"MATCH (mr:MergeRequest {traversal_path: '2/'})
            RETURN mr
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path scope rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_above_scope() {
    let err = compile(
        r#"MATCH (p:Project {traversal_path: '1/'})
            RETURN p
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &compiler::SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path scope rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_without_trailing_slash() {
    let err = compile(
        r#"MATCH (g:Group)
            WHERE g.traversal_path STARTS WITH '1/100'
            RETURN g
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid traversal_path format"),
        "expected traversal_path format rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_contains_operator() {
    let err = compile(
        r#"MATCH (p:Project)
            WHERE p.traversal_path CONTAINS '100'
            RETURN p
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("only eq, in, and starts_with"),
        "expected traversal_path operator rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_below_entity_role_floor() {
    let err = compile(
        r#"MATCH (v:Vulnerability {traversal_path: '1/100/1000/'})
            RETURN v
            LIMIT 10"#,
        Frontend::Gql,
        &embedded_ontology(),
        &compiler::SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/100/", 20)])
            .unwrap(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path role-scope rejection, got: {err}"
    );
}

#[test]
fn filterable_allows_traversal_path_in_columns() {
    assert!(
        compile(
            r#"MATCH (g:Group {id: 100})
                RETURN g{.name, .traversal_path}
                LIMIT 10"#,
            Frontend::Gql,
            &embedded_ontology(),
            &test_ctx(),
        )
        .is_ok()
    );
}

// Bug 1 regression guard: single-aggregate queries with a sort-key filter must
// keep the filter inside the FINAL scan so ClickHouse uses the primary-key index
// to skip granules. Without this, the latest-row scan reads the full authorized
// table before aggregation.
#[test]
fn aggregation_count_pushes_project_id_into_dedup_subquery() {
    let query = r#"MATCH (d:Definition)
        WHERE d.project_id = 278964
        RETURN count(d) AS total"#;
    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains("COUNT()") || rendered.contains("countIf"),
        "should contain COUNT() or countIf: {rendered}"
    );
    let inner = rendered
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id must appear inside the FINAL scan: {rendered}"
    );
}

#[test]
fn pinned_traversal_narrows_joined_node_via_nf_cte() {
    // Bug 2: when one node has node_ids pinned and joins to another via an
    // edge, the joined-side node table must be narrowed to ids reachable
    // from the pinned source. Without the fix, the joined Definition table
    // dedups the full authorized scope (~tens of millions of rows on
    // production data) before the JOIN.
    let query = r#"MATCH (f:File {id: 12345})-[:DEFINES]->(d:Definition)
        RETURN f{.path}, d{.name}
        LIMIT 50"#;
    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains("gl_code_edge"),
        "DEFINES should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("12345"),
        "pinned File node_id must appear in WHERE clause: {rendered}"
    );
    assert!(
        rendered.contains("_orbit_edge_0.source_id"),
        "edge-centric filter must reference source_id: {rendered}"
    );
}

#[test]
fn calls_traversal_compiles_against_embedded_ontology() {
    let query = r#"MATCH (caller:Definition {id: 1})-[:CALLS]->(callee:Definition)
        RETURN caller{.name}, callee{.name}
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "CALLS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'CALLS'"),
        "CALLS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn aggregation_count_in_clause_pushes_project_id() {
    let query = r#"MATCH (d:Definition)
        WHERE d.project_id IN [69095239, 278964, 74646916]
        RETURN count(d) AS total"#;
    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    let inner = rendered
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id IN must appear inside FINAL scan: {rendered}"
    );
}

#[test]
fn extends_traversal_compiles_against_embedded_ontology() {
    let query = r#"MATCH (child:Definition {id: 1})-[:EXTENDS]->(parent:Definition)
        RETURN child{.name}, parent{.name}
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "EXTENDS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'EXTENDS'"),
        "EXTENDS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn calls_to_imported_symbol_variant_compiles() {
    let query = r#"MATCH (caller:Definition {id: 1})-[:CALLS]->(sym:ImportedSymbol)
        RETURN caller{.name}, sym{.identifier_name}
        LIMIT 10"#;

    assert!(compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).is_ok());
}

#[test]
fn calls_aggregation_compiles() {
    let query = r#"MATCH (caller:Definition {id: 1})-[:CALLS]->(callee:Definition)
        RETURN callee, count(caller) AS callers
        LIMIT 1"#;

    assert!(compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).is_ok());
}

#[test]
fn code_graph_edge_union_routes_to_code_table() {
    let query = r#"MATCH (a:Definition {id: 1})-[:CALLS|EXTENDS|DEFINES]->(b:Definition)
        RETURN a, b
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "code-graph edges should scan gl_code_edge: {rendered}"
    );
    // Match `gl_edge` only when it is a standalone identifier so the assertion
    // does not get fooled by `gl_code_edge` (which contains the substring
    // `_edge`) or future suffixed table names. `gl_edge` followed by an
    // alphanumeric or underscore is a different identifier and must not flag.
    let mentions_sdlc_edge = rendered.match_indices("gl_edge").any(|(idx, _)| {
        let after = rendered.as_bytes().get(idx + "gl_edge".len()).copied();
        let before = idx
            .checked_sub(1)
            .and_then(|i| rendered.as_bytes().get(i).copied());
        let next_is_ident = matches!(after, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        let prev_is_ident = matches!(before, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        !next_is_ident && !prev_is_ident
    });
    assert!(
        !mentions_sdlc_edge,
        "code-graph edges should not touch SDLC gl_edge: {rendered}"
    );
}

#[test]
fn agent_session_detail_columns_are_not_queryable() {
    for column in [
        "goal",
        "title",
        "summary",
        "status",
        "user_id",
        "workflow_definition",
        "agent_type",
        "external_xid",
        "session_finished_at",
    ] {
        let query = format!("MATCH (w:AgentSession) RETURN w{{.{column}}} LIMIT 10");
        assert!(
            compile(&query, Frontend::Gql, &embedded_ontology(), &test_ctx()).is_err(),
            "AgentSession.{column} must not be selectable"
        );
    }
}

#[test]
fn agent_session_created_mr_traversal_compiles() {
    let query = r#"MATCH (w:AgentSession {flow_type: 'developer'})-[:CREATED_MR]->(mr:MergeRequest)
        RETURN w{.flow_type}, mr{.title}
        LIMIT 25"#;

    let result = compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("gl_agent_session"));
    assert!(rendered.contains("'CREATED_MR'"));
    assert!(rendered.contains("(_orbit_edge_0.source_kind = 'AgentSession')"));
    assert!(rendered.contains("(_orbit_edge_0.target_kind = 'MergeRequest')"));
}
