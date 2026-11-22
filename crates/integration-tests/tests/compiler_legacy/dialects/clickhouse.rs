use crate::compiler_legacy::setup::{
    admin_ctx, compile_pair, embedded_ontology, test_ctx, test_ontology,
};
use crate::compiler_legacy::utils::has_param_value;
use compiler::gql::{PreparedStatement, prepare};
use compiler::input::DynamicColumnMode;
use compiler::{Frontend, compile};
use ontology::introspection::{
    IntrospectionScope::{All, Local},
    build_compact_schema_response,
};

#[test]
fn invalid_json_rejected() {
    assert!(
        compile(
            "not valid json",
            Frontend::JsonDsl,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn cross_namespace_related_to_edge_scans_only_the_source_namespace() {
    let orbit_query = "MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE p.id = 1 RETURN p, wi.id, rel.id, rel.title LIMIT 100";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}},
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "rel", "entity": "WorkItem", "columns": ["id", "title"]}
        ],
        "relationships": [
            {"type": "IN_PROJECT", "from": "wi", "to": "p"},
            {"type": "RELATED_TO", "from": "wi", "to": "rel"}
        ],
        "limit": 100
    }"#;
    let ontology = embedded_ontology();
    let compiled = compile_pair(json, orbit_query, &ontology, &admin_ctx()).unwrap();
    let sql = compiled.base.render();

    let related_scan = sql.split("FROM gl_edge AS _orbit_edge_1").nth(1).unwrap();
    let related_scope = related_scan.split(" ON ").next().unwrap();
    assert!(
        related_scope.contains("FROM gl_project AS _orbit_scope WHERE")
            && !sql.contains("startsWith(rel.traversal_path, (SELECT"),
        "the RELATED_TO rows carry the source's path, the target may live anywhere: {sql}"
    );

    let compiler::HydrationPlan::Static(templates) = &compiled.hydration else {
        panic!("expected static hydration");
    };
    let rel = templates.iter().find(|t| t.node_alias == "rel").unwrap();
    assert!(rel.injected_columns.is_empty());
    assert_eq!(rel.destination_table, "gl_work_item");
}

#[test]
fn orbit_query_virtual_filter_equality_hydration_parity() {
    let ontology = embedded_ontology();
    for (json, query) in [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":"abc"}}]}"#,
            "MATCH (f:File {id: 1}) WHERE f.content = 'abc' RETURN f",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":{"eq":"abc"}}}]}"#,
            "MATCH (f:File {id: 1, content: 'abc'}) RETURN f",
        ),
    ] {
        let compiled = compile_pair(json, query, &ontology, &test_ctx()).unwrap();
        let compiler::HydrationPlan::Static(templates) = compiled.hydration else {
            panic!("expected static hydration for {query}");
        };
        let filters = &templates
            .iter()
            .find(|t| t.node_alias == "f")
            .unwrap()
            .virtual_filters;
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "content");
        assert_eq!(filters[0].1.op, Some(compiler::input::FilterOp::Eq));
        assert_eq!(filters[0].1.value, Some(serde_json::Value::from("abc")));
    }
}

#[test]
fn orbit_query_rejects_input_past_the_byte_limit() {
    let oversized = format!(
        "MATCH (u:User {{username: '{}'}}) RETURN u",
        "x".repeat(40_000)
    );
    assert!(compiler::passes::frontend::gql::parse(&oversized).is_err());
}

#[test]
fn native_predicate_trees_are_bounded_before_planning() {
    use compiler::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};
    let model = query_data_model::ClickHouseDataModel::derive(
        crate::compiler_legacy::setup::embedded_ontology(),
    )
    .unwrap();
    let validator = compiler::passes::validate::Validator::new(&model);
    let mut input =
        compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let leaf = |target| {
        BooleanExpression::Leaf(Condition {
            target,
            property: "id".into(),
            filter: InputFilter {
                op: Some(FilterOp::In),
                value: Some(serde_json::json!([1])),
                ..Default::default()
            },
        })
    };
    let mut deep = leaf(PredicateTarget::Node("u".into()));
    for _ in 0..1024 {
        deep = BooleanExpression::Not(Box::new(deep));
    }
    input.predicates = vec![deep];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("must not nest deeper than 32"),
        "{error}"
    );
    input.predicates = vec![BooleanExpression::Not(Box::new(leaf(
        PredicateTarget::Relationship(7),
    )))];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("undefined relationship"),
        "{error}"
    );
}

#[test]
fn native_conditions_need_a_column() {
    use compiler::input::{BooleanExpression, InputFilter};
    let base = compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let mut empty_group = base.clone();
    empty_group.predicates = vec![BooleanExpression::Not(Box::new(BooleanExpression::And(
        vec![],
    )))];
    let mut empty_property = base;
    empty_property.nodes[0].filters.insert(
        String::new(),
        vec![InputFilter {
            value: Some(serde_json::json!(1)),
            ..Default::default()
        }],
    );
    for input in [empty_group, empty_property] {
        let error = compiler::compile_input(
            input,
            Default::default(),
            &crate::compiler_legacy::setup::embedded_ontology(),
            &test_ctx(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("every condition must compare a property"),
            "{error}"
        );
    }
}

#[test]
fn orbit_query_debug_and_dynamic_properties_match_json_options() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"options":{"include_debug_sql":true}}"#;
    let compiled = compile_pair(
        json,
        "MATCH (u:User {id: 1}) RETURN u DEBUG",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(compiled.input.options.include_debug_sql);

    let json = r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[100]}],"neighbors":{"direction":"both"},"options":{"dynamic_columns":"*"}}"#;
    let compiled = compile_pair(
        json,
        "MATCH (g:Group {id: 100})--(n) RETURN g, properties(n)",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );

    let json = r#"{"query_type":"path_finding","nodes":[{"id":"start","entity":"Project","node_ids":[100]},{"id":"end","entity":"Project","node_ids":[200]}],"path":{"type":"shortest","from":"start","to":"end","max_depth":3,"rel_types":["CONTAINS"]},"options":{"dynamic_columns":"*"}}"#;
    let compiled = compile_pair(
        json,
        "MATCH p = ANY SHORTEST (start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200}) RETURN properties(p)",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );
}

#[test]
fn orbit_query_debug_survives_optional_clauses() {
    for order in ["", " ORDER BY u.id"] {
        for limit in ["", " LIMIT 7", " SKIP 7", " SKIP 7 LIMIT 3"] {
            for debug in ["", " DEBUG"] {
                let query = format!("MATCH (u:User {{id: 1}}) RETURN u{order}{limit}{debug}");
                let compiled =
                    compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
                assert_eq!(
                    compiled.input.options.include_debug_sql,
                    !debug.is_empty(),
                    "{query}"
                );
            }
        }
    }
}

#[test]
fn orbit_query_debug_in_literals_and_comments_stays_data() {
    for (literal, value) in [
        (r#"'DEBUG'"#, "DEBUG"),
        (r#"'x\' RETURN u DEBUG //'"#, "x' RETURN u DEBUG //"),
        (r#"'\u0027 RETURN u DEBUG //'"#, "' RETURN u DEBUG //"),
        (
            r#""'; DROP TABLE gl_user; --""#,
            "'; DROP TABLE gl_user; --",
        ),
    ] {
        for comment in ["", " /* DEBUG */", " // DEBUG"] {
            let query = format!("MATCH (u:User {{username: {literal}}}) RETURN u{comment}");
            let compiled = compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
            assert!(!compiled.input.options.include_debug_sql, "{query}");
            assert!(
                has_param_value(&compiled.base.params, &serde_json::json!(value)),
                "{query}"
            );
            assert!(!compiled.base.sql.contains(value), "{query}");
        }
    }
}

#[test]
fn gql_prepares_queries_and_scoped_schema() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    assert!(matches!(
        prepare("MATCH (n:User {id: 1}) RETURN n", &ontology, &ctx, All),
        Ok(PreparedStatement::Query(_))
    ));
    for (scope, call, node) in [
        (All, "CALL db.schema()", None),
        (Local, "CALL db.schema()", None),
        (All, "CALL db.schema('MergeRequest')", Some("MergeRequest")),
        (Local, "CALL db.schema('File')", Some("File")),
    ] {
        let PreparedStatement::Schema(response) = prepare(call, &ontology, &ctx, scope).unwrap()
        else {
            panic!("expected schema: {call}");
        };
        if let Some(name) = node {
            assert_eq!(response.domains.len(), 1);
            assert_eq!(response.domains[0].nodes.len(), 1);
            let node = serde_json::to_value(&response.domains[0].nodes[0]).unwrap();
            assert_eq!(node["name"], name);
            assert!(!node["props"].as_array().unwrap().is_empty());
            assert!(response.edges.is_empty());
        } else {
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(build_compact_schema_response(&ontology, scope, None))
                    .unwrap(),
            );
        }
    }
    for (scope, call) in [
        (All, "CALL db.schema('Missing')"),
        (All, "CALL db.schema('*')"),
        (All, "CALL db.schema('File', 'User')"),
        (All, "CALL db.schema() YIELD name"),
        (Local, "CALL db.schema('MergeRequest')"),
    ] {
        assert!(
            prepare(call, &ontology, &ctx, scope)
                .unwrap_err()
                .is_client_safe()
        );
    }
    assert!(compile("CALL db.schema()", Frontend::Gql, &ontology, &ctx).is_err());
}

#[test]
fn cte_statements_carry_the_ctes_they_read() {
    let compiled = compile(
        "MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile) WHERE g.full_path = 'g701' RETURN p, count(f) AS c LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let statements = compiler::passes::codegen::clickhouse::cte_statements(&compiled.ast).unwrap();
    assert!(
        statements
            .iter()
            .any(|(_, sql, _)| sql.starts_with("WITH ")),
        "expected a CTE body that reads another CTE"
    );
    for (own, sql, _) in &statements {
        for (position, _) in sql.match_indices("_orbit_relation_") {
            let digits: String = sql[position + "_orbit_relation_".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            let name = format!("_orbit_relation_{digits}");
            assert!(
                &name == own || sql.contains(&format!("{name} AS ")),
                "{name} is read but not defined in: {sql}"
            );
        }
    }
}
