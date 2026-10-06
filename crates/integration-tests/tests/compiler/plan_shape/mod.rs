#[test]
fn yaml_plan_shapes() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/plan_shape/fixtures");
    integration_testkit::plan_shape::run_dir(&directory, super::setup::embedded_ontology());
}

#[test]
fn query_graph_request_contract() {
    use query_engine::compiler::{self, Frontend, HydrationPlan, QueryError, SecurityContext};
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let mut query = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {"username": "alice"}, "columns": ["username"]}],
        "cursor": {"page_size": 10}
    });
    let first =
        compiler::compile_graph(&query.to_string(), Frontend::JsonDsl, &model, &security).unwrap();
    assert_eq!(first.pagination.key_count, 1);
    assert_eq!(
        first.base.result_context.get("u").unwrap().entity_type,
        "User"
    );
    assert!(matches!(first.hydration, HydrationPlan::None));
    assert!(first.base.sql.contains("LIMIT 11"));
    assert!(first.base.sql.contains("_gkg_cursor_0"));
    assert!(first.base.sql.contains("substringUTF8"));
    assert!(!first.base.sql.contains("alice"));
    assert!(
        first
            .base
            .params
            .values()
            .any(|parameter| parameter.value == "alice")
    );

    query["cursor"]["after"] =
        compiler::passes::cursor::encode(first.pagination.query_hash, &[Some("42".into())]).into();
    let next =
        compiler::compile_graph(&query.to_string(), Frontend::JsonDsl, &model, &security).unwrap();
    assert_eq!(first.pagination.query_hash, next.pagination.query_hash);
    assert!(
        next.base
            .params
            .values()
            .any(|parameter| parameter.value == 42)
    );
    query["nodes"][0]["filters"]["username"] = "bob".into();
    assert!(matches!(
        compiler::compile_graph(&query.to_string(), Frontend::JsonDsl, &model, &security),
        Err(QueryError::PaginationError(_))
    ));

    let project = r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","node_ids":[7],"columns":["name"]}]}"#;
    let compiled = compiler::compile_graph(project, Frontend::JsonDsl, &model, &security).unwrap();
    assert!(compiled.base.sql.contains("argMaxOrNull"));
    assert!(compiled.base.sql.contains("startsWith"));
    assert!(compiled.base.result_context.get("p").is_some());
    let empty = SecurityContext::new(1, vec![]).unwrap();
    assert!(matches!(
        compiler::compile_graph(project, Frontend::JsonDsl, &model, &empty),
        Err(QueryError::Security(_))
    ));
}

#[test]
fn query_graph_stored_identity_contract() {
    use query_data_model::QueryDataModel;
    use query_engine::compiler::{
        self,
        query_graph::{Expression, LoweredOperation, QueryGraph},
    };
    let ontology = super::setup::embedded_ontology();
    let model = compiler::data_model::clickhouse(ontology.clone()).unwrap();
    let other = compiler::data_model::clickhouse(ontology).unwrap();
    let users = model
        .stored_table(model.entity_table("User").unwrap())
        .unwrap();
    let projects = model
        .stored_table(model.entity_table("Project").unwrap())
        .unwrap();
    let foreign_users = other
        .stored_table(other.entity_table("User").unwrap())
        .unwrap();
    let id = users.column("id").unwrap();
    assert_eq!(users.resolve(id.id()), Some(id));
    assert!(projects.resolve(id.id()).is_none());
    assert!(foreign_users.resolve(id.id()).is_none());

    let mut graph = QueryGraph::<_, Expression<'_>, LoweredOperation<'_>>::new(model.as_ref());
    let root = graph.select(LoweredOperation::One);
    let first = graph.scan_stored(root, users, "first").unwrap();
    let second = graph.scan_stored(root, users, "second").unwrap();
    let first_id = graph.stored_port(first, id).unwrap();
    let second_id = graph.stored_port(second, id).unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(first_id.port(), second_id.port());
    assert!(graph.scan_stored(root, foreign_users, "foreign").is_err());
    assert!(
        graph
            .stored_port(first, projects.column("id").unwrap())
            .is_err()
    );
    assert!(
        graph
            .stored_port(first, foreign_users.column("id").unwrap())
            .is_err()
    );
    graph
        .project(root, "first_id", Expression::Column(first_id))
        .unwrap();
    graph
        .project(root, "second_id", Expression::Column(second_id))
        .unwrap();
    *graph.operation_mut(root).unwrap() = LoweredOperation::current(first).join(
        LoweredOperation::current(second),
        Expression::equal(Expression::Column(first_id), Expression::Column(second_id)),
    );
    let (sql, _) = graph.render_parameterized(root).unwrap();
    assert!(sql.contains("AS \"first_id\""), "{sql}");
    assert!(sql.contains("AS \"second_id\""), "{sql}");
}
