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
