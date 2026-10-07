mod security;

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
fn query_graph_relationship_scope_contract() {
    use query_data_model::QueryDataModel;
    use query_engine::compiler::{
        self,
        input::Input,
        query_graph::{Expression as E, LoweredOperation as Operation, QueryGraph, ScanInput},
        scope::{self, ScopeProof},
    };
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut input: Input = serde_json::from_value(serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "caller", "entity": "Definition"}, {"id": "callee", "entity": "Definition"}],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}]
    })).unwrap();
    let proofs = ["caller", "callee"]
        .into_iter()
        .map(|alias| (alias.to_string(), ScopeProof::literal("1/42/")))
        .collect();
    let scope = scope::prepare(&mut input, proofs, model.as_ref());
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let table = model.relationship_table("CALLS").unwrap();
    assert!(!model.table_path_scopable(table));
    let edge = graph.scan(root, table, "edge").unwrap();
    graph.bind_scan(edge, ScanInput::Relationship(0)).unwrap();
    *graph.operation_mut(root).unwrap() = Operation::source(edge);
    graph
        .project(
            root,
            "id",
            E::Column(graph.stored_column(edge, "source_id").unwrap()),
        )
        .unwrap();
    scope::apply_graph(&mut graph, &scope, &input).unwrap();
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("startsWith") && sql.contains("1/42/"), "{sql}");
}

#[test]
fn query_graph_subquery_visibility_contract() {
    use query_engine::compiler::{
        self,
        query_graph::{Expression as E, GraphError, LoweredOperation as Operation, QueryGraph},
    };
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let body = graph.select(Operation::One);
    let key = graph.project(body, "id", E::Integer(7)).unwrap();
    let definition = graph.define(root, body, "keys", false).unwrap();
    let reference = graph.reference(root, definition, "keys").unwrap();
    let key = graph.output_column(reference, key).unwrap();
    graph.project(root, "id", E::Integer(7)).unwrap();
    *graph.operation_mut(root).unwrap() =
        Operation::One.filter(E::equal(E::Column(key), E::Integer(7)));
    assert!(matches!(
        graph.validate_lowered(root),
        Err(GraphError::OperationVisibility)
    ));
    *graph.operation_mut(root).unwrap() = Operation::One.filter(E::InQuery {
        value: Box::new(E::Integer(7)),
        key,
    });
    let (sql, _) = graph.render_parameterized(root).unwrap();
    assert!(sql.contains("IN (SELECT"), "{sql}");
}

#[test]
fn query_graph_scalar_subquery_contract() {
    use query_engine::compiler::{
        self,
        query_graph::{Expression as E, GraphError, LoweredOperation as Operation, QueryGraph},
    };
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let body = graph.select(Operation::One);
    let output = graph.project(body, "count", E::Count).unwrap();
    let relation = graph.derive(root, body, "scalar").unwrap();
    let value = graph.output_column(relation, output).unwrap();
    graph.project(root, "count", E::ScalarQuery(value)).unwrap();
    assert!(matches!(
        graph.validate_lowered(root),
        Err(GraphError::AggregatePlacement)
    ));
    *graph.operation_mut(body).unwrap() = Operation::One.aggregate(vec![]);
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("(SELECT") && sql.contains("COUNT(*)"), "{sql}");
    let foreign = graph.select(Operation::One);
    graph
        .project(foreign, "count", E::ScalarQuery(value))
        .unwrap();
    assert!(matches!(
        graph.render(foreign),
        Err(GraphError::OutsideBlock)
    ));
}

#[test]
fn query_graph_rebinds_nested_computations_between_blocks() {
    use query_data_model::QueryDataModel;
    use query_engine::compiler::{
        self,
        query_graph::{
            Expression as E, GraphError, LoweredOperation as Operation, Port, QueryGraph,
        },
    };
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let original = graph.select(Operation::One);
    let first = graph
        .scan(original, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let id = E::Column(graph.stored_column(first, "id").unwrap());
    let condition = E::Or(
        Box::new(E::equal(id.clone(), E::Integer(1))),
        Box::new(E::Greater(
            Box::new(E::Add(Box::new(id.clone()), Box::new(E::Integer(2)))),
            Box::new(E::Integer(10)),
        )),
    );
    let value = E::JsonObject(vec![
        (
            "count".into(),
            E::ToString(Box::new(E::CountIf(Box::new(condition.clone())))),
        ),
        (
            "sum".into(),
            E::ToString(Box::new(E::Sum {
                value: Box::new(id),
                condition: Some(Box::new(condition)),
            })),
        ),
        (
            "path".into(),
            E::LatestPath {
                path: graph.stored_column(first, "traversal_path").unwrap(),
                version: graph.stored_column(first, "_version").unwrap(),
                deletion: graph.stored_column(first, "_deleted").unwrap(),
            },
        ),
    ]);
    graph.project(original, "summary", value.clone()).unwrap();
    *graph.operation_mut(original).unwrap() = Operation::source(first).aggregate(vec![]);
    let root = graph.select(Operation::One);
    let second = graph
        .scan(root, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    *graph.operation_mut(root).unwrap() = Operation::source(second).aggregate(vec![]);
    let output = graph.project(root, "summary", value.clone()).unwrap();
    assert!(matches!(
        graph.validate_lowered(root),
        Err(GraphError::OutsideBlock)
    ));
    let rebound = value
        .rebind(&|column| {
            let Port::Stored(stored) = column.port() else {
                panic!("stored source")
            };
            graph.stored_port(second, stored)
        })
        .unwrap();
    graph.replace_output(output, rebound).unwrap();
    let (sql, params) = graph.render_parameterized(root).unwrap();
    assert!(
        sql.contains("argMaxOrNull") && sql.contains("countIf") && sql.contains("sumIf"),
        "{sql}"
    );
    assert!(params.values().any(|parameter| parameter.value == 10));
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
