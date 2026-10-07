mod security;

use compiler::query_graph::{
    Expression as E, GraphError, JoinKind, Port, QueryGraph, ReadMode, ScanInput,
};
use query_data_model::QueryDataModel;
use query_engine::compiler::{self, Frontend, HydrationPlan, QueryError, SecurityContext};
use std::convert::Infallible;

#[test]
fn wildcard_selection_preserves_empty_resolution() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    for (frontend, query) in [
        (
            Frontend::JsonDsl,
            r#"{"query_type":"traversal","nodes":[{"id":"n","entity":"Note","node_ids":[1]},{"id":"r","entity":"Runner"}],"relationships":[{"type":"*","from":"n","to":"r"}]}"#,
        ),
        (
            Frontend::Gql,
            "MATCH (n:Note {id: 1})-->(r:Runner) RETURN n, r",
        ),
    ] {
        let compiled = compiler::compile_graph(query, frontend, &model, &security).unwrap();
        assert_eq!(
            compiled.input.relationships[0].types,
            compiler::input::RelationshipSelection::Kinds(vec![])
        );
        assert!(compiled.base.render().contains("IN []"));
    }
    let mut query = serde_json::json!({"query_type":"neighbors","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"neighbors":{"direction":"both"}});
    let expected =
        compiler::compile_graph(&query.to_string(), Frontend::JsonDsl, &model, &security).unwrap();
    for selection in [serde_json::json!([]), serde_json::json!(["*"])] {
        query["neighbors"]["rel_types"] = selection;
        let compiled =
            compiler::compile_graph(&query.to_string(), Frontend::JsonDsl, &model, &security)
                .unwrap();
        assert_eq!(compiled.base.render(), expected.base.render());
    }
}

#[test]
fn yaml_plan_shapes() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/plan_shape/fixtures");
    integration_testkit::plan_shape::run_dir(&directory, super::setup::embedded_ontology());
}

#[test]
fn query_graph_request_contract() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let mut query = serde_json::json!({
        "query_type": "traversal", "nodes": [{"id": "u", "entity": "User", "filters": {"username": "alice"}, "columns": ["username"]}],
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
    for expected in ["LIMIT 11", "_gkg_cursor_0", "substringUTF8"] {
        assert!(first.base.sql.contains(expected));
    }
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
    assert!(compiled.base.sql.contains("argMaxOrNull") && compiled.base.sql.contains("startsWith"));
    assert!(compiled.base.result_context.get("p").is_some());
    assert!(matches!(
        compiler::compile_graph(
            project,
            Frontend::JsonDsl,
            &model,
            &SecurityContext::new(1, vec![]).unwrap()
        ),
        Err(QueryError::Security(_))
    ));
}

#[test]
fn query_graph_relationship_scope_contract() {
    use compiler::scope::{self, ScopeProof};
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut input: compiler::Input = serde_json::from_value(serde_json::json!({
        "query_type": "traversal", "nodes": [{"id":"caller","entity":"Definition"},{"id":"callee","entity":"Definition"}],
        "relationships": [{"type":"CALLS","from":"caller","to":"callee"}]
    })).unwrap();
    let proofs = ["caller", "callee"]
        .into_iter()
        .map(|alias| (alias.to_string(), ScopeProof::literal("1/42/")))
        .collect();
    let scope = scope::prepare(&mut input, proofs, model.as_ref());
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let table = model.relationship_table("CALLS").unwrap();
    assert!(!model.table_path_scopable(table));
    let edge = graph.scan(root, table, "edge").unwrap();
    graph.bind_scan(edge, ScanInput::Relationship(0)).unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(edge, ReadMode::Raw).unwrap(),
            [(
                "id".into(),
                E::Column(graph.stored_column(edge, "source_id").unwrap()),
            )],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let graph = scope::apply_graph(graph, root, &scope, &input).unwrap();
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("startsWith") && sql.contains("1/42/"), "{sql}");
}

#[test]
fn query_graph_subquery_visibility_contract() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let body = graph.query();
    let projection = graph
        .project_values(
            graph.unit_relation(body).unwrap(),
            [("id".into(), E::Integer(7))],
        )
        .unwrap();
    let key = projection.outputs().next().unwrap().0;
    graph.finish_query(projection).unwrap();
    let definition = graph.define(root, body, "keys").unwrap();
    let reference = graph.reference(root, definition, "keys").unwrap();
    let key = graph.output_column(reference, key).unwrap();
    assert!(matches!(
        graph.filter_relation(
            graph.unit_relation(root).unwrap(),
            E::equal(E::Column(key), E::Integer(7))
        ),
        Err(GraphError::OperationVisibility)
    ));
    let source = graph
        .filter_relation(
            graph.unit_relation(root).unwrap(),
            E::InQuery {
                value: Box::new(E::Integer(7)),
                key,
            },
        )
        .unwrap();
    let projection = graph
        .project_values(source, [("id".into(), E::Integer(7))])
        .unwrap();
    graph.finish_query(projection).unwrap();
    let (sql, _) = graph.render_parameterized(root).unwrap();
    assert!(sql.contains("IN (SELECT"), "{sql}");
}

#[test]
fn query_graph_scalar_subquery_contract() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let body = graph.query();
    assert!(matches!(
        graph.project_values(
            graph.unit_relation(body).unwrap(),
            [("count".into(), E::Count)]
        ),
        Err(GraphError::AggregatePlacement)
    ));
    let aggregate = graph
        .aggregate_relation(graph.unit_relation(body).unwrap(), vec![])
        .unwrap();
    let projection = graph
        .project_values(aggregate, [("count".into(), E::Count)])
        .unwrap();
    let output = projection.outputs().next().unwrap().0;
    graph.finish_query(projection).unwrap();
    let scalar = graph.scalar_query(root, output, "scalar").unwrap();
    let foreign = graph.query();
    assert!(matches!(
        graph.project_values(
            graph.unit_relation(foreign).unwrap(),
            [("count".into(), scalar.clone())]
        ),
        Err(GraphError::OutsideBlock)
    ));
    let projection = graph
        .project_values(
            graph.unit_relation(root).unwrap(),
            [("count".into(), scalar)],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("(SELECT") && sql.contains("COUNT(*)"), "{sql}");
}

#[test]
fn query_graph_rebinds_nested_computations_between_blocks() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let original = graph.query();
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
    let source = graph
        .aggregate_relation(graph.read_relation(first, ReadMode::Raw).unwrap(), vec![])
        .unwrap();
    let projection = graph
        .project_values(source, [("summary".into(), value.clone())])
        .unwrap();
    graph.finish_query(projection).unwrap();
    let root = graph.query();
    let second = graph
        .scan(root, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let source = graph
        .aggregate_relation(graph.read_relation(second, ReadMode::Raw).unwrap(), vec![])
        .unwrap();
    assert!(matches!(
        graph.project_values(source, [("summary".into(), value.clone())]),
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
    let source = graph
        .aggregate_relation(graph.read_relation(second, ReadMode::Raw).unwrap(), vec![])
        .unwrap();
    let projection = graph
        .project_values(source, [("summary".into(), rebound)])
        .unwrap();
    graph.finish_query(projection).unwrap();
    let (sql, params) = graph.render_parameterized(root).unwrap();
    assert!(
        sql.contains("argMaxOrNull") && sql.contains("countIf") && sql.contains("sumIf"),
        "{sql}"
    );
    assert!(params.values().any(|parameter| parameter.value == 10));
}

#[test]
fn query_graph_stored_identity_contract() {
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
    assert!(projects.resolve(id.id()).is_none() && foreign_users.resolve(id.id()).is_none());
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
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
    let source = graph
        .join_relations(
            graph.read_relation(first, ReadMode::Current).unwrap(),
            graph.read_relation(second, ReadMode::Current).unwrap(),
            JoinKind::Inner,
            E::equal(E::Column(first_id), E::Column(second_id)),
        )
        .unwrap();
    let projection = graph
        .project_values(
            source,
            [
                ("first_id".into(), E::Column(first_id)),
                ("second_id".into(), E::Column(second_id)),
            ],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let (sql, _) = graph.render_parameterized(root).unwrap();
    assert!(
        sql.contains("AS \"first_id\"") && sql.contains("AS \"second_id\""),
        "{sql}"
    );
}
