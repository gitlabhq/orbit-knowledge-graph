mod security;

use compiler::query_graph::api::*;
use query_data_model::QueryDataModel;
use query_engine::compiler::{self, Frontend, HydrationPlan, QueryError, SecurityContext};

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
    let mut graph = QueryGraph::new(model.as_ref());
    let table = model.relationship_table("CALLS").unwrap();
    assert!(!model.table_path_scopable(table));
    let root = graph
        .query(|q| {
            let edge = q.scan(table, Read::Raw)?.labeled("e0")?;
            let id = edge.column("source_id")?;
            q.select(edge, [id.named("id")])
        })
        .unwrap();
    let graph = scope::apply_graph(graph.lower(), root, &scope, &input).unwrap();
    let (sql, params) = graph.render(root).unwrap();
    assert!(sql.contains("startsWith"));
    assert!(params.values().any(|parameter| parameter.value == "1/42/"));
}

#[test]
fn query_graph_subquery_visibility_contract() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let definition = q.cte("keys", |q| q.values([lit(7).named("id")]))?;
            let keys = q.read(definition)?;
            let key = keys.column("id")?;
            let rows = q.values([lit(7).named("id")])?;
            assert!(matches!(q.filter(rows, key.eq(7)), Err(Error::Column)));
            let rows = q.values([lit(7).named("id")])?;
            let id = rows.column("id")?;
            q.filter_in(rows, id, keys, key)
        })
        .unwrap();
    let (sql, _) = graph.lower().render(root).unwrap();
    assert!(sql.contains("IN (SELECT"), "{sql}");
}

#[test]
fn query_graph_scalar_subquery_contract() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut escaped = None;
    let root = graph
        .query(|q| {
            assert!(matches!(
                q.values([count().named("count")]),
                Err(Error::Aggregate)
            ));
            let body = q.subquery(|q| {
                let rows = q.values([lit(1).named("id")])?;
                q.aggregate(rows, [], [count().named("count")])
            })?;
            let scalar = q.scalar(body, "count")?;
            escaped = Some(scalar.clone());
            q.values([scalar.named("count")])
        })
        .unwrap();
    assert!(matches!(
        graph.query(|q| q.values([escaped.unwrap().named("count")])),
        Err(Error::Scope)
    ));
    let (sql, _) = graph.lower().render(root).unwrap();
    assert!(sql.contains("(SELECT") && sql.contains("count()"), "{sql}");
}

#[test]
fn query_graph_rebinds_nested_computations_between_blocks() {
    let model = compiler::data_model::clickhouse(super::setup::embedded_ontology()).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut escaped = None;
    graph
        .query(|q| {
            let rows = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let id = rows.column("id")?;
            escaped = Some(count().filter(id.eq(1).or(id.add(2).gt(10))));
            q.select(rows, [id.named("id")])
        })
        .unwrap();
    let root = graph
        .query(|q| {
            let rows = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let measure = escaped.unwrap().rewrite(&mut |value| {
                if let ExprKind::Column(column) = value.kind() {
                    Ok(rows.column(column.name())?.expr())
                } else {
                    Ok(value)
                }
            })?;
            let id = rows.column("id")?;
            let condition = id.eq(1).or(id.add(2).gt(10));
            let path = rows.column("traversal_path")?;
            let version = rows.column("_version")?;
            let rows = q.aggregate(
                rows,
                [],
                [
                    measure.named("count"),
                    Expr::aggregate(Aggregate::Sum, [id.expr()])
                        .filter(condition)
                        .named("sum"),
                    Expr::aggregate(Aggregate::ArgMax, [path.expr(), version.expr()]).named("path"),
                ],
            )?;
            let fields = ["count", "sum", "path"]
                .into_iter()
                .map(|name| {
                    rows.column(name)
                        .map(|column| Expr::call(Function::ToString, [column.expr()]))
                })
                .collect::<Result<Vec<_>>>()?;
            q.select(
                rows,
                [Expr::call(
                    Function::JsonObject(vec!["count".into(), "sum".into(), "path".into()]),
                    fields,
                )
                .named("summary")],
            )
        })
        .unwrap();
    let (sql, params) = graph.lower().render(root).unwrap();
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
    let mut graph = QueryGraph::new(model.as_ref());
    let mut foreign = None;
    let mut other_graph = QueryGraph::new(other.as_ref());
    other_graph
        .query(|q| {
            let rows = q.scan(foreign_users.name(), Read::Current)?;
            foreign = Some(rows.column("id")?);
            Ok(rows)
        })
        .unwrap();
    let root = graph
        .query(|q| {
            let first = q.scan(users.name(), Read::Current)?;
            let second = q.scan(users.name(), Read::Current)?;
            let first_id = first.column("id")?;
            let second_id = second.column("id")?;
            assert_ne!(first_id, second_id);
            let invalid = q.scan(users.name(), Read::Current)?;
            assert!(matches!(
                q.filter(invalid, foreign.as_ref().unwrap().eq(7)),
                Err(Error::Scope)
            ));
            let rows = q.join(first, second, first_id.eq(&second_id))?;
            q.select(
                rows,
                [first_id.named("first_id"), second_id.named("second_id")],
            )
        })
        .unwrap();
    let (sql, _) = graph.lower().render(root).unwrap();
    assert!(
        sql.contains("AS \"first_id\"") && sql.contains("AS \"second_id\""),
        "{sql}"
    );
}
