use compiler::query_graph::{Error, Function, QueryGraph, Read, count, lit};
use query_data_model::QueryDataModel;
use std::sync::Arc;

#[test]
fn labeled_columns_resolve_self_joins_but_cannot_cross_projection_boundaries() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let table = model.entity_table("User").unwrap();
            let author = q.scan(table, Read::Current)?.labeled("author")?;
            let reviewer = q.scan(table, Read::Current)?.labeled("reviewer")?;
            let author_id = author.column("id")?;
            let reviewer_id = reviewer.column("id")?;
            let rows = q.cross_join(author, reviewer)?;
            assert_eq!(rows.column_from("author", "id")?, author_id);
            assert_eq!(rows.column_from("reviewer", "id")?, reviewer_id);
            assert!(matches!(rows.column("id"), Err(Error::Column)));
            let rows = q.select(
                rows,
                [author_id.named("author"), reviewer_id.named("reviewer")],
            )?;
            assert!(matches!(
                rows.column_from("author", "id"),
                Err(Error::Column)
            ));
            q.select_all(rows)
        })
        .unwrap();
    let (sql, _) = graph.lower().render(root).unwrap();
    assert!(sql.contains("CROSS JOIN"));
}

#[test]
fn typed_literals_reject_incompatible_bound_values() {
    use compiler::query_graph::api::Expr;
    use orbit_utils::query_types::SqlType;
    use serde_json::json;

    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    for (data_type, value) in [
        (SqlType::Bool, json!("true")),
        (SqlType::UInt32, json!(-1)),
        (SqlType::UInt32, json!(u64::from(u32::MAX) + 1)),
        (SqlType::Int64.to_array(), json!([1, "two"])),
        (SqlType::Date, json!(42)),
    ] {
        let result = graph.query(|q| q.values([Expr::literal(data_type, value).named("invalid")]));
        assert_eq!(result.unwrap_err(), Error::Type);
    }
}

#[test]
fn query_scopes_reject_foreign_columns_and_hidden_definitions() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut escaped = None;
    let mut hidden = None;
    graph
        .query(|q| {
            hidden = Some(q.cte("keys", |q| q.values([lit(1).named("id")]))?);
            let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
            escaped = Some(users.column("id")?);
            q.select_all(users)
        })
        .unwrap();
    let result = graph.query(|q| {
        let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
        q.filter(users, escaped.as_ref().unwrap().eq(1))
    });
    assert_eq!(result.unwrap_err(), Error::Scope);
    assert_eq!(
        graph.query(|q| q.read(hidden.unwrap())).unwrap_err(),
        Error::CteScope
    );
}

#[test]
fn query_connections_reject_duplicate_ownership_and_union_type_changes() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    graph
        .query(|q| {
            let first = q.subquery(|q| q.values([lit(1).named("value")]))?;
            let second = q.subquery(|q| q.values([lit("wrong").named("value")]))?;
            assert!(matches!(q.union_all([first, second]), Err(Error::Outputs)));
            assert!(matches!(q.union_all([first, first]), Err(Error::Ownership)));
            let rows = q.from(first)?;
            assert!(matches!(q.from(first), Err(Error::Ownership)));
            q.select_all(rows)
        })
        .unwrap();
}

#[test]
fn aggregation_hides_input_columns_and_rejects_nested_measures() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let result = graph.query(|q| {
        let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
        let id = users.column("id")?;
        let totals = q.aggregate(users, [], [count().named("total")])?;
        q.filter(totals, id.eq(1))
    });
    assert_eq!(result.unwrap_err(), Error::Column);
    let result = graph.query(|q| {
        let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
        q.aggregate(users, [], [count().filter(count().gt(0)).named("invalid")])
    });
    assert_eq!(result.unwrap_err(), Error::Aggregate);
}

#[test]
fn scalar_subquery_is_reachable_and_cannot_lose_its_cardinality() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut scalar_id = None;
    let root = graph
        .query(|q| {
            let scalar = q.subquery(|q| {
                let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
                q.aggregate(users, [], [count().named("count")])
            })?;
            scalar_id = Some(scalar);
            let count = q.scalar(scalar, "count")?;
            q.values([count.named("count")])
        })
        .unwrap();
    assert!(graph.reachable(root).unwrap().contains(&scalar_id.unwrap()));
    let result = graph.rewrite(root, |q, rows| {
        if matches!(
            rows.kind(),
            compiler::query_graph::OperationKind::Aggregate { .. }
        ) {
            q.filter(rows, lit(false))
        } else {
            Ok(rows)
        }
    });
    assert!(matches!(result, Err(Error::Replacement)));
}

#[test]
fn expansion_preserves_array_input_and_adds_an_element_column() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    graph
        .query(|q| {
            let rows = q.values([compiler::query_graph::array([1, 2]).named("items")])?;
            let array = rows.column("items")?;
            let rows = q.expand(rows, array.named("item"))?;
            let item = rows.column("item")?;
            assert!(matches!(
                array.data_type(),
                compiler::query_graph::ValueType::Array(_)
            ));
            assert!(matches!(
                item.data_type(),
                compiler::query_graph::ValueType::Scalar(_)
            ));
            assert_ne!(array, item);
            let invalid = compiler::query_graph::Expr::call(Function::TupleField(0), [item.expr()]);
            assert!(matches!(
                q.select(rows, [invalid.named("invalid")]),
                Err(Error::Type)
            ));
            q.values([lit(1).named("done")])
        })
        .unwrap();
}
