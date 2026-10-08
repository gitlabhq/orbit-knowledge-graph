use compiler::query_graph::api::{Error, Join, OperationKind, QueryGraph, Read, lit};
use query_data_model::QueryDataModel;
use std::sync::Arc;

#[test]
fn rewrite_cannot_attach_an_ancestor_as_a_subquery() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let child = q.subquery(|q| q.values([lit(1).named("value")]))?;
            q.from(child)
        })
        .unwrap();
    let result = graph.rewrite(root, |q, rows| {
        if matches!(rows.kind(), OperationKind::Unit) {
            q.from(root)?;
        }
        Ok::<_, Error>(rows)
    });
    assert!(matches!(result, Err(Error::Ownership)));
}

#[test]
fn lowered_result_rejects_rows_retained_before_lowering() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut retained = None;
    let root = graph
        .query(|q| {
            let projects = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let path = projects.column("traversal_path")?;
            let id = projects.column("id")?;
            let version = projects.column("_version")?;
            retained = Some(q.latest(projects, [path, id], version)?);
            q.values([lit(1).named("value")])
        })
        .unwrap();
    let result = graph
        .lower()
        .map_result(root, |_, _| Ok::<_, Error>(retained.take().unwrap()));
    assert!(matches!(result, Err(Error::Replacement)));
}

#[test]
fn result_shaping_and_page_wrapping_use_checked_columns() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph.query(|q| q.values([lit(7).named("id")])).unwrap();
    let mut graph = graph
        .lower()
        .map_result(root, |q, rows| {
            let id = rows.column("id")?;
            q.select(rows, [id.named("id"), lit("User").named("kind")])
        })
        .unwrap();
    let page = graph
        .query(|q| {
            let rows = q.from(root)?;
            let id = rows.column("id")?;
            let rows = q.filter(rows, id.gt(3))?;
            let rows = q.sort(rows, [id.asc()])?;
            q.limit(rows, 10)
        })
        .unwrap();
    let (sql, params) = graph.render(page).unwrap();
    assert!(sql.contains("ORDER BY") && sql.contains("LIMIT 10"));
    assert!(params.values().any(|value| value.value == "User"));
    assert!(matches!(graph.render(root), Err(Error::Ownership)));
    let result = graph.map_result(root, |q, rows| q.select_all(rows));
    assert!(matches!(result, Err(Error::Ownership)));
}

#[test]
fn identity_expression_edit_preserves_cross_join_and_membership_sql() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let values = q.values([lit(1).named("value")])?;
            let value = values.column("value")?;
            let keys = q.values([lit(1).named("key")])?;
            let key = keys.column("key")?;
            let values = q.filter_in(values, value, keys, key)?;
            let labels = q.values([lit("label").named("label")])?;
            q.cross_join(values, labels)
        })
        .unwrap();
    let graph = graph.lower();
    let before = graph.render(root).unwrap();
    let graph = graph
        .rewrite(root, |q, rows| {
            q.map_expressions(rows, |value| Ok(value.clone()))
        })
        .unwrap();
    assert_eq!(graph.render(root).unwrap(), before);
}

#[test]
fn membership_edit_rejects_reversed_input_references() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let mut reversed = None;
    let root = graph
        .query(|q| {
            let values = q.values([lit(1).named("value")])?;
            let value = values.column("value")?;
            let keys = q.values([lit(1).named("key")])?;
            let key = keys.column("key")?;
            reversed = Some(key.eq(&value));
            q.filter_in(values, value, keys, key)
        })
        .unwrap();
    let result = graph.rewrite(root, |q, rows| {
        if matches!(
            rows.kind(),
            OperationKind::Join {
                kind: Join::Membership,
                ..
            }
        ) {
            q.map_expressions(rows, |_| Ok(reversed.as_ref().unwrap().clone()))
        } else {
            Ok(rows)
        }
    });
    assert!(matches!(result, Err(Error::Column)));
}

#[test]
fn lowered_rewrite_rejects_latest_in_a_new_subquery() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph.query(|q| q.values([lit(1).named("value")])).unwrap();
    let result = graph.lower().rewrite(root, |q, rows| {
        q.subquery(|q| {
            let projects = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let path = projects.column("traversal_path")?;
            let id = projects.column("id")?;
            let version = projects.column("_version")?;
            q.latest(projects, [path, id], version)
        })?;
        Ok::<_, Error>(rows)
    });
    assert!(matches!(result, Err(Error::Latest)));
}

#[test]
fn lowered_rewrite_preserves_the_first_by_ordering_boundary() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let projects = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let path = projects.column("traversal_path")?;
            let id = projects.column("id")?;
            let version = projects.column("_version")?;
            let projects = q.latest(projects, [path, id.clone()], version)?;
            q.select(projects, [id.named("id")])
        })
        .unwrap();
    let result = graph.lower().rewrite(root, |q, rows| {
        if matches!(rows.kind(), OperationKind::Sort { .. }) {
            q.filter(rows, lit(true))
        } else {
            Ok(rows)
        }
    });
    assert!(matches!(result, Err(Error::Replacement)));
}
