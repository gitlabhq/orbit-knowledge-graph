use compiler::query_graph::{
    Expression as E, GraphError, JoinKind, LatestRows, OperationKind, QueryGraph, ReadMode,
};
use query_data_model::QueryDataModel;
use std::convert::Infallible;
use std::sync::Arc;

#[test]
fn recursive_rewrite_is_bottom_up_and_visits_shared_queries_once() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, LatestRows<'_>>::new(model.as_ref());
    let root = graph.query();
    let body = graph.query();
    let scan = graph
        .scan(body, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let id = graph.stored_column(scan, "id").unwrap();
    let source = graph
        .latest_relation(
            graph.read_relation(scan, ReadMode::Raw).unwrap(),
            graph.stored_column(scan, "_version").unwrap(),
            None,
        )
        .unwrap();
    let projection = graph
        .project_values(source, [("id".into(), E::Column(id))])
        .unwrap();
    let output = projection.outputs().next().unwrap().0;
    graph.finish_query(projection).unwrap();
    let definition = graph.define(root, body, "projects").unwrap();
    let left = graph.reference(root, definition, "left").unwrap();
    let right = graph.reference(root, definition, "right").unwrap();
    let left_id = graph.output_column(left, output).unwrap();
    let right_id = graph.output_column(right, output).unwrap();
    let joined = graph
        .join_relations(
            graph.read_relation(left, ReadMode::Raw).unwrap(),
            graph.read_relation(right, ReadMode::Raw).unwrap(),
            JoinKind::Inner,
            E::equal(E::Column(left_id), E::Column(right_id)),
        )
        .unwrap();
    let projection = graph
        .project_values(joined, [("id".into(), E::Column(left_id))])
        .unwrap();
    graph.finish_query(projection).unwrap();
    let unused = graph.query();
    let projection = graph
        .project_values(
            graph.unit_relation(unused).unwrap(),
            [("id".into(), E::Integer(0))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let mut visited = Vec::new();
    let mut stored_reads = 0;
    let mut inserted = None;
    let graph = graph
        .rewrite_operations(root, |graph, operation| match operation.kind() {
            OperationKind::Source { relation, .. } => {
                visited.push(*relation);
                if *relation == scan {
                    stored_reads += 1;
                    let block = graph.query();
                    let projection = graph.project_values(
                        graph.unit_relation(block)?,
                        [("id".into(), E::Integer(0))],
                    )?;
                    graph.finish_query(projection)?;
                    inserted = Some(block);
                }
                graph.filter_relation(operation, E::Boolean(true))
            }
            OperationKind::Join { .. } => {
                assert_eq!(visited, vec![left, right]);
                assert!(
                    operation
                        .inputs()
                        .all(|input| matches!(input.kind(), OperationKind::Filter { .. }))
                );
                Ok(operation)
            }
            OperationKind::Latest { input, .. } => {
                assert!(matches!(input.kind(), OperationKind::Filter { .. }));
                Ok(operation)
            }
            _ => panic!("new filters and unattached queries must not be revisited"),
        })
        .unwrap();
    assert_eq!(stored_reads, 1);
    assert_eq!(graph.output_column(left, output).unwrap(), left_id);
    assert!(matches!(
        graph.operation(unused).unwrap().kind(),
        OperationKind::One
    ));
    assert!(matches!(
        graph.operation(inserted.unwrap()).unwrap().kind(),
        OperationKind::One
    ));
    let sql = graph.lower_operations().render(root).unwrap();
    assert!(
        sql.contains("LIMIT 1 BY") && sql.contains("WHERE true"),
        "{sql}"
    );
}

#[test]
fn recursive_rewrite_rejects_changed_parent_contracts() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    for change in ["columns", "grouping", "expansion", "foreign", "occurrence"] {
        let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
        let root = graph.query();
        let scan = graph
            .scan(root, model.default_edge_table(), "edge")
            .unwrap();
        let id = graph.stored_column(scan, "source_id").unwrap();
        let tags = graph.stored_column(scan, "source_tags").unwrap();
        let other = graph
            .scan(root, model.default_edge_table(), "other")
            .unwrap();
        let source = graph.read_relation(scan, ReadMode::Raw).unwrap();
        let projection = graph
            .project_values(source, [("id".into(), E::Column(id))])
            .unwrap();
        graph.finish_query(projection).unwrap();
        let mut foreign = QueryGraph::<_, Infallible>::new(model.as_ref());
        let block = foreign.query();
        let result = graph.rewrite_operations(root, |graph, operation| match change {
            "columns" => graph.unit_relation(root),
            "grouping" => graph.aggregate_relation(operation, vec![E::Column(id)]),
            "expansion" => graph.expand_relation(operation, tags),
            "foreign" => foreign.unit_relation(block),
            "occurrence" => graph.read_relation(other, ReadMode::Raw),
            _ => unreachable!(),
        });
        assert!(
            matches!(result, Err(GraphError::OutputContract)),
            "{change}"
        );
    }
}

#[test]
fn recursive_rewrite_propagates_callback_errors() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let projection = graph
        .project_values(
            graph.unit_relation(root).unwrap(),
            [("id".into(), E::Integer(1))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let result = graph.rewrite_operations(root, |_, _| {
        Err::<_, compiler::QueryError>(compiler::QueryError::Security("stop".into()))
    });
    assert!(matches!(result, Err(compiler::QueryError::Security(message)) if message == "stop"));
}
