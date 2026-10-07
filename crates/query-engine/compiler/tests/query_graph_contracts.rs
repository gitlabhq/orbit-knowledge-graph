use compiler::query_graph::{Expression, GraphError, QueryGraph, ReadMode};
use std::convert::Infallible;
use std::sync::Arc;

#[test]
fn cte_references_require_explicit_scope_and_single_ownership() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let body = graph.query();
    let projection = graph
        .project_values(
            graph.unit_relation(body).unwrap(),
            [("id".into(), Expression::Integer(7))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let output = graph.outputs(body).unwrap().next().unwrap();
    let definition = graph.define(root, body, "keys").unwrap();
    let unrelated = graph.query();
    assert_eq!(
        graph
            .reference(unrelated, definition, "hidden")
            .unwrap_err(),
        GraphError::DefinitionVisibility
    );
    assert_eq!(
        graph.derive(unrelated, body, "shared").unwrap_err(),
        GraphError::BlockOwnership
    );

    let consumer = graph.query_in(root).unwrap();
    let relation = graph.reference(consumer, definition, "keys").unwrap();
    let column = graph.output_column(relation, output).unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(relation, ReadMode::Raw).unwrap(),
            [("id".into(), Expression::Column(column))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    assert_eq!(
        graph.derive(unrelated, consumer, "escaped").unwrap_err(),
        GraphError::DefinitionVisibility
    );
    let relation = graph.derive(root, consumer, "consumer").unwrap();
    let output = graph.outputs(consumer).unwrap().next().unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(relation, ReadMode::Raw).unwrap(),
            [(
                "id".into(),
                Expression::Column(graph.output_column(relation, output).unwrap()),
            )],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let sql = graph.render(root).unwrap();
    assert!(sql.starts_with("WITH "), "{sql}");
    assert!(sql.contains("7 AS"), "{sql}");
}

#[test]
fn substitution_preserves_consumers_and_rejects_changed_output_types() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let body = graph.query();
    let projection = graph
        .project_values(
            graph.unit_relation(body).unwrap(),
            [("id".into(), Expression::Integer(7))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    let output = graph.outputs(body).unwrap().next().unwrap();
    let root = graph.query();
    let source = graph.derive(root, body, "source").unwrap();
    let reference = graph.output_column(source, output).unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(source, ReadMode::Raw).unwrap(),
            [("id".into(), Expression::Column(reference))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();

    let original = graph.render(root).unwrap();
    let invalid = graph
        .project_values(
            graph.unit_relation(body).unwrap(),
            [("id".into(), Expression::Text("wrong".into()))],
        )
        .unwrap();
    assert_eq!(
        graph.substitute_query(invalid).unwrap_err(),
        GraphError::OutputContract
    );
    assert_eq!(graph.render(root).unwrap(), original);

    let replacement = graph
        .project_values(
            graph.unit_relation(body).unwrap(),
            [("id".into(), Expression::Integer(9))],
        )
        .unwrap();
    graph.substitute_query(replacement).unwrap();
    assert_eq!(graph.output_column(source, output).unwrap(), reference);
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("9 AS") && !sql.contains("7 AS"), "{sql}");
    assert_eq!(
        graph
            .rewrite_output(output, Expression::Boolean(true))
            .unwrap_err(),
        GraphError::OutputContract
    );
    assert_eq!(graph.render(root).unwrap(), sql);

    let graph = graph
        .rewrite_query(body, |graph, operation| {
            let (input, outputs) = operation.into_projection()?;
            let filtered = graph.filter_relation(input, Expression::Boolean(false))?;
            graph.project_values(filtered, outputs)
        })
        .unwrap();
    assert_eq!(graph.output_column(source, output).unwrap(), reference);
    let sql = graph.render(root).unwrap();
    assert!(sql.contains("WHERE false"), "{sql}");
}

#[test]
fn connections_reject_nonboolean_filters_and_foreign_operations() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    assert!(matches!(
        graph.filter_relation(graph.unit_relation(root).unwrap(), Expression::Integer(1)),
        Err(GraphError::ExpressionType)
    ));
    let mut other = QueryGraph::<_, Infallible>::new(model.as_ref());
    let foreign = other.query();
    assert!(matches!(
        graph.limit_relation(other.unit_relation(foreign).unwrap(), 1),
        Err(GraphError::ForeignGraph)
    ));
    assert!(matches!(
        graph.project_values(
            other.unit_relation(foreign).unwrap(),
            [("id".into(), Expression::Integer(1))]
        ),
        Err(GraphError::ForeignGraph)
    ));
}

#[test]
fn union_checks_types_before_claiming_its_arms() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let mut arms = Vec::new();
    for value in [Expression::Integer(1), Expression::Text("wrong".into())] {
        let block = graph.query();
        let projection = graph
            .project_values(
                graph.unit_relation(block).unwrap(),
                [("value".into(), value)],
            )
            .unwrap();
        graph.finish_query(projection).unwrap();
        arms.push(block);
    }
    assert_eq!(
        graph
            .union_all(arms.clone(), vec!["value".into()])
            .unwrap_err(),
        GraphError::UnionType
    );
    assert_eq!(
        graph
            .union_all(vec![arms[0], arms[0]], vec!["value".into()])
            .unwrap_err(),
        GraphError::BlockOwnership
    );
    let root = graph.query();
    assert!(graph.derive(root, arms[0], "still_available").is_ok());
    assert!(graph.derive(root, arms[1], "also_available").is_ok());
}
