use compiler::passes::{check::check_graph, security::apply_graph_security};
use compiler::query_graph::{Expression as E, JoinKind, OperationKind, QueryGraph, ReadMode};
use query_data_model::QueryDataModel;
use query_engine::compiler::{self, AccessLevel, AuthorizedPath, SecurityContext};
use std::convert::Infallible;

#[test]
fn authorization_covers_every_nested_scan_occurrence() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    for position in [
        "derived",
        "definition",
        "nested_definition",
        "union",
        "scalar",
        "scalar_filter",
        "membership",
        "join",
    ] {
        let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
        let root = graph.query();
        let body = graph.query();
        let scan = graph
            .scan(body, model.entity_table("Project").unwrap(), "same_hint")
            .unwrap();
        let id = graph.stored_column(scan, "id").unwrap();
        let scalar = matches!(position, "scalar" | "scalar_filter");
        let mut source = graph.read_relation(scan, ReadMode::Current).unwrap();
        if scalar {
            source = graph.aggregate_relation(source, vec![]).unwrap();
        }
        let value = if scalar { E::Count } else { E::Column(id) };
        let projection = graph
            .project_values(source, [("id".into(), value.clone())])
            .unwrap();
        let output = projection.outputs().next().unwrap().0;
        graph.finish_query(projection).unwrap();
        let (source, value) = match position {
            "scalar" | "scalar_filter" => {
                let scalar = graph.scalar_query(root, output, "scalar").unwrap();
                let source = graph.unit_relation(root).unwrap();
                if position == "scalar_filter" {
                    (
                        graph
                            .filter_relation(source, E::equal(scalar, E::Integer(1)))
                            .unwrap(),
                        E::Integer(1),
                    )
                } else {
                    (source, scalar)
                }
            }
            "nested_definition" => {
                let wrapper = graph.query();
                let definition = graph.define(wrapper, body, "inner").unwrap();
                let reference = graph.reference(wrapper, definition, "inner").unwrap();
                let projection = graph
                    .project_values(
                        graph.read_relation(reference, ReadMode::Raw).unwrap(),
                        [(
                            "id".into(),
                            E::Column(graph.output_column(reference, output).unwrap()),
                        )],
                    )
                    .unwrap();
                let output = projection.outputs().next().unwrap().0;
                graph.finish_query(projection).unwrap();
                let definition = graph.define(root, wrapper, "outer").unwrap();
                let reference = graph.reference(root, definition, "outer").unwrap();
                (
                    graph.read_relation(reference, ReadMode::Raw).unwrap(),
                    E::Column(graph.output_column(reference, output).unwrap()),
                )
            }
            "definition" | "membership" => {
                let definition = graph.define(root, body, "keys").unwrap();
                let reference = graph.reference(root, definition, "keys").unwrap();
                let key = graph.output_column(reference, output).unwrap();
                if position == "membership" {
                    (
                        graph
                            .filter_relation(
                                graph.unit_relation(root).unwrap(),
                                E::InQuery {
                                    value: Box::new(E::Integer(1)),
                                    key,
                                },
                            )
                            .unwrap(),
                        E::Integer(1),
                    )
                } else {
                    (
                        graph.read_relation(reference, ReadMode::Raw).unwrap(),
                        E::Column(key),
                    )
                }
            }
            "union" => {
                let other = graph.query();
                let scan = graph
                    .scan(other, model.default_edge_table(), "same_hint")
                    .unwrap();
                let projection = graph
                    .project_values(
                        graph.read_relation(scan, ReadMode::Raw).unwrap(),
                        [(
                            "id".into(),
                            E::Column(graph.stored_column(scan, "source_id").unwrap()),
                        )],
                    )
                    .unwrap();
                graph.finish_query(projection).unwrap();
                let union = graph
                    .union_all(vec![body, other], vec!["id".into()])
                    .unwrap();
                let reference = graph.derive(root, union, "union").unwrap();
                (
                    graph.read_relation(reference, ReadMode::Raw).unwrap(),
                    E::Column(graph.column(reference, "id").unwrap()),
                )
            }
            _ => {
                let reference = graph.derive(root, body, "derived").unwrap();
                let value = graph.output_column(reference, output).unwrap();
                let mut operation = graph.read_relation(reference, ReadMode::Raw).unwrap();
                if position == "join" {
                    let other = graph
                        .scan(root, model.entity_table("Project").unwrap(), "same_hint")
                        .unwrap();
                    operation = graph
                        .join_relations(
                            operation,
                            graph.read_relation(other, ReadMode::Current).unwrap(),
                            JoinKind::Inner,
                            E::equal(
                                E::Column(value),
                                E::Column(graph.stored_column(other, "id").unwrap()),
                            ),
                        )
                        .unwrap();
                }
                (operation, E::Column(value))
            }
        };
        let projection = graph
            .project_values(source, [("id".into(), value)])
            .unwrap();
        graph.finish_query(projection).unwrap();
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
        apply_graph_security(&mut graph, root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        graph.render(root).unwrap();
        let source = graph.read_relation(scan, ReadMode::Current).unwrap();
        let (source, value) = if scalar {
            (graph.aggregate_relation(source, vec![]).unwrap(), E::Count)
        } else {
            (source, E::Column(id))
        };
        let replacement = graph
            .project_values(source, [("id".into(), value)])
            .unwrap();
        graph.substitute_query(replacement).unwrap();
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
    }
}

#[test]
fn authorization_walk_visits_shared_bodies_once_and_ignores_unreachable_blocks() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let body = graph.query();
    let scan = graph
        .scan(body, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(scan, ReadMode::Current).unwrap(),
            [(
                "id".into(),
                E::Column(graph.stored_column(scan, "id").unwrap()),
            )],
        )
        .unwrap();
    let id = projection.outputs().next().unwrap().0;
    graph.finish_query(projection).unwrap();
    let definition = graph.define(root, body, "projects").unwrap();
    let left = graph.reference(root, definition, "left").unwrap();
    let right = graph.reference(root, definition, "right").unwrap();
    let left_id = graph.output_column(left, id).unwrap();
    let right_id = graph.output_column(right, id).unwrap();
    let operation = graph
        .join_relations(
            graph.read_relation(left, ReadMode::Raw).unwrap(),
            graph.read_relation(right, ReadMode::Raw).unwrap(),
            JoinKind::Inner,
            E::equal(E::Column(left_id), E::Column(right_id)),
        )
        .unwrap();
    let projection = graph
        .project_values(operation, [("id".into(), E::Column(left_id))])
        .unwrap();
    graph.finish_query(projection).unwrap();
    let unused = graph.query();
    let scan = graph
        .scan(
            unused,
            model.entity_table("Vulnerability").unwrap(),
            "unused",
        )
        .unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(scan, ReadMode::Current).unwrap(),
            [(
                "id".into(),
                E::Column(graph.stored_column(scan, "id").unwrap()),
            )],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    apply_graph_security(&mut graph, root, &context).unwrap();
    check_graph(&graph, root, &context).unwrap();
    assert!(matches!(
        graph.operation(unused).unwrap().kind(),
        OperationKind::Source { .. }
    ));
    assert!(check_graph(&graph, unused, &context).is_err());
    let (mut scans, mut filters) = (0, 0);
    graph
        .walk_operations(root, |_, operation, _| {
            scans += usize::from(matches!(operation.kind(), OperationKind::Source { .. }));
            filters += usize::from(matches!(operation.kind(), OperationKind::Filter { .. }));
            Ok::<_, compiler::QueryError>(())
        })
        .unwrap();
    assert_eq!((scans, filters), (3, 1));
    graph.render(root).unwrap();
}

#[test]
fn authorization_rejects_broad_or_disjunctive_guards() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let scan = graph
        .scan(root, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let id = graph.stored_column(scan, "id").unwrap();
    let path = graph.stored_column(scan, "traversal_path").unwrap();
    let allowed = E::StartsWith(
        Box::new(E::Column(path)),
        Box::new(E::Text("1/100/".into())),
    );
    let projection = graph
        .project_values(
            graph.read_relation(scan, ReadMode::Current).unwrap(),
            [("id".into(), E::Column(id))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    for predicate in [
        E::Boolean(false),
        E::equal(E::Boolean(false), E::Boolean(false)),
        E::Or(Box::new(allowed.clone()), Box::new(E::Boolean(true))),
        E::Or(Box::new(E::Boolean(false)), Box::new(E::Boolean(true))),
        E::equal(allowed.clone(), E::Boolean(false)),
        E::StartsWith(
            Box::new(E::Text("1/100/".into())),
            Box::new(E::Column(path)),
        ),
        E::StartsWith(Box::new(E::Column(path)), Box::new(E::Text("2/".into()))),
        E::StartsWith(Box::new(E::Column(path)), Box::new(E::Text("1/".into()))),
    ] {
        let source = graph
            .filter_relation(
                graph.read_relation(scan, ReadMode::Current).unwrap(),
                predicate,
            )
            .unwrap();
        let replacement = graph
            .project_values(source, [("id".into(), E::Column(id))])
            .unwrap();
        graph.substitute_query(replacement).unwrap();
        assert!(check_graph(&graph, root, &context).is_err());
    }
    let source = graph
        .filter_relation(
            graph.read_relation(scan, ReadMode::Current).unwrap(),
            allowed,
        )
        .unwrap();
    let source = graph.filter_relation(source, E::Boolean(true)).unwrap();
    let replacement = graph
        .project_values(source, [("id".into(), E::Column(id))])
        .unwrap();
    graph.substitute_query(replacement).unwrap();
    check_graph(&graph, root, &context).unwrap();
}

#[test]
fn denied_roles_require_a_false_filter_on_the_scan() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
    let root = graph.query();
    let scan = graph
        .scan(
            root,
            model.entity_table("Vulnerability").unwrap(),
            "protected",
        )
        .unwrap();
    let id = graph.stored_column(scan, "id").unwrap();
    let projection = graph
        .project_values(
            graph.read_relation(scan, ReadMode::Current).unwrap(),
            [("id".into(), E::Column(id))],
        )
        .unwrap();
    graph.finish_query(projection).unwrap();
    apply_graph_security(&mut graph, root, &context).unwrap();
    check_graph(&graph, root, &context).unwrap();
    assert!(graph.render(root).unwrap().contains("WHERE false"));
    for predicate in [
        E::equal(E::Boolean(false), E::Boolean(false)),
        E::Or(Box::new(E::Boolean(false)), Box::new(E::Boolean(true))),
    ] {
        let source = graph
            .filter_relation(
                graph.read_relation(scan, ReadMode::Current).unwrap(),
                predicate,
            )
            .unwrap();
        let replacement = graph
            .project_values(source, [("id".into(), E::Column(id))])
            .unwrap();
        graph.substitute_query(replacement).unwrap();
        assert!(check_graph(&graph, root, &context).is_err());
    }
    let empty = SecurityContext::new(1, vec![]).unwrap();
    assert!(apply_graph_security(&mut graph, root, &empty).is_err());
    assert!(check_graph(&graph, root, &empty).is_err());
}

#[test]
fn role_filtering_precedes_prefix_collapse() {
    let ontology = super::super::setup::embedded_ontology()
        .as_ref()
        .clone()
        .with_schema_version_prefix("v101_");
    let model = compiler::data_model::clickhouse(std::sync::Arc::new(ontology)).unwrap();
    let context = SecurityContext::new_with_roles(
        1,
        vec![
            AuthorizedPath::new("1/", AccessLevel::Reporter as u32),
            AuthorizedPath::new("1/100/", AccessLevel::SecurityManager as u32),
            AuthorizedPath::new("1/100/200/", AccessLevel::SecurityManager as u32),
            AuthorizedPath::new("1/102/", AccessLevel::SecurityManager as u32),
        ],
    )
    .unwrap();
    for (entity, expected) in [
        ("User", vec![]),
        ("Project", vec!["1/"]),
        ("Vulnerability", vec!["1/100/", "1/102/"]),
    ] {
        let mut graph = QueryGraph::<_, Infallible>::new(model.as_ref());
        let root = graph.query();
        let scan = graph
            .scan(root, model.entity_table(entity).unwrap(), "entity")
            .unwrap();
        let projection = graph
            .project_values(
                graph.read_relation(scan, ReadMode::Current).unwrap(),
                [(
                    "id".into(),
                    E::Column(graph.stored_column(scan, "id").unwrap()),
                )],
            )
            .unwrap();
        graph.finish_query(projection).unwrap();
        apply_graph_security(&mut graph, root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        let (_, params) = graph.render_parameterized(root).unwrap();
        let mut paths = params
            .values()
            .filter_map(|param| param.value.as_str())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(paths, expected, "{entity}");
    }
}
