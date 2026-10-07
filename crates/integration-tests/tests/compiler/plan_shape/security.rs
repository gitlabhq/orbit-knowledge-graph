use query_data_model::QueryDataModel;
use query_engine::compiler::{
    self, AccessLevel, AuthorizedPath, SecurityContext,
    passes::{check::check_graph, security::apply_graph_security},
    query_graph::{Expression as E, LoweredOperation as Operation, QueryGraph},
};

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
        let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
        let root = graph.select(Operation::One);
        let body = graph.select(Operation::One);
        let scan = graph
            .scan(body, model.entity_table("Project").unwrap(), "same_hint")
            .unwrap();
        let id = graph.stored_column(scan, "id").unwrap();
        *graph.operation_mut(body).unwrap() = Operation::current(scan);
        let output = graph.project(body, "id", E::Column(id)).unwrap();
        match position {
            "scalar" | "scalar_filter" => {
                *graph.operation_mut(body).unwrap() = Operation::current(scan).aggregate(vec![]);
                graph.replace_output(output, E::Count).unwrap();
                let relation = graph.derive(root, body, "scalar").unwrap();
                let scalar = E::ScalarQuery(graph.output_column(relation, output).unwrap());
                if position == "scalar_filter" {
                    *graph.operation_mut(root).unwrap() =
                        Operation::One.filter(E::equal(scalar, E::Integer(1)));
                    graph.project(root, "count", E::Integer(1)).unwrap();
                } else {
                    graph.project(root, "count", scalar).unwrap();
                }
            }
            "nested_definition" => {
                let wrapper = graph.select(Operation::One);
                let definition = graph.define(wrapper, body, "inner", false).unwrap();
                let reference = graph.reference(wrapper, definition, "inner").unwrap();
                let output = graph
                    .project(
                        wrapper,
                        "id",
                        E::Column(graph.output_column(reference, output).unwrap()),
                    )
                    .unwrap();
                *graph.operation_mut(wrapper).unwrap() = Operation::source(reference);
                let definition = graph.define(root, wrapper, "outer", false).unwrap();
                let reference = graph.reference(root, definition, "outer").unwrap();
                graph
                    .project(
                        root,
                        "id",
                        E::Column(graph.output_column(reference, output).unwrap()),
                    )
                    .unwrap();
                *graph.operation_mut(root).unwrap() = Operation::source(reference);
            }
            "definition" | "membership" => {
                let definition = graph.define(root, body, "keys", false).unwrap();
                let relation = graph.reference(root, definition, "keys").unwrap();
                let value = graph.output_column(relation, output).unwrap();
                if position == "membership" {
                    *graph.operation_mut(root).unwrap() = Operation::One.filter(E::InQuery {
                        value: Box::new(E::Integer(1)),
                        key: value,
                    });
                    graph.project(root, "id", E::Integer(1)).unwrap();
                } else {
                    *graph.operation_mut(root).unwrap() = Operation::source(relation);
                    graph.project(root, "id", E::Column(value)).unwrap();
                }
            }
            "union" => {
                let other = graph.select(Operation::One);
                let other_scan = graph
                    .scan(other, model.default_edge_table(), "same_hint")
                    .unwrap();
                graph
                    .project(
                        other,
                        "id",
                        E::Column(graph.stored_column(other_scan, "source_id").unwrap()),
                    )
                    .unwrap();
                *graph.operation_mut(other).unwrap() = Operation::source(other_scan);
                let union = graph
                    .union_all(vec![body, other], vec!["id".into()])
                    .unwrap();
                let relation = graph.derive(root, union, "union").unwrap();
                graph
                    .project(root, "id", E::Column(graph.column(relation, "id").unwrap()))
                    .unwrap();
                *graph.operation_mut(root).unwrap() = Operation::source(relation);
            }
            _ => {
                let relation = graph.derive(root, body, "derived").unwrap();
                let value = graph.output_column(relation, output).unwrap();
                let mut operation = Operation::source(relation);
                if position == "join" {
                    let other = graph
                        .scan(root, model.entity_table("Project").unwrap(), "same_hint")
                        .unwrap();
                    operation = operation.join(
                        Operation::current(other),
                        E::equal(
                            E::Column(value),
                            E::Column(graph.stored_column(other, "id").unwrap()),
                        ),
                    );
                }
                *graph.operation_mut(root).unwrap() = operation;
                graph.project(root, "id", E::Column(value)).unwrap();
            }
        }
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
        let unprotected = graph.operation(body).unwrap().clone();
        apply_graph_security(&mut graph, root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        graph.render(root).unwrap();
        *graph.operation_mut(body).unwrap() = unprotected;
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
    }
}

#[test]
fn authorization_walk_visits_shared_bodies_once_and_ignores_unreachable_blocks() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let body = graph.select(Operation::One);
    let scan = graph
        .scan(body, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    *graph.operation_mut(body).unwrap() = Operation::current(scan);
    let id = graph
        .project(
            body,
            "id",
            E::Column(graph.stored_column(scan, "id").unwrap()),
        )
        .unwrap();
    let definition = graph.define(root, body, "projects", false).unwrap();
    let left = graph.reference(root, definition, "left").unwrap();
    let right = graph.reference(root, definition, "right").unwrap();
    let left_id = graph.output_column(left, id).unwrap();
    let right_id = graph.output_column(right, id).unwrap();
    *graph.operation_mut(root).unwrap() = Operation::source(left).join(
        Operation::source(right),
        E::equal(E::Column(left_id), E::Column(right_id)),
    );
    graph.project(root, "id", E::Column(left_id)).unwrap();
    let unused = graph.select(Operation::One);
    let unused_scan = graph
        .scan(
            unused,
            model.entity_table("Vulnerability").unwrap(),
            "unused",
        )
        .unwrap();
    *graph.operation_mut(unused).unwrap() = Operation::current(unused_scan);
    graph
        .project(
            unused,
            "id",
            E::Column(graph.stored_column(unused_scan, "id").unwrap()),
        )
        .unwrap();

    apply_graph_security(&mut graph, root, &context).unwrap();
    check_graph(&graph, root, &context).unwrap();
    assert!(matches!(
        graph.operation(unused).unwrap(),
        Operation::Source { .. }
    ));
    assert!(check_graph(&graph, unused, &context).is_err());
    let mut scans = 0;
    let mut filters = 0;
    graph
        .walk_operations(root, |_, operation, _| {
            scans += usize::from(matches!(operation, Operation::Source { .. }));
            filters += usize::from(matches!(operation, Operation::Filter { .. }));
            Ok::<_, compiler::QueryError>(())
        })
        .unwrap();
    assert_eq!(scans, 3);
    assert_eq!(filters, 1);
    graph.render(root).unwrap();
}

#[test]
fn authorization_rejects_broad_or_disjunctive_guards() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let scan = graph
        .scan(root, model.entity_table("Project").unwrap(), "project")
        .unwrap();
    let path = graph.stored_column(scan, "traversal_path").unwrap();
    let allowed = E::StartsWith(
        Box::new(E::Column(path)),
        Box::new(E::Text("1/100/".into())),
    );
    graph
        .project(
            root,
            "id",
            E::Column(graph.stored_column(scan, "id").unwrap()),
        )
        .unwrap();
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
        *graph.operation_mut(root).unwrap() = Operation::current(scan).filter(predicate);
        assert!(check_graph(&graph, root, &context).is_err());
    }
    *graph.operation_mut(root).unwrap() = Operation::current(scan).filter(allowed);
    check_graph(&graph, root, &context).unwrap();
    let operation = graph.operation_mut(root).unwrap();
    *operation = std::mem::replace(operation, Operation::One).filter(E::Boolean(true));
    check_graph(&graph, root, &context).unwrap();
}

#[test]
fn denied_roles_require_a_false_filter_on_the_scan() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
    let root = graph.select(Operation::One);
    let scan = graph
        .scan(
            root,
            model.entity_table("Vulnerability").unwrap(),
            "protected",
        )
        .unwrap();
    graph
        .project(
            root,
            "id",
            E::Column(graph.stored_column(scan, "id").unwrap()),
        )
        .unwrap();
    *graph.operation_mut(root).unwrap() = Operation::current(scan);
    apply_graph_security(&mut graph, root, &context).unwrap();
    check_graph(&graph, root, &context).unwrap();
    assert!(graph.render(root).unwrap().contains("WHERE false"));
    for predicate in [
        E::equal(E::Boolean(false), E::Boolean(false)),
        E::Or(Box::new(E::Boolean(false)), Box::new(E::Boolean(true))),
    ] {
        *graph.operation_mut(root).unwrap() = Operation::current(scan).filter(predicate);
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
    for (entity, expected_paths) in [
        ("User", vec![]),
        ("Project", vec!["1/"]),
        ("Vulnerability", vec!["1/100/", "1/102/"]),
    ] {
        let mut graph = QueryGraph::<_, E<'_>, Operation<'_>>::new(model.as_ref());
        let root = graph.select(Operation::One);
        let scan = graph
            .scan(root, model.entity_table(entity).unwrap(), "entity")
            .unwrap();
        *graph.operation_mut(root).unwrap() = Operation::current(scan);
        graph
            .project(
                root,
                "id",
                E::Column(graph.stored_column(scan, "id").unwrap()),
            )
            .unwrap();
        apply_graph_security(&mut graph, root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        let (_, params) = graph.render_parameterized(root).unwrap();
        let mut paths = params
            .values()
            .filter_map(|param| param.value.as_str())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(paths, expected_paths, "{entity}");
    }
}
