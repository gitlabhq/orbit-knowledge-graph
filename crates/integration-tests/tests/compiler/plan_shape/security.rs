use compiler::passes::{check::check_graph, security::apply_graph_security};
use compiler::query_graph::api::*;
use query_data_model::QueryDataModel;
use query_engine::compiler::{self, AccessLevel, AuthorizedPath, SecurityContext};

fn projects<'a, M: QueryDataModel + ?Sized>(q: &mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>> {
    let rows = q.scan(q.catalog().entity_table("Project").unwrap(), Read::Current)?;
    let id = rows.column("id")?;
    q.select(rows, [id.named("id")])
}

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
        let mut graph = QueryGraph::new(model.as_ref());
        let root = graph
            .query(|q| match position {
                "scalar" | "scalar_filter" => {
                    let body = q.subquery(|q| {
                        let rows = projects(q)?;
                        q.aggregate(rows, [], [count().named("count")])
                    })?;
                    let scalar = q.scalar(body, "count")?;
                    if position == "scalar" {
                        q.values([scalar.named("id")])
                    } else {
                        let rows = q.values([lit(1).named("id")])?;
                        q.filter(rows, scalar.eq(1))
                    }
                }
                "nested_definition" => {
                    let outer = q.cte("outer", |q| {
                        let inner = q.cte("inner", projects)?;
                        q.read(inner)
                    })?;
                    q.read(outer)
                }
                "definition" | "membership" => {
                    let definition = q.cte("keys", projects)?;
                    let keys = q.read(definition)?;
                    if position == "definition" {
                        Ok(keys)
                    } else {
                        let key = keys.column("id")?;
                        let rows = q.values([lit(1).named("id")])?;
                        let id = rows.column("id")?;
                        q.filter_in(rows, id, keys, key)
                    }
                }
                "union" => {
                    let first = q.subquery(projects)?;
                    let second = q.subquery(|q| {
                        let rows = q.scan(q.catalog().default_edge_table(), Read::Raw)?;
                        let id = rows.column("source_id")?;
                        q.select(rows, [id.named("id")])
                    })?;
                    q.union_all([first, second])
                }
                _ => {
                    let body = q.subquery(projects)?;
                    let rows = q.from(body)?;
                    if position == "join" {
                        let right = projects(q)?;
                        let id = rows.column("id")?;
                        let condition = id.eq(right.column("id")?);
                        let rows = q.join(rows, right, condition)?;
                        q.select(rows, [id.named("id")])
                    } else {
                        Ok(rows)
                    }
                }
            })
            .unwrap();
        let graph = graph.lower();
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
        let graph = apply_graph_security(graph, root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        graph.render(root).unwrap();
        let graph = graph.rewrite(root, |_, rows| {
            if matches!(rows.kind(), OperationKind::Filter { input, .. } if matches!(input.kind(), OperationKind::Scan { .. })) {
                rows.remove_filter()
            } else { Ok(rows) }
        }).unwrap();
        assert!(check_graph(&graph, root, &context).is_err(), "{position}");
    }
}

#[test]
fn authorization_walk_visits_shared_bodies_once_and_ignores_unreachable_blocks() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .query(|q| {
            let definition = q.cte("projects", projects)?;
            let left = q.read(definition)?;
            let right = q.read(definition)?;
            let id = left.column("id")?;
            let condition = id.eq(right.column("id")?);
            let rows = q.join(left, right, condition)?;
            q.select(rows, [id.named("id")])
        })
        .unwrap();
    let unused = graph
        .query(|q| {
            q.scan(
                q.catalog().entity_table("Vulnerability").unwrap(),
                Read::Current,
            )
        })
        .unwrap();
    let graph = apply_graph_security(graph.lower(), root, &context).unwrap();
    check_graph(&graph, root, &context).unwrap();
    assert!(matches!(
        graph.graph().rows(unused).unwrap().kind(),
        OperationKind::Scan { .. }
    ));
    assert!(check_graph(&graph, unused, &context).is_err());
    let (mut scans, mut filters) = (0, 0);
    for query in graph.graph().reachable(root).unwrap() {
        graph
            .graph()
            .rows(query)
            .unwrap()
            .walk(&mut |rows| {
                scans += usize::from(matches!(
                    rows.kind(),
                    OperationKind::Scan { .. } | OperationKind::Read { .. }
                ));
                filters += usize::from(matches!(rows.kind(), OperationKind::Filter { .. }));
                Ok::<_, Error>(())
            })
            .unwrap();
    }
    assert_eq!((scans, filters), (3, 1));
    graph.render(root).unwrap();
}

#[test]
fn authorization_rejects_broad_or_disjunctive_guards() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    for case in 0..9 {
        let mut graph = QueryGraph::new(model.as_ref());
        let root = graph
            .query(|q| {
                let rows = q.scan(model.entity_table("Project").unwrap(), Read::Current)?;
                let path = rows.column("traversal_path")?;
                let allowed = path.starts_with("1/100/");
                let predicate = match case {
                    0 => lit(false),
                    1 => lit(false).eq(false),
                    2 => allowed.or(true),
                    3 => lit(false).or(true),
                    4 => allowed.eq(false),
                    5 => lit("1/100/").starts_with(path),
                    6 => path.starts_with("2/"),
                    7 => path.starts_with("1/"),
                    _ => allowed,
                };
                let rows = q.filter(rows, predicate)?;
                q.filter(rows, lit(true))
            })
            .unwrap();
        assert_eq!(
            check_graph(&graph.lower(), root, &context).is_ok(),
            case == 8
        );
    }
}

#[test]
fn denied_roles_require_a_false_filter_on_the_scan() {
    let model = compiler::data_model::clickhouse(super::super::setup::embedded_ontology()).unwrap();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    for predicate in [lit(false), lit(false).eq(false), lit(false).or(true)] {
        let mut graph = QueryGraph::new(model.as_ref());
        let root = graph
            .query(|q| q.scan(model.entity_table("Vulnerability").unwrap(), Read::Current))
            .unwrap();
        let graph = apply_graph_security(graph.lower(), root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        let (_, params) = graph.render(root).unwrap();
        assert!(params.values().any(|parameter| parameter.value == false));
        let expected = predicate == lit(false);
        let graph = graph
            .rewrite(root, |q, rows| {
                if matches!(rows.kind(), OperationKind::Filter { .. }) {
                    q.map_expressions(rows, |_| Ok(predicate.clone()))
                } else {
                    Ok(rows)
                }
            })
            .unwrap();
        assert_eq!(check_graph(&graph, root, &context).is_ok(), expected);
        let empty = SecurityContext::new(1, vec![]).unwrap();
        assert!(check_graph(&graph, root, &empty).is_err());
        assert!(apply_graph_security(graph, root, &empty).is_err());
    }
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
        let mut graph = QueryGraph::new(model.as_ref());
        let root = graph
            .query(|q| q.scan(model.entity_table(entity).unwrap(), Read::Current))
            .unwrap();
        let graph = apply_graph_security(graph.lower(), root, &context).unwrap();
        check_graph(&graph, root, &context).unwrap();
        let (_, params) = graph.render(root).unwrap();
        let mut paths = params
            .values()
            .filter_map(|param| param.value.as_str())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(paths, expected, "{entity}");
    }
}
