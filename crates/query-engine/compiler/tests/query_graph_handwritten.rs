use compiler::data_model;
use compiler::query_graph::{
    OperationKind, QueryGraph, Read, array, array_concat, count, lit, singleton_if, tuple,
};
use ontology::Ontology;
use query_data_model::QueryDataModel;
use std::sync::Arc;

#[test]
fn filtered_scan() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let users = q.scan(model.entity_table("User").unwrap(), Read::Current)?;
            let id = users.column("id")?;
            let name = users.column("username")?;
            let deleted = users.column("_deleted")?;
            let users = q.filter(users, name.eq("alice").and(deleted.eq(false)))?;
            let users = q.sort(users, [id.asc()])?;
            let users = q.limit(users, 20)?;
            q.select(users, [id.named("id"), name.named("username")])
        })
        .unwrap();
    let (sql, params) = graph.lower().render(query).unwrap();
    assert!(sql.contains("FINAL") && sql.contains("LIMIT 20") && sql.contains("ORDER BY"));
    assert!(!sql.contains("alice"));
    assert!(params.values().any(|value| value.value == "alice"));
}

#[test]
fn candidate_key_cte_and_join() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let candidates = q.cte("candidate_projects", |q| {
                let projects = q.scan(model.entity_table("Project").unwrap(), Read::Current)?;
                let id = projects.column("id")?;
                let visibility = projects.column("visibility_level")?;
                let deleted = projects.column("_deleted")?;
                let projects =
                    q.filter(projects, visibility.eq("public").and(deleted.eq(false)))?;
                q.select(projects, [id.named("id")])
            })?;
            let requests = q.scan(model.entity_table("MergeRequest").unwrap(), Read::Current)?;
            let project_id = requests.column("project_id")?;
            let title = requests.column("title")?;
            let deleted = requests.column("_deleted")?;
            let keys = q.read(candidates)?;
            let key = keys.column("id")?;
            let requests = q.filter_in(requests, project_id.clone(), keys, key)?;
            let requests = q.filter(requests, deleted.eq(false))?;
            let projects = q.scan(model.entity_table("Project").unwrap(), Read::Current)?;
            let id = projects.column("id")?;
            let name = projects.column("name")?;
            let deleted = projects.column("_deleted")?;
            let projects = q.filter(projects, deleted.eq(false))?;
            let rows = q.join(requests, projects, project_id.eq(id))?;
            q.select(rows, [title.named("title"), name.named("project")])
        })
        .unwrap();
    let (sql, _) = graph.lower().render(query).unwrap();
    assert!(sql.starts_with("WITH ") && sql.contains(" IN (SELECT ") && sql.contains("INNER JOIN"));
}

#[test]
fn grouped_aggregate_exposes_measure_columns() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let requests = q.scan(model.entity_table("MergeRequest").unwrap(), Read::Current)?;
            let author = requests.column("author_id")?;
            let state = requests.column("state")?;
            let deleted = requests.column("_deleted")?;
            let requests = q.filter(requests, deleted.eq(false))?;
            let totals = q.aggregate(
                requests,
                [author.named("author")],
                [
                    count().named("total"),
                    count().filter(state.eq("merged")).named("merged"),
                ],
            )?;
            let total = totals.column("total")?;
            let merged = totals.column("merged")?;
            let totals = q.filter(totals, merged.gt(0))?;
            let totals = q.sort(totals, [total.desc()])?;
            let totals = q.limit(totals, 10)?;
            q.select_all(totals)
        })
        .unwrap();
    let (sql, _) = graph.lower().render(query).unwrap();
    assert!(sql.contains("countIf(") && sql.contains("GROUP BY") && sql.contains("DESC"));
}

#[test]
fn latest_rows_keep_deletion_after_version_selection() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let projects = q.scan(model.entity_table("Project").unwrap(), Read::Raw)?;
            let id = projects.column("id")?;
            let path = projects.column("traversal_path")?;
            let version = projects.column("_version")?;
            let deleted = projects.column("_deleted")?;
            let name = projects.column("name")?;
            let projects = q.filter(
                projects,
                path.starts_with("1/100/").and(id.in_values([7, 8])),
            )?;
            let projects = q.latest(projects, [path, id.clone()], version)?;
            let projects = q.filter(projects, deleted.eq(false).and(name.eq("orbit")))?;
            q.select(projects, [id.named("id"), name.named("name")])
        })
        .unwrap();
    let graph = graph.lower();
    let OperationKind::Select { input, .. } = graph.graph().rows(query).unwrap().kind() else {
        panic!("projection")
    };
    let OperationKind::Filter { input, .. } = input.kind() else {
        panic!("deletion filter")
    };
    assert!(matches!(input.kind(), OperationKind::FirstBy { .. }));
    let (sql, _) = graph.render(query).unwrap();
    assert!(sql.contains("LIMIT 1 BY"));
}

#[test]
fn fused_neighbors_expand_a_new_tuple_column() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let edges = q.scan(model.default_edge_table(), Read::Raw)?;
            let source = edges.column("source_id")?;
            let target = edges.column("target_id")?;
            let source_kind = edges.column("source_kind")?;
            let target_kind = edges.column("target_kind")?;
            let relationship = edges.column("relationship_kind")?;
            let deleted = edges.column("_deleted")?;
            let outgoing = source.eq(42);
            let incoming = target.eq(42);
            let edges = q.filter(
                edges,
                outgoing.clone().or(incoming.clone()).and(deleted.eq(false)),
            )?;
            let matches = array_concat([
                singleton_if(
                    outgoing,
                    tuple([target.expr(), target_kind.expr(), lit(true)]),
                ),
                singleton_if(
                    incoming,
                    tuple([source.expr(), source_kind.expr(), lit(false)]),
                ),
            ]);
            let rows = q.expand(edges, matches.named("neighbor"))?;
            let neighbor = rows.column("neighbor")?;
            let rows = q.limit(rows, 20)?;
            q.select(
                rows,
                [
                    neighbor.field(0).named("id"),
                    neighbor.field(1).named("kind"),
                    neighbor.field(2).named("outgoing"),
                    relationship.named("relationship"),
                ],
            )
        })
        .unwrap();
    let (sql, _) = graph.lower().render(query).unwrap();
    assert_eq!(sql.matches("arrayJoin(").count(), 1);
    assert_eq!(sql.matches("arrayFilter(").count(), 2);
}

#[test]
fn path_frontier_reuses_a_cte_across_union_arms() {
    let model = data_model::clickhouse(Arc::new(Ontology::load_embedded().unwrap())).unwrap();
    let mut graph = QueryGraph::new(model.as_ref());
    let query = graph
        .query(|q| {
            let first_hop = q.cte("first_hop", |q| {
                let edges = q.scan(model.default_edge_table(), Read::Raw)?;
                let source = edges.column("source_id")?;
                let target = edges.column("target_id")?;
                let kind = edges.column("target_kind")?;
                let deleted = edges.column("_deleted")?;
                let edges = q.filter(edges, source.eq(42).and(deleted.eq(false)))?;
                q.select(
                    edges,
                    [
                        source.named("anchor"),
                        target.named("end"),
                        kind.named("end_kind"),
                    ],
                )
            })?;
            let direct = q.subquery(|q| {
                let frontier = q.read(first_hop)?;
                let anchor = frontier.column("anchor")?;
                let end = frontier.column("end")?;
                let kind = frontier.column("end_kind")?;
                q.select(
                    frontier,
                    [
                        anchor.named("anchor"),
                        end.named("end"),
                        array([tuple([end.expr(), kind.expr()])]).named("path"),
                        lit(1).named("depth"),
                    ],
                )
            })?;
            let extended = q.subquery(|q| {
                let frontier = q.read(first_hop)?;
                let edges = q.scan(model.default_edge_table(), Read::Raw)?;
                let anchor = frontier.column("anchor")?;
                let middle = frontier.column("end")?;
                let middle_kind = frontier.column("end_kind")?;
                let source = edges.column("source_id")?;
                let target = edges.column("target_id")?;
                let target_kind = edges.column("target_kind")?;
                let deleted = edges.column("_deleted")?;
                let edges = q.filter(edges, deleted.eq(false))?;
                let rows = q.join(frontier, edges, middle.eq(source))?;
                q.select(
                    rows,
                    [
                        anchor.named("anchor"),
                        target.named("end"),
                        array([
                            tuple([middle.expr(), middle_kind.expr()]),
                            tuple([target.expr(), target_kind.expr()]),
                        ])
                        .named("path"),
                        lit(2).named("depth"),
                    ],
                )
            })?;
            q.union_all([direct, extended])
        })
        .unwrap();
    let (sql, _) = graph.lower().render(query).unwrap();
    assert_eq!(sql.matches(" AS (SELECT").count(), 1);
    assert!(sql.contains("UNION ALL"));
}
