mod support;

use query_data_model::Packages;
use query_data_model::plan::logical::{ErrorClass, Expr, LogicalOperation, Projection};
use query_data_model::surface::{SurfaceError, gql};

fn plan(query: &str) -> Result<Vec<String>, SurfaceError> {
    let model = support::gitlab();
    let statement = gql::parse(query, model.logical().as_admin())?;
    let plan = statement.plan;
    Ok(plan
        .nodes()
        .map(|node| match plan.operation(node) {
            LogicalOperation::Entities(_) => "entities".to_string(),
            LogicalOperation::Filter(_) => "filter".to_string(),
            LogicalOperation::Follow { .. } => "follow".to_string(),
            LogicalOperation::Neighbors { .. } => "neighbors".to_string(),
            LogicalOperation::Shortest { .. } => "shortest".to_string(),
            LogicalOperation::Aggregate { groups, measures } => {
                format!("aggregate {} {}", groups.len(), measures.len())
            }
            LogicalOperation::Select(items) => format!(
                "select {}",
                items
                    .iter()
                    .map(|item| match item {
                        Projection::Graph { .. } => "graph",
                        Projection::Value(_) => "value",
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            LogicalOperation::Order(_) => "order".to_string(),
            LogicalOperation::Limit { rows, skip } => {
                format!("limit {rows} {}", skip.unwrap_or_default())
            }
            LogicalOperation::Union | LogicalOperation::Read(_) => "other".to_string(),
            LogicalOperation::Structural(structural) => {
                format!("structural {}", structural.pattern.text())
            }
        })
        .collect())
}

#[test]
fn a_match_lowers_to_bindings_steps_and_projections() {
    let steps = plan(
        "MATCH (u:User)-[r:AUTHORED]->(mr:MergeRequest {state: 'opened'}) \
         WHERE u.id IN [1, 2] AND NOT mr.title STARTS WITH 'wip' \
         RETURN mr.title AS title, type(r) ORDER BY title DESC SKIP 10 LIMIT 5",
    )
    .unwrap();
    assert_eq!(
        steps,
        [
            "entities",
            "follow",
            "filter",
            "filter",
            "order",
            "select graph,value",
            "limit 5 10"
        ]
    );
}

#[test]
fn aggregates_neighbors_and_paths_choose_their_operations() {
    let aggregate = plan(
        "MATCH (u:User)-[:AUTHORED]->(n:Note) WHERE u.id = 1 RETURN u, count(n) AS notes ORDER BY notes DESC",
    )
    .unwrap();
    assert_eq!(
        aggregate,
        [
            "entities",
            "follow",
            "filter",
            "aggregate 1 1",
            "order",
            "select graph,value",
            "limit 30 0"
        ]
    );
    let neighbors = plan("MATCH (p:Project)-[:IN_PROJECT]-(n) WHERE p.id = 1 RETURN p, n").unwrap();
    assert_eq!(
        neighbors,
        [
            "entities",
            "neighbors",
            "filter",
            "select graph,graph",
            "limit 30 0"
        ]
    );
    let path = plan(
        "MATCH p = ANY SHORTEST (g:Group {id: 1})-[:CONTAINS*1..3]->(x:Project {id: 2}) RETURN p",
    )
    .unwrap();
    assert_eq!(
        path,
        [
            "entities",
            "filter",
            "entities",
            "filter",
            "shortest",
            "select graph",
            "limit 30 0"
        ]
    );
}

#[test]
fn surface_and_model_errors_keep_their_kind() {
    let class = |query| match plan(query).unwrap_err() {
        SurfaceError::Plan(error) => Some(error.class()),
        _ => None,
    };
    assert_eq!(
        class("MATCH (m:MergeRequst) RETURN m"),
        Some(ErrorClass::Name)
    );
    assert_eq!(
        class("MATCH (p:Project)-[:AUTHORED]->(u:User) RETURN p"),
        Some(ErrorClass::Connectivity)
    );
    assert_eq!(
        class("MATCH (a:User)-[]-(b:User) RETURN a"),
        Some(ErrorClass::Shape)
    );
    assert!(matches!(plan("MATCH RETURN"), Err(SurfaceError::Schema(_))));
}

#[test]
fn relationship_attributes_filter_inline_and_in_where() {
    let mut sources = Packages::embedded().sources().clone();
    sources.insert(
        "overlay/graph.yaml".into(),
        "kind: graph\npackage: overlay\nrelationships:\n  ON_BRANCH:\n    variants: {}\n    attributes: {branch: string, project_id: int64}\n".into(),
    );
    let model = Packages::new(sources).load().unwrap();
    let statement = gql::parse(
        "MATCH (f:File {project_id: 1})-[r:ON_BRANCH {branch: 'main'}]->(b:Branch) \
         WHERE r.project_id = 1 RETURN b",
        model.logical(),
    )
    .unwrap();
    let plan = statement.plan;
    let attributes: Vec<_> = plan
        .nodes()
        .filter_map(|node| match plan.operation(node) {
            LogicalOperation::Filter(predicate) => Some(predicate.exprs()),
            _ => None,
        })
        .flatten()
        .filter_map(|expr| match expr {
            Expr::Attribute { attribute, .. } => {
                Some(model.graph().attribute(attribute).name.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(attributes, ["branch", "project_id"]);

    let error = gql::parse(
        "MATCH (u:User {id: 1})-[r:AUTHORED]->(m:MergeRequest) WHERE r.branch = 'main' RETURN m",
        model.logical(),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "unknown column branch; available: relationship_kind, source_id, source_kind, target_id, target_kind"
    );
}

#[test]
fn a_match_declares_its_nodes_in_written_order() {
    let model = Packages::embedded().load().unwrap();
    let statement = gql::parse(
        "MATCH (u:User {id: 1})-[:AUTHORED]->(mr:MergeRequest), (pipe:Pipeline), \
         (mr)-[:IN_PROJECT]->(p:Project), (mr)-[:HAS_HEAD_PIPELINE]->(pipe) RETURN *",
        model.logical(),
    )
    .unwrap();
    let plan = statement.plan;
    let order: Vec<_> = plan
        .declaration_order()
        .map(|output| plan.name(output).unwrap())
        .collect();
    assert_eq!(order, ["u", "mr", "pipe", "p"]);
}

#[test]
fn shortest_paths_need_a_walk_within_the_depth_not_a_direct_variant() {
    let shortest = |depth: u32| {
        plan(&format!(
            "MATCH p = ANY SHORTEST (u:User {{id: 1}})-[:AUTHORED|IN_PROJECT*1..{depth}]->(x:Project {{id: 2}}) RETURN p"
        ))
    };
    shortest(2).expect("User AUTHORED MergeRequest IN_PROJECT Project is two hops");
    let SurfaceError::Plan(error) = shortest(1).unwrap_err() else {
        panic!("no single AUTHORED or IN_PROJECT hop reaches Project from User");
    };
    assert_eq!(error.class(), ErrorClass::Connectivity);
}
