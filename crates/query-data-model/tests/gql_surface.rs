mod support;

use query_data_model::plan::logical::{ErrorClass, LogicalOperation, Projection};
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
            LogicalOperation::Limit { rows, skip } => format!("limit {rows} {skip}"),
            LogicalOperation::Union | LogicalOperation::Read(_) => "other".to_string(),
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
            "filter",
            "neighbors",
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
    assert!(matches!(
        plan("MATCH (a:User)-[]-(b:User) RETURN a"),
        Err(SurfaceError::Schema(_))
    ));
    assert!(matches!(plan("MATCH RETURN"), Err(SurfaceError::Schema(_))));
}
