use std::sync::LazyLock;

use query_data_model::plan::logical::{
    EdgeField, Expr, Literal, LogicalOperation, LogicalPlan, Operand, Predicate,
};
use query_data_model::{Catalog, ClickHouse};

use crate::QueryError;
use crate::surface;

static MODEL: LazyLock<Catalog<ClickHouse>> =
    LazyLock::new(|| Catalog::<ClickHouse>::embedded().unwrap());

static CONNECTIONS: LazyLock<crate::dag::Connections> = LazyLock::new(|| {
    crate::graph_only(&std::sync::Arc::new(
        Catalog::<ClickHouse>::embedded().unwrap(),
    ))
});

fn normalized(gql: &str) -> Result<LogicalPlan, QueryError> {
    let model = MODEL.model();
    let statement = surface::parse(gql, model.logical(crate::testkit::limits()).as_admin())?;
    super::normalize(
        statement.plan,
        model,
        &*MODEL,
        &CONNECTIONS,
        crate::testkit::limits(),
    )
}

fn literals(plan: &LogicalPlan) -> Vec<Literal> {
    fn walk(predicate: &Predicate, into: &mut Vec<Literal>) {
        match predicate {
            Predicate::And(all) => all.iter().for_each(|inner| walk(inner, into)),
            Predicate::Not(inner) => walk(inner, into),
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => into.push(literal.clone()),
            Predicate::In { values, .. } => into.extend(values.iter().cloned()),
            _ => {}
        }
    }
    let mut found = Vec::new();
    for node in plan.nodes() {
        if let LogicalOperation::Filter(predicate) = plan.operation(node) {
            walk(predicate, &mut found);
        }
    }
    found
}

fn relationships(plan: &LogicalPlan) -> Vec<Vec<String>> {
    let graph = MODEL.model().graph();
    plan.nodes()
        .filter_map(|node| match plan.operation(node) {
            LogicalOperation::Follow { relationships, .. }
            | LogicalOperation::Neighbors { relationships, .. } => Some(
                relationships
                    .iter()
                    .map(|id| graph.relationship(*id).name.clone())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[test]
fn enum_codes_become_labels() {
    let plan = normalized("MATCH (mr:MergeRequest) WHERE mr.state IN [1, 3] RETURN mr").unwrap();
    assert_eq!(
        literals(&plan),
        [Literal::from("opened"), Literal::from("merged")]
    );
    let plan = normalized("MATCH (mr:MergeRequest {state: 'opened'}) RETURN mr").unwrap();
    assert_eq!(literals(&plan), [Literal::from("opened")]);
}

#[test]
fn type_conditions_narrow_the_step_and_drop_the_condition() {
    let plan = normalized(
        "MATCH (mr:MergeRequest {id: 1})-[r]->(n:WorkItem) WHERE type(r) IN ['CLOSES'] RETURN n",
    )
    .unwrap();
    assert_eq!(relationships(&plan), [vec!["CLOSES".to_string()]]);
    let kinds = plan.nodes().any(|node| {
        matches!(plan.operation(node), LogicalOperation::Filter(predicate)
            if predicate.exprs().iter().any(|expr| matches!(expr, Expr::Edge { field: EdgeField::Kind, .. })))
    });
    assert!(!kinds);
}

#[test]
fn wildcard_steps_name_the_reachable_relationships() {
    let plan = normalized("MATCH (u:User {id: 1})-[]->(mr:MergeRequest) RETURN mr").unwrap();
    let names = &relationships(&plan)[0];
    assert!(names.contains(&"AUTHORED".to_string()), "{names:?}");
    assert!(!names.is_empty());
}

#[test]
fn excluding_every_type_is_rejected() {
    let error = normalized(
        "MATCH (u:User {id: 1})-[r:AUTHORED]->(mr:MergeRequest) WHERE type(r) <> 'AUTHORED' RETURN mr",
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("excludes every relationship type"),
        "{error}"
    );
}
