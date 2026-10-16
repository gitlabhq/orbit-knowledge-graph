use super::QueryScope;
use crate::passes::plan::QueryPlan;
use crate::passes::plan::physical::{ReadOwner, RelationNode};

pub fn apply_plan(plan: &mut QueryPlan, scope: &QueryScope) {
    let execution = match plan {
        QueryPlan::Traversal(plan) => &mut plan.operation.execution,
        QueryPlan::Aggregation(plan) => &mut plan.operation.execution,
        _ => return,
    };
    let filters: Vec<_> = execution
        .graph
        .iter()
        .filter_map(|(id, node)| {
            let RelationNode::Owned { owner, alias, .. } = node else {
                return None;
            };
            let proof = match owner {
                ReadOwner::Node(node) => scope.node(node),
                ReadOwner::Relationship(index) => scope.relationship(*index),
            }?;
            Some((id, alias.clone(), proof.clone()))
        })
        .collect();
    for (id, alias, proof) in filters {
        execution.graph.filter_uses(
            &mut execution.root,
            id,
            vec![crate::passes::plan::requirements::Predicate::Scope { alias, proof }],
        );
    }
}
