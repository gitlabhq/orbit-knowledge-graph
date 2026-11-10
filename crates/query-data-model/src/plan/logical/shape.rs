use super::limits::RESERVED_PREFIX;
use super::{
    Comparison, Conjunct, EdgeField, Expr, LogicalOperation, LogicalPlan, LogicalType, Operand,
    PlanError, Predicate,
};
use crate::plan::tracking::Ref;

pub(super) fn plan(plan: &LogicalPlan, internal: &[String]) -> Result<(), PlanError> {
    reserved(plan, internal)?;
    overlapping(plan)?;
    conditions(plan)?;
    named(plan)
}

fn reserved(plan: &LogicalPlan, internal: &[String]) -> Result<(), PlanError> {
    let names = plan
        .nodes()
        .flat_map(|node| plan.outputs(node))
        .filter_map(|output| plan.name(output))
        .filter(|name| !internal.iter().any(|allocated| allocated == name));
    for name in names {
        if format!("{name}_").starts_with(RESERVED_PREFIX) {
            return Err(PlanError::shape(format!(
                "identifier {name:?} conflicts with reserved prefix {RESERVED_PREFIX:?}"
            )));
        }
    }
    Ok(())
}

fn overlapping(plan: &LogicalPlan) -> Result<(), PlanError> {
    let mut names: Vec<&str> = Vec::new();
    let entities = plan
        .nodes()
        .flat_map(|node| plan.outputs(node))
        .filter(|output| matches!(plan.value(*output), LogicalType::Entity(_)))
        .filter_map(|output| plan.name(output));
    for name in entities {
        if names.contains(&name) {
            continue;
        }
        for other in &names {
            let (short, long) = match name.len() < other.len() {
                true => (name, *other),
                false => (*other, name),
            };
            if long.starts_with(&format!("{short}_")) {
                return Err(PlanError::shape(format!(
                    "node ids \"{short}\" and \"{long}\" overlap; rename one so neither starts with the other followed by \"_\""
                )));
            }
        }
        names.push(name);
    }
    Ok(())
}

fn conditions(plan: &LogicalPlan) -> Result<(), PlanError> {
    let mut shape = None;
    let mut traversal = true;
    for node in plan.nodes() {
        match plan.operation(node) {
            LogicalOperation::Neighbors { .. } => shape = Some("neighbors queries"),
            LogicalOperation::Shortest { .. } => shape = Some("shortest paths"),
            LogicalOperation::Aggregate { .. } => {}
            _ => continue,
        }
        traversal = false;
    }
    let predicates = plan.nodes().filter_map(|node| match plan.operation(node) {
        LogicalOperation::Filter(predicate) => Some(predicate),
        _ => None,
    });
    for predicate in predicates {
        let leaves = predicate.leaf_predicates();
        if !traversal && leaves.iter().any(|leaf| compares_bindings(leaf)) {
            return Err(PlanError::shape(
                "cross-node property comparisons are only supported in traversal queries",
            ));
        }
        let negated = predicate
            .conjuncts()
            .into_iter()
            .any(|conjunct| !conjunct.negated_leaves().is_empty());
        if let (Some(shape), true) = (shape, negated) {
            return Err(PlanError::shape(format!(
                "{shape} support NOT only on =, <>, IS NULL, and IS NOT NULL conditions; use a traversal for other negated conditions"
            )));
        }
        let spanning = predicate.conjuncts().into_iter().any(|conjunct| {
            let bound: Vec<Ref> = conjunct_exprs(conjunct).into_iter().map(binding).collect();
            bound.iter().any(|other| *other != bound[0])
        });
        if let (Some(shape), true) = (shape, spanning) {
            return Err(PlanError::shape(format!(
                "each condition of {shape} must reference one node"
            )));
        }
    }
    Ok(())
}

fn conjunct_exprs(conjunct: Conjunct<'_>) -> Vec<Expr> {
    match conjunct {
        Conjunct::Leaf { predicate, .. } | Conjunct::Negated(predicate) => predicate.exprs(),
    }
}

/// A shortest path over every relationship type expands too far; a type() condition must name them.
fn named(plan: &LogicalPlan) -> Result<(), PlanError> {
    for node in plan.nodes() {
        let LogicalOperation::Shortest { relationships, .. } = plan.operation(node) else {
            continue;
        };
        let outputs: Vec<Ref> = plan.outputs(node).collect();
        let [.., edge, _] = outputs[..] else {
            continue;
        };
        if relationships.is_empty() && !narrowed(plan, edge) {
            return Err(PlanError::shape(
                "shortest paths require named relationship types, such as -[:CONTAINS*1..3]->; a wildcard expands over every edge type",
            ));
        }
    }
    Ok(())
}

fn narrowed(plan: &LogicalPlan, edge: Ref) -> bool {
    let kind = Expr::Edge {
        binding: edge,
        field: EdgeField::Kind,
    };
    plan.nodes()
        .filter_map(|node| match plan.operation(node) {
            LogicalOperation::Filter(predicate) => Some(predicate),
            _ => None,
        })
        .flat_map(Predicate::conjuncts)
        .any(|conjunct| match conjunct {
            Conjunct::Leaf { predicate, negated } if predicate.exprs() == [kind] => match predicate
            {
                Predicate::Compare {
                    comparison: Comparison::Ne,
                    ..
                } => negated,
                _ => !negated,
            },
            _ => false,
        })
}

fn compares_bindings(leaf: &Predicate) -> bool {
    let Predicate::Compare {
        left,
        right: Operand::Expr(right),
        ..
    } = leaf
    else {
        return false;
    };
    match left {
        Expr::Property { .. } | Expr::Truncate { .. } => binding(*left) != binding(*right),
        _ => true,
    }
}

fn binding(expr: Expr) -> Ref {
    match expr {
        Expr::Property { binding, .. }
        | Expr::Truncate { binding, .. }
        | Expr::Edge { binding, .. }
        | Expr::Attribute { binding, .. }
        | Expr::Node(binding)
        | Expr::Output(binding) => binding,
    }
}
