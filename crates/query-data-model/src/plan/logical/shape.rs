use super::limits::RESERVED_PREFIX;
use super::{Expr, LogicalOperation, LogicalPlan, LogicalType, Operand, PlanError, Predicate};
use crate::plan::tracking::Ref;

pub(super) fn plan(plan: &LogicalPlan) -> Result<(), PlanError> {
    reserved(plan)?;
    overlapping(plan)?;
    conditions(plan)
}

fn reserved(plan: &LogicalPlan) -> Result<(), PlanError> {
    let names = plan
        .nodes()
        .flat_map(|node| plan.outputs(node))
        .filter_map(|output| plan.name(output));
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
    }
    Ok(())
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
