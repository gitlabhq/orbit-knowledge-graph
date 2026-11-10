use super::limits::MAX_IN_VALUES;
use super::{
    Comparison, Conjunct, EdgeField, Expr, Literal, Logical, LogicalOperation, LogicalType,
    Operand, PlanError, Predicate,
};
use crate::plan::tracking::Construction;
use crate::{Direction, EntityId, Model};

const CONSTANT: &str = "every condition must compare a property of a matched node or relationship with a value or another property";

pub(super) fn conditions(
    model: &Model,
    construction: &Construction<Logical>,
    predicate: &Predicate,
) -> Result<(), PlanError> {
    for leaf in predicate.leaf_predicates() {
        constant(leaf)?;
    }
    kinds(model, predicate)?;
    for conjunct in predicate.conjuncts() {
        let leaves = match conjunct {
            Conjunct::Leaf { predicate, negated } => vec![(predicate, negated)],
            Conjunct::Negated(group) => group
                .leaf_predicates()
                .into_iter()
                .map(|leaf| (leaf, false))
                .collect(),
        };
        for (leaf, negated) in leaves {
            endpoint(model, construction, leaf, negated)?;
        }
    }
    Ok(())
}

fn constant(leaf: &Predicate) -> Result<(), PlanError> {
    let constant = match leaf {
        Predicate::In { values, .. } if values.is_empty() => {
            return Err(PlanError::shape(format!(
                "IN requires 1-{MAX_IN_VALUES} values"
            )));
        }
        Predicate::Compare {
            left,
            right: Operand::Expr(right),
            ..
        } => left == right,
        _ => false,
    };
    match constant {
        true => Err(PlanError::shape(CONSTANT)),
        false => Ok(()),
    }
}

fn is_kind(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Edge {
            field: EdgeField::Kind,
            ..
        }
    )
}

fn kinds(model: &Model, predicate: &Predicate) -> Result<(), PlanError> {
    for conjunct in predicate.conjuncts() {
        let leaf = match conjunct {
            Conjunct::Leaf { predicate, .. } => predicate,
            Conjunct::Negated(group) if group.exprs().iter().any(is_kind) => {
                return Err(PlanError::shape(
                    "type(r) cannot be used inside a NOT group; write it as its own condition joined with AND, such as NOT type(r) = 'CLOSES'",
                ));
            }
            Conjunct::Negated(_) => continue,
        };
        if !leaf.exprs().first().is_some_and(is_kind) {
            continue;
        }
        let names = kind_names(leaf).ok_or_else(|| {
            PlanError::shape(
                "type(r) supports =, <>, or IN with relationship type names, such as type(r) = 'CLOSES' or type(r) IN ['CLOSES', 'MENTIONS']",
            )
        })?;
        if names.iter().any(|name| model.relationship(name).is_err()) {
            return Err(PlanError::shape(
                "type() names a relationship type that the schema does not have; relationship type names are uppercase and case-sensitive, and CALL db.schema() lists them",
            ));
        }
    }
    Ok(())
}

fn kind_names(leaf: &Predicate) -> Option<Vec<&str>> {
    fn text(literal: &Literal) -> Option<&str> {
        match literal {
            Literal::String(name) => Some(name),
            _ => None,
        }
    }
    match leaf {
        Predicate::Compare {
            comparison: Comparison::Eq | Comparison::Ne,
            right: Operand::Literal(literal),
            ..
        } => Some(vec![text(literal)?]),
        Predicate::In { values, .. } if !values.is_empty() => values.iter().map(text).collect(),
        _ => None,
    }
}

fn endpoint(
    model: &Model,
    construction: &Construction<Logical>,
    leaf: &Predicate,
    negated: bool,
) -> Result<(), PlanError> {
    let (binding, field, comparison, values) = match leaf {
        Predicate::Compare {
            left: Expr::Edge { binding, field },
            comparison: comparison @ (Comparison::Eq | Comparison::Ne),
            right: Operand::Literal(literal),
        } => {
            let flipped = match (comparison, negated) {
                (Comparison::Eq, true) => Comparison::Ne,
                (Comparison::Ne, true) => Comparison::Eq,
                (comparison, _) => *comparison,
            };
            (binding, field, Some(flipped), std::slice::from_ref(literal))
        }
        Predicate::In {
            left: Expr::Edge { binding, field },
            values,
        } => (binding, field, None, values.as_slice()),
        _ => return Ok(()),
    };
    let source = match field {
        EdgeField::SourceKind => true,
        EdgeField::TargetKind => false,
        _ => return Ok(()),
    };
    let Some(allowed) = endpoint_kinds(model, construction, *binding, source)? else {
        return Ok(());
    };
    let Some(kinds) = values
        .iter()
        .map(|value| match value {
            Literal::String(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<&str>>>()
    else {
        return Ok(());
    };
    let name = if source { "source_kind" } else { "target_kind" };
    if comparison == Some(Comparison::Ne)
        && let ([only], [kind]) = (allowed.as_slice(), kinds.as_slice())
        && only == kind
    {
        return Err(PlanError::shape(format!(
            "filter on \"{name}\": <> {only:?} excludes the only value this pattern allows; remove the filter"
        )));
    }
    match kinds
        .into_iter()
        .find(|kind| !allowed.iter().any(|allowed| allowed == kind))
    {
        Some(kind) if allowed.is_empty() => Err(PlanError::shape(format!(
            "filter on \"{name}\": {kind:?} cannot match this pattern"
        ))),
        Some(kind) => Err(PlanError::shape(format!(
            "filter on \"{name}\": {kind:?} cannot match this pattern. Valid values: {}",
            allowed.join(", ")
        ))),
        None => Ok(()),
    }
}

fn endpoint_kinds(
    model: &Model,
    construction: &Construction<Logical>,
    edge: crate::plan::tracking::Ref,
    source: bool,
) -> Result<Option<Vec<String>>, PlanError> {
    let LogicalOperation::Follow {
        from,
        direction,
        range,
        hops,
        ..
    } = construction.producer(edge)?
    else {
        return Ok(None);
    };
    let near = match construction.value(*from)? {
        (_, LogicalType::Entity(entity)) => Some(*entity),
        _ => None,
    };
    let mut reached: Vec<EntityId> = hops.iter().map(|hop| hop.to).collect();
    reached.dedup();
    let far = match reached.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    let endpoints = match (direction, source) {
        (Direction::Outgoing, true) | (Direction::Incoming, false) => vec![near],
        (Direction::Outgoing, false) | (Direction::Incoming, true) => vec![far],
        (Direction::Both, true) => vec![near, far],
        (Direction::Both, false) => vec![far, near],
    };
    let graph = model.graph();
    let mut allowed: Vec<String> = match range.max > 1 || endpoints.contains(&None) {
        true => graph.entities().map(|entity| entity.name.clone()).collect(),
        false => endpoints
            .into_iter()
            .flatten()
            .map(|entity| graph.entity(entity).name.clone())
            .collect(),
    };
    allowed.dedup();
    Ok(Some(allowed))
}
