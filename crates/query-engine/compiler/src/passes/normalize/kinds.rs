use std::collections::{BTreeSet, HashMap};

use query_data_model::plan::logical::{
    Comparison, Literal, LogicalOperation, PlanError, Predicate,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, RelationshipId};

use crate::error::{QueryError, Result};
use crate::logical::{Leaf, Literals, Query};

type Condition = (Vec<String>, bool);

pub(super) struct Narrowed {
    pub names: Vec<String>,
    pub ids: Vec<RelationshipId>,
}

impl Narrowed {
    fn new(query: Query<'_>, names: Vec<String>) -> Result<Self> {
        let ids = names
            .iter()
            .map(|name| query.model.relationship(name))
            .collect::<std::result::Result<_, _>>()
            .map_err(PlanError::from)?;
        Ok(Self { names, ids })
    }
}

pub(super) fn narrowed(query: Query<'_>) -> Result<HashMap<Ref, Narrowed>> {
    let conditions = conditions(query)?;
    let graph = query.model.graph();
    let names = |ids: &[RelationshipId]| -> Vec<String> {
        ids.iter()
            .map(|id| graph.relationship(*id).name.clone())
            .collect()
    };
    let mut narrowed = HashMap::new();
    for step in query.steps() {
        let current = names(step.relationships);
        let (near, far) = (query.entity_name(step.from), query.entity_name(step.to));
        let inferred = match (step.relationships, step.range.max, near.zip(far)) {
            ([], 1, Some((near, far))) => match step.direction {
                Direction::Incoming => infer(query, Direction::Outgoing, Some(far), Some(near)),
                direction => infer(query, direction, Some(near), Some(far)),
            },
            _ => current.clone(),
        };
        let types = apply(query, inferred, conditions.get(&step.edge))?;
        if types != current {
            narrowed.insert(step.edge, Narrowed::new(query, types)?);
        }
    }
    for (node, operation) in query.operations() {
        let LogicalOperation::Neighbors {
            from,
            relationships,
            direction,
        } = operation
        else {
            continue;
        };
        let outputs: Vec<Ref> = query.plan.outputs(node).collect();
        let [.., edge, _] = outputs[..] else {
            continue;
        };
        let current = names(relationships);
        let inferred = match (relationships.is_empty(), query.entity_name(*from)) {
            (true, Some(center)) => infer(query, *direction, Some(center), None),
            _ => current.clone(),
        };
        let types = apply(query, inferred, conditions.get(&edge))?;
        if types != current {
            narrowed.insert(edge, Narrowed::new(query, types)?);
        }
    }
    Ok(narrowed)
}

fn infer(
    query: Query<'_>,
    direction: Direction,
    source: Option<&str>,
    target: Option<&str>,
) -> Vec<String> {
    let graph = query.model.graph();
    match direction {
        Direction::Outgoing => graph.relationship_names(source, target),
        Direction::Incoming => graph.relationship_names(target, source),
        Direction::Both => graph
            .relationship_names(source, target)
            .into_iter()
            .chain(graph.relationship_names(target, source))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn apply(
    query: Query<'_>,
    mut types: Vec<String>,
    conditions: Option<&Vec<Condition>>,
) -> Result<Vec<String>> {
    for (names, negated) in conditions.into_iter().flatten() {
        if types.is_empty() {
            types = match negated {
                true => query.model.graph().relationship_names(None, None),
                false => names.clone(),
            };
        }
        types.retain(|kind| names.contains(kind) != *negated);
        if types.is_empty() {
            return Err(QueryError::Validation(
                "type(r) excludes every relationship type the pattern allows".into(),
            ));
        }
    }
    Ok(types)
}

fn conditions(query: Query<'_>) -> Result<HashMap<Ref, Vec<Condition>>> {
    let mut found: HashMap<Ref, Vec<Condition>> = HashMap::new();
    for leaf in query.conjuncts().into_iter().filter_map(type_leaf) {
        let names = names(leaf).ok_or_else(|| {
            QueryError::PipelineInvariant("the builder admits only named type() conditions".into())
        })?;
        let negated = leaf.negated != (leaf.comparison() == Some(Comparison::Ne));
        found
            .entry(leaf.binding())
            .or_default()
            .push((names, negated));
    }
    Ok(found)
}

fn type_leaf(conjunct: &Predicate) -> Option<Leaf<'_>> {
    Leaf::of(conjunct).filter(|leaf| leaf.edge_kind())
}

fn names(leaf: Leaf<'_>) -> Option<Vec<String>> {
    let text = |literal: &Literal| match literal {
        Literal::String(name) => Some(name.clone()),
        _ => None,
    };
    match (leaf.comparison(), leaf.literals()) {
        (Some(Comparison::Eq | Comparison::Ne), Literals::One(literal)) => {
            text(literal).map(|name| vec![name])
        }
        (None, Literals::Many(values)) if !values.is_empty() => values.iter().map(text).collect(),
        _ => None,
    }
}

pub(super) fn without_types(predicate: &Predicate) -> Option<Predicate> {
    fn keep(predicate: &Predicate, into: &mut Vec<Predicate>) {
        match predicate {
            Predicate::And(all) => all.iter().for_each(|inner| keep(inner, into)),
            conjunct if type_leaf(conjunct).is_some() => {}
            conjunct => into.push(conjunct.clone()),
        }
    }
    let mut kept = Vec::new();
    keep(predicate, &mut kept);
    match kept.len() {
        0 | 1 => kept.pop(),
        _ => Some(Predicate::And(kept)),
    }
}
