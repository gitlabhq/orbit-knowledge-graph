use std::collections::{BTreeSet, HashMap};

use query_data_model::plan::logical::{
    Comparison, Literal, LogicalOperation, PlanError, Predicate,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, RelationshipId};

use crate::error::{QueryError, Result};
use crate::logical::{Boolean, Leaf, Query, polarized};

type Condition = (Vec<String>, bool);

pub(super) struct Narrowed {
    pub ids: Vec<RelationshipId>,
}

impl Narrowed {
    fn new(query: Query<'_>, names: Vec<String>) -> Result<Self> {
        let ids = names
            .iter()
            .map(|name| query.model.relationship(name))
            .collect::<std::result::Result<_, _>>()
            .map_err(PlanError::from)?;
        Ok(Self { ids })
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
        let (relationships, inferred) = match operation {
            LogicalOperation::Neighbors {
                from,
                relationships,
                direction,
            } => match (relationships.is_empty(), query.entity_name(*from)) {
                (true, Some(center)) => (
                    relationships,
                    Some(infer(query, *direction, Some(center), None)),
                ),
                _ => (relationships, None),
            },
            LogicalOperation::Shortest { relationships, .. } => (relationships, None),
            _ => continue,
        };
        let outputs: Vec<Ref> = query.plan.outputs(node).collect();
        let [.., edge, _] = outputs[..] else {
            continue;
        };
        let current = names(relationships);
        let inferred = inferred.unwrap_or_else(|| current.clone());
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
        let allowed = types.join(", ");
        types.retain(|kind| names.contains(kind) != *negated);
        if types.is_empty() {
            return Err(QueryError::Validation(format!(
                "type(r) excludes every relationship type the pattern allows. Valid values: {allowed}"
            )));
        }
    }
    Ok(types)
}

fn conditions(query: Query<'_>) -> Result<HashMap<Ref, Vec<Condition>>> {
    let mut found: HashMap<Ref, Vec<Condition>> = HashMap::new();
    for tree in query.filters() {
        let Some((leaf, kept_not)) = type_leaf(&tree) else {
            continue;
        };
        let names = names(leaf).ok_or_else(|| {
            QueryError::PipelineInvariant("the builder admits only named type() conditions".into())
        })?;
        let negated = kept_not != (leaf.comparison() == Some(Comparison::Ne));
        found
            .entry(leaf.binding())
            .or_default()
            .push((names, negated));
    }
    Ok(found)
}

fn type_leaf<'t>(tree: &'t Boolean<Leaf<'_>>) -> Option<(&'t Leaf<'t>, bool)> {
    let (leaf, kept_not) = match tree {
        Boolean::Leaf(leaf) => (leaf, false),
        Boolean::Not(inner) => match &**inner {
            Boolean::Leaf(leaf) => (leaf, true),
            _ => return None,
        },
        Boolean::And(_) => return None,
    };
    leaf.edge_kind().then_some((leaf, kept_not))
}

fn names(leaf: &Leaf<'_>) -> Option<Vec<String>> {
    let text = |literal: &Literal| match literal {
        Literal::String(name) => Some(name.clone()),
        _ => None,
    };
    match &*leaf.predicate {
        Predicate::Compare {
            comparison: Comparison::Eq | Comparison::Ne,
            ..
        }
        | Predicate::In { .. }
            if !leaf.literals().is_empty() =>
        {
            leaf.literals().iter().map(text).collect()
        }
        _ => None,
    }
}

pub(super) fn without_types(predicate: &Predicate) -> Option<Predicate> {
    fn keep(predicate: &Predicate, into: &mut Vec<Predicate>) {
        match predicate {
            Predicate::And(all) => all.iter().for_each(|inner| keep(inner, into)),
            conjunct if matches!(&polarized(conjunct)[..], [tree] if type_leaf(tree).is_some()) => {
            }
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
