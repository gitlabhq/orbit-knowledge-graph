use std::collections::HashMap;

use query_data_model::plan::logical::{Comparison, Literal, LogicalOperation, Predicate};
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, EntityId, RelationshipId};

use crate::error::{QueryError, Result};
use crate::logical::{Boolean, Leaf, Query, polarized};

type Condition = (Vec<String>, bool);

pub(super) struct Narrowed {
    pub ids: Vec<RelationshipId>,
}

pub(super) fn narrowed(query: Query<'_>) -> Result<HashMap<Ref, Narrowed>> {
    let conditions = conditions(query)?;
    let mut narrowed = HashMap::new();
    for step in query.steps() {
        let current = step.relationships.to_vec();
        let (near, far) = (query.entity(step.from), query.entity(step.to));
        let inferred = match (step.relationships, step.range.max, near.zip(far)) {
            ([], 1, Some((near, far))) => match step.direction {
                Direction::Incoming => infer(query, Direction::Outgoing, Some(far), Some(near)),
                direction => infer(query, direction, Some(near), Some(far)),
            },
            _ => current.clone(),
        };
        let ids = apply(query, inferred, conditions.get(&step.edge))?;
        if ids != current {
            narrowed.insert(step.edge, Narrowed { ids });
        }
    }
    for (node, operation) in query.operations() {
        let (relationships, inferred) = match operation {
            LogicalOperation::Neighbors {
                from,
                relationships,
                direction,
            } => match (relationships.is_empty(), query.entity(*from)) {
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
        let current = relationships.to_vec();
        let inferred = inferred.unwrap_or_else(|| current.clone());
        let ids = apply(query, inferred, conditions.get(&edge))?;
        if ids != current {
            narrowed.insert(edge, Narrowed { ids });
        }
    }
    Ok(narrowed)
}

fn infer(
    query: Query<'_>,
    direction: Direction,
    source: Option<EntityId>,
    target: Option<EntityId>,
) -> Vec<RelationshipId> {
    let graph = query.model.graph();
    match direction {
        Direction::Outgoing => graph.relationships_between(source, target),
        Direction::Incoming => graph.relationships_between(target, source),
        Direction::Both => {
            let mut both: Vec<RelationshipId> = graph
                .relationships_between(source, target)
                .into_iter()
                .chain(graph.relationships_between(target, source))
                .collect();
            both.sort_by(|a, b| {
                graph
                    .relationship(*a)
                    .name
                    .cmp(&graph.relationship(*b).name)
            });
            both.dedup();
            both
        }
    }
}

fn apply(
    query: Query<'_>,
    mut ids: Vec<RelationshipId>,
    conditions: Option<&Vec<Condition>>,
) -> Result<Vec<RelationshipId>> {
    let graph = query.model.graph();
    let named =
        |id: &RelationshipId, names: &[String]| names.contains(&graph.relationship(*id).name);
    for (names, negated) in conditions.into_iter().flatten() {
        if ids.is_empty() {
            ids = match negated {
                true => graph.relationships_between(None, None),
                false => names
                    .iter()
                    .map(|name| {
                        graph
                            .relationships()
                            .find(|relationship| relationship.name == *name)
                            .map(|relationship| relationship.id)
                            .ok_or_else(|| {
                                QueryError::PipelineInvariant(format!(
                                    "the builder admits only declared type() names, not {name}"
                                ))
                            })
                    })
                    .collect::<Result<_>>()?,
            };
        }
        let allowed = ids
            .iter()
            .map(|id| graph.relationship(*id).name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        ids.retain(|id| named(id, names) != *negated);
        if ids.is_empty() {
            return Err(QueryError::Validation(format!(
                "type(r) excludes every relationship type the pattern allows. Valid values: {allowed}"
            )));
        }
    }
    Ok(ids)
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
