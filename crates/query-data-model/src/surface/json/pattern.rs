use serde_json::Value;

use super::filters;
use super::request::{Node, Relationship, Request};
use crate::plan::logical::{Comparison, Hops, LogicalBuilder, PlanError, Predicate, Rows, Step};
use crate::plan::tracking::Declared;
use crate::surface::SurfaceError;
use crate::{Direction, RelationshipId};

type Built<'m> = Result<Rows<'m>, SurfaceError>;

pub(super) fn node<'m>(q: &mut LogicalBuilder<'m>, node: &Node) -> Built<'m> {
    let entity = q.catalog().entity(&node.entity)?;
    let rows = q.entities_as(entity, &node.id)?;
    restrict(q, rows, node)
}

pub(super) fn restrict<'m>(q: &mut LogicalBuilder<'m>, rows: Rows<'m>, node: &Node) -> Built<'m> {
    let binding = rows.binding(&node.id)?;
    let key = binding.property(node.id_property.as_deref().unwrap_or("id"))?;
    let rows = match node.node_ids.as_slice() {
        [] => rows,
        ids => {
            let ids = ids.iter().map(filters::id).collect::<Result<Vec<_>, _>>()?;
            q.pin(rows, key, &ids)?
        }
    };
    let mut predicates = Vec::new();
    if let Some(range) = &node.id_range {
        let (start, end) = (filters::id(&range.start)?, filters::id(&range.end)?);
        predicates.push(key.compare(Comparison::Ge, start));
        predicates.push(key.compare(Comparison::Le, end));
    }
    predicates.extend(filters::conditions(&node.filters, |name| {
        Ok(binding.property(name)?)
    })?);
    for (property, entry) in &node.not {
        let single = serde_json::Map::from_iter([(property.clone(), entry.clone())]);
        let negated = filters::conditions(&single, |name| Ok(binding.property(name)?))?;
        predicates.push(!Predicate::And(negated));
    }
    conjoin(q, rows, predicates)
}

pub(super) fn conjoin<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    mut predicates: Vec<Predicate>,
) -> Built<'m> {
    Ok(match predicates.len() {
        0 => rows,
        1 => q.filter(rows, predicates.remove(0))?,
        _ => q.filter(rows, Predicate::And(predicates))?,
    })
}

pub(super) fn relationships(
    q: &LogicalBuilder<'_>,
    names: &[String],
) -> Result<Vec<Declared<RelationshipId>>, SurfaceError> {
    if names.iter().any(|name| name == "*") {
        if names.len() > 1 {
            return Err(SurfaceError::schema(
                "the \"*\" relationship type cannot be combined with named types; list only the types to follow",
            ));
        }
        return Ok(Vec::new());
    }
    Ok(names
        .iter()
        .map(|name| q.catalog().relationship(name))
        .collect::<Result<_, _>>()?)
}

pub(super) fn pattern<'m>(q: &mut LogicalBuilder<'m>, request: &Request) -> Built<'m> {
    let first = &request.nodes[0];
    let mut rows = node(q, first)?;
    let mut bound = vec![first.id.as_str()];
    let mut pending: Vec<_> = request.relationships.iter().enumerate().collect();
    while let Some(position) = pending.iter().position(|(_, edge)| {
        bound.contains(&edge.from.as_str()) || bound.contains(&edge.to.as_str())
    }) {
        let (index, edge) = pending.remove(position);
        let forward = bound.contains(&edge.from.as_str());
        let (near, far) = if forward {
            (&edge.from, &edge.to)
        } else {
            (&edge.to, &edge.from)
        };
        if bound.contains(&far.as_str()) {
            return Err(PlanError::shape(format!(
                "relationship[{index}] closes a cycle or repeats an endpoint; each relationship must reach one new node"
            ))
            .into());
        }
        let target = declared(request, far, index)?;
        rows = step(q, rows, edge, near, target, forward)?;
        rows = restrict(q, rows, target)?;
        bound.push(far);
    }
    if let Some((index, edge)) = pending.first() {
        let missing = [&edge.from, &edge.to]
            .into_iter()
            .find(|alias| !request.nodes.iter().any(|node| &node.id == *alias));
        return Err(match missing {
            Some(alias) => PlanError::UnknownBinding {
                name: alias.clone(),
                available: request.nodes.iter().map(|node| node.id.clone()).collect(),
            },
            None => PlanError::shape(format!(
                "relationship[{index}] does not connect to the pattern that starts at {}",
                first.id
            )),
        }
        .into());
    }
    match request
        .nodes
        .iter()
        .find(|node| !bound.contains(&node.id.as_str()))
    {
        Some(node) => Err(PlanError::shape(format!(
            "node \"{}\" is not connected to the pattern by any relationship",
            node.id
        ))
        .into()),
        None => Ok(rows),
    }
}

fn declared<'r>(request: &'r Request, alias: &str, index: usize) -> Result<&'r Node, SurfaceError> {
    request
        .nodes
        .iter()
        .find(|node| node.id == alias)
        .ok_or_else(|| {
            PlanError::UnknownBinding {
                name: format!("{alias} in relationship[{index}]"),
                available: request.nodes.iter().map(|node| node.id.clone()).collect(),
            }
            .into()
        })
}

fn step<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    edge: &Relationship,
    near: &str,
    target: &Node,
    forward: bool,
) -> Built<'m> {
    let names: Vec<String> = match &edge.types {
        Value::String(name) => vec![name.clone()],
        Value::Array(names) => names
            .iter()
            .filter_map(|name| name.as_str().map(str::to_string))
            .collect(),
        other => {
            return Err(SurfaceError::schema(format!(
                "relationship type {other} is not a name"
            )));
        }
    };
    let direction = Direction::from(edge.direction);
    let direction = match (forward, direction) {
        (false, Direction::Outgoing) => Direction::Incoming,
        (false, Direction::Incoming) => Direction::Outgoing,
        (_, direction) => direction,
    };
    let [min, max] = edge.hops.unwrap_or([1, 1]);
    let step = Step::new(direction)
        .via(relationships(q, &names)?)
        .to(q.catalog().entity(&target.entity)?)
        .hops(Hops { min, max })
        .named(&target.id);
    let from = rows.binding(near)?;
    let rows = q.traverse(rows, from, step)?;
    let link = rows.edge().ok_or(PlanError::NoEntity)?;
    let predicates = filters::conditions(&edge.filters, |name| Ok(rows.edge_column(link, name)?))?;
    conjoin(q, rows, predicates)
}
