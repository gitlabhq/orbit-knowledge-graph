mod aggregate;
mod classify;
mod conditions;
mod returns;
mod scope;

use super::ast::{BooleanExpression, Predicate as Leaf, Query};
use crate::RelationshipId;
use crate::plan::logical::{LogicalBuilder, LogicalCatalog, PlanError, Predicate, Rows, Step};
use crate::plan::tracking::Declared;
use crate::surface::{Statement, SurfaceError};
use scope::{Node, Scope, Shape};

type Built<'m> = Result<Rows<'m>, SurfaceError>;
type Conditions<'i> = Vec<BooleanExpression<Leaf<'i>>>;

const DEFAULT_LIMIT: u32 = 30;

pub(super) fn lower(
    query: Query<'_>,
    catalog: LogicalCatalog<'_>,
) -> Result<Statement, SurfaceError> {
    let Query {
        pattern,
        predicates,
        projections,
        order,
        skip,
        limit,
        debug,
    } = query;
    let mut scope = Scope::collect(pattern)?;
    scope.classify(&conditions::referenced(&predicates))?;
    let plan = catalog.build(|q| -> Built<'_> {
        let rows = match &scope.shape {
            Shape::Match => matched(q, &scope, predicates)?,
            Shape::Path(name) => path(q, &scope, predicates, name)?,
            Shape::Neighbors(name) => neighbors(q, &scope, predicates, name)?,
        };
        let declared = scope.nodes.iter().map(|node| rows.binding(&node.alias));
        q.declare_order(declared.collect::<Result<Vec<_>, _>>()?)?;
        let rows = returns::project(q, rows, &scope, projections, order)?;
        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        Ok(q.slice(rows, limit.into(), skip.map(u64::from))?)
    })?;
    Ok(Statement { plan, debug })
}

fn bind<'m>(q: &mut LogicalBuilder<'m>, node: &Node<'_>) -> Built<'m> {
    let label = node.label.as_deref().unwrap_or_default();
    let rows = q.entities_as(q.catalog().entity(label)?, &node.alias)?;
    inline(q, rows, node)
}

fn inline<'m>(q: &mut LogicalBuilder<'m>, rows: Rows<'m>, node: &Node<'_>) -> Built<'m> {
    let binding = rows.binding(&node.alias)?;
    let predicates = conditions::entries(&node.properties, |name| Ok(binding.property(name)?))?;
    conjoin(q, rows, predicates)
}

fn conjoin<'m>(
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

fn relationships(
    q: &LogicalBuilder<'_>,
    types: &[String],
) -> Result<Vec<Declared<RelationshipId>>, SurfaceError> {
    Ok(types
        .iter()
        .map(|name| q.catalog().relationship(name))
        .collect::<Result<_, _>>()?)
}

fn filtered<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    scope: &Scope<'_>,
    predicates: Conditions<'_>,
) -> Built<'m> {
    if predicates.is_empty() {
        return Ok(rows);
    }
    let predicate = conditions::tree(BooleanExpression::And(predicates), &rows, scope)?;
    Ok(q.filter(rows, predicate)?)
}

fn matched<'m>(
    q: &mut LogicalBuilder<'m>,
    scope: &Scope<'_>,
    predicates: Conditions<'_>,
) -> Built<'m> {
    let first = &scope.nodes[0];
    let mut rows = bind(q, first)?;
    let mut bound = vec![first.alias.as_str()];
    for edge in &scope.edges {
        let forward = bound.contains(&edge.from.as_str());
        let (near, far) = if forward {
            (&edge.from, &edge.to)
        } else {
            (&edge.to, &edge.from)
        };
        let direction = match forward {
            true => edge.direction,
            false => classify::reverse(edge.direction),
        };
        let target = scope
            .node(far)
            .ok_or_else(|| PlanError::shape("unbound endpoint"))?;
        let label = target.label.as_deref().unwrap_or_default();
        let mut step = Step::new(direction)
            .via(relationships(q, &edge.types)?)
            .to(q.catalog().entity(label)?)
            .hops(edge.hops)
            .named(far);
        if let Some(variable) = &edge.variable {
            step = step.edge(variable);
        }
        let from = rows.binding(near)?;
        rows = q.traverse(rows, from, step)?;
        let link = rows.edge().ok_or(PlanError::NoEntity)?;
        let predicates =
            conditions::entries(&edge.properties, |name| Ok(rows.edge_column(link, name)?))?;
        rows = conjoin(q, rows, predicates)?;
        rows = inline(q, rows, target)?;
        bound.push(far);
    }
    filtered(q, rows, scope, predicates)
}

fn flatten(predicates: Conditions<'_>) -> Conditions<'_> {
    predicates
        .into_iter()
        .flat_map(|tree| match tree {
            BooleanExpression::And(all) => flatten(all),
            tree => vec![tree],
        })
        .collect()
}

fn local<'i>(
    predicates: Conditions<'i>,
    aliases: &[&str],
) -> Result<(Vec<Conditions<'i>>, Conditions<'i>), SurfaceError> {
    let mut split: Vec<Conditions<'i>> = aliases.iter().map(|_| Vec::new()).collect();
    let mut typed = Vec::new();
    for conjunct in flatten(predicates) {
        if conditions::typed(&conjunct) {
            typed.push(conjunct);
            continue;
        }
        let referenced = conditions::referenced(std::slice::from_ref(&conjunct));
        let owner = aliases
            .iter()
            .position(|alias| referenced.iter().all(|name| name == alias))
            .ok_or_else(|| {
                PlanError::shape(
                    "each condition of a path or neighbors query must reference one endpoint",
                )
            })?;
        split[owner].push(conjunct);
    }
    Ok((split, typed))
}

fn path<'m>(
    q: &mut LogicalBuilder<'m>,
    scope: &Scope<'_>,
    predicates: Conditions<'_>,
    name: &str,
) -> Built<'m> {
    let edge = &scope.edges[0];
    if edge.types.is_empty() {
        return Err(SurfaceError::schema(
            "shortest paths require named relationship types, such as -[:CONTAINS*1..3]->; a wildcard expands over every edge type",
        ));
    }
    let (split, typed) = local(predicates, &[&edge.from, &edge.to])?;
    if !typed.is_empty() {
        return Err(
            PlanError::shape("type() conditions on a shortest path are unsupported").into(),
        );
    }
    let mut split = split.into_iter();
    let mut endpoint = |q: &mut LogicalBuilder<'m>, alias: &str| {
        let node = scope
            .node(alias)
            .ok_or_else(|| PlanError::shape("unbound endpoint"))?;
        let rows = bind(q, node)?;
        filtered(q, rows, scope, split.next().unwrap_or_default())
    };
    let start = endpoint(q, &edge.from)?;
    let end = endpoint(q, &edge.to)?;
    let via = relationships(q, &edge.types)?;
    Ok(q.shortest(start, end, &via, edge.hops.max, name)?)
}

fn neighbors<'m>(
    q: &mut LogicalBuilder<'m>,
    scope: &Scope<'_>,
    predicates: Conditions<'_>,
    name: &str,
) -> Built<'m> {
    let (center, edge) = (&scope.nodes[0], &scope.edges[0]);
    let rows = bind(q, center)?;
    let (mut split, typed) = local(predicates, &[&center.alias])?;
    let rows = filtered(q, rows, scope, split.remove(0))?;
    let from = rows.binding(&center.alias)?;
    let via = relationships(q, &edge.types)?;
    let variable = edge.variable.as_deref();
    let rows = q.neighbors(rows, from, &via, edge.direction, name, variable)?;
    filtered(q, rows, scope, typed)
}
