//! Access restriction on the logical plan.
//!
//! Rejects entities whose role floor no authorized path meets, and, for
//! callers that are not instance administrators, every read of an
//! `admin_only` property and every aggregation in which a globally scoped
//! entity is not reachable from a `traversal_path`-scoped one, since
//! aggregation bypasses the Rails redaction layer.

mod hydration;
mod properties;
#[cfg(test)]
mod tests;

pub use hydration::restrict_hydration;

use std::collections::HashSet;

use query_data_model::plan::logical::{LogicalOperation, LogicalPlan};
use query_data_model::plan::tracking::Ref;
use query_data_model::{EntityId, Model, OrbitQueryModel};

use crate::error::{QueryError, Result};
use crate::input::QueryType;
use crate::logical::Query;
use crate::types::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};

pub fn restrict(
    plan: &LogicalPlan,
    logical: &Model,
    model: &impl OrbitQueryModel,
    security_ctx: &SecurityContext,
) -> Result<()> {
    let query = Query::new(plan, logical);
    let entities = entities(query);
    for (_, entity) in &entities {
        role_floor(query, *entity, model, security_ctx)?;
    }
    if security_ctx.admin {
        return Ok(());
    }
    if query.kind() == QueryType::Aggregation {
        aggregation_scope(query, &entities, model)?;
    }
    properties::admin_only(query, model)
}

fn entities(query: Query<'_>) -> Vec<(Ref, EntityId)> {
    query
        .operations()
        .filter(|(_, operation)| {
            matches!(
                operation,
                LogicalOperation::Entities(_) | LogicalOperation::Follow { .. }
            )
        })
        .filter_map(|(node, _)| {
            let output = query.plan.outputs(node).last()?;
            Some((output, query.entity(output)?))
        })
        .collect()
}

pub(super) fn role_floor(
    query: Query<'_>,
    entity: EntityId,
    model: &impl OrbitQueryModel,
    security_ctx: &SecurityContext,
) -> Result<()> {
    let Some(min_role) = model
        .required_access_level(entity)
        .filter(|level| *level > DEFAULT_PATH_ACCESS_LEVEL)
    else {
        return Ok(());
    };
    if security_ctx.paths_at_least(min_role).is_empty() {
        let name = &query.model.graph().entity(entity).name;
        return Err(QueryError::Authorization(format!(
            "{name} requires access level {min_role} on at least one authorized path"
        )));
    }
    Ok(())
}

fn aggregation_scope(
    query: Query<'_>,
    entities: &[(Ref, EntityId)],
    model: &impl OrbitQueryModel,
) -> Result<()> {
    let graph = query.model.graph();
    let mut reachable: HashSet<Ref> = entities
        .iter()
        .filter(|(_, entity)| model.entity_has_traversal_path(&graph.entity(*entity).name))
        .map(|(output, _)| *output)
        .collect();
    if reachable.is_empty() {
        return Err(QueryError::Restrict(
            "aggregation requires at least one node scoped by traversal_path \
             (e.g. Group, Project, Note); aggregating on globally-scoped entities \
             such as User alone is not permitted"
                .into(),
        ));
    }
    let mut edges: Vec<(Ref, Ref)> = query.steps().iter().map(|s| (s.from, s.to)).collect();
    edges.extend(
        query
            .operations()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Shortest { from, to, .. } => Some((*from, *to)),
                _ => None,
            }),
    );
    loop {
        let before = reachable.len();
        for (a, b) in &edges {
            if reachable.contains(a) || reachable.contains(b) {
                reachable.insert(*a);
                reachable.insert(*b);
            }
        }
        if reachable.len() == before {
            break;
        }
    }
    if let Some((orphan, _)) = entities
        .iter()
        .find(|(output, _)| !reachable.contains(output))
    {
        return Err(QueryError::Restrict(format!(
            "aggregation node \"{}\" is globally-scoped and must be connected to a \
             traversal_path-scoped node via \"relationships\" or \"path\"",
            query.name(*orphan)
        )));
    }
    Ok(())
}
