//! Access restriction on the logical plan.
//!
//! Rejects entities for which the caller holds the entity's permission in no scope unless a
//! trusted connection reads them, and, for callers that are not instance administrators, every
//! read of an `admin_only` property and every aggregation in which an entity without a scope key
//! is not reachable from one with a scope key, since aggregation bypasses the row checks.

mod hydration;
mod properties;
#[cfg(test)]
mod tests;

pub use hydration::restrict_hydration;

use orbit_authorization::Grants;
use query_data_model::Catalog;
use query_data_model::catalog::Rule;
use std::collections::HashSet;

use query_data_model::plan::logical::{LogicalOperation, LogicalPlan};
use query_data_model::plan::tracking::Ref;
use query_data_model::{EntityId, Model};

use crate::dag::Connections;
use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Query;
use crate::passes::codegen::SqlBackend;
use crate::passes::guards::scope_set;

pub fn restrict(
    plan: &LogicalPlan,
    logical: &Model,
    model: &Catalog<impl SqlBackend>,
    connections: &Connections,
    grants: &Grants,
) -> Result<()> {
    let query = Query::new(plan, logical);
    let entities = entities(query);
    for (_, entity) in &entities {
        readable(*entity, connections.guard(*entity), model, grants)?;
    }
    let admin = grants.caller().admin();
    if !admin && query.kind() == Kind::Aggregation {
        aggregation_scope(query, &entities, model, connections)?;
    }
    properties::fields(query, model, admin)
}

fn entities(query: Query<'_>) -> Vec<(Ref, EntityId)> {
    query
        .bindings()
        .into_iter()
        .map(|binding| (binding.output, binding.entity))
        .collect()
}

/// The caller holds the entity's permission in at least one scope, unless a trusted connection
/// reads it; `guard` is the policy of the connection that reads it.
pub(super) fn readable(
    entity: EntityId,
    guard: Option<&query_data_model::catalog::Guard>,
    model: &Catalog<impl SqlBackend>,
    grants: &Grants,
) -> Result<()> {
    let name = &model.graph().entity(entity).name;
    let guard = guard.ok_or_else(|| {
        QueryError::Security(format!(
            "{name} has no guard on connection {}",
            model.connection()
        ))
    })?;
    if guard.rule == Rule::Trusted {
        return Ok(());
    }
    if scope_set(grants, model.model(), guard.scope, guard.permission)?.is_empty() {
        let access = model.model().access();
        return Err(QueryError::Authorization(format!(
            "{name} requires permission {} in at least one {} scope",
            access.permission(guard.permission).name,
            access.scope(guard.scope).name
        )));
    }
    Ok(())
}

fn scoped(connections: &Connections, entity: EntityId) -> bool {
    connections
        .guard(entity)
        .is_some_and(|guard| matches!(guard.rule, Rule::Column(_) | Rule::Owner(_)))
}

fn aggregation_scope(
    query: Query<'_>,
    entities: &[(Ref, EntityId)],
    model: &Catalog<impl SqlBackend>,
    connections: &Connections,
) -> Result<()> {
    let mut reachable: HashSet<Ref> = entities
        .iter()
        .filter(|(_, entity)| scoped(connections, *entity))
        .map(|(output, _)| *output)
        .collect();
    if reachable.is_empty() {
        let global: Vec<&str> = entities
            .iter()
            .map(|(_, entity)| model.graph().entity(*entity).name.as_str())
            .collect();
        return Err(QueryError::Restrict(format!(
            "aggregation requires at least one node scoped by traversal_path; \
             aggregating on globally-scoped entities ({}) alone is not permitted",
            global.join(", ")
        )));
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
