//! Planning for the connections that are not SQL: an Orbit Next code plan for structural matches
//! and a live plan for entities fetched at query time.

mod code;
mod live;

use std::collections::BTreeSet;
use std::sync::Arc;

use query_data_model::implementations::live::Live;
use query_data_model::implementations::orbit_code::OrbitCode;
use query_data_model::plan::code::CodePlan;
use query_data_model::plan::live::LivePlan;
use query_data_model::plan::logical::{LogicalOperation, LogicalPlan};
use query_data_model::{Catalog, EntityId, Model, PropertyId, StorageBackend};

use crate::error::{QueryError, Result};

/// The connection one compile targets.
#[derive(Debug)]
pub enum Target {
    Code(Catalog<OrbitCode>),
    Live(Catalog<Live>),
}

#[derive(Debug)]
pub enum RemotePlan {
    Code(CodePlan),
    Live(LivePlan),
}

/// Chooses the connection that holds the requested data and builds its plan in one step.
pub fn plan(logical: &LogicalPlan, model: &Arc<Model>) -> Result<(Target, RemotePlan)> {
    let target = target(logical, model)?;
    let plan = match &target {
        Target::Code(catalog) => RemotePlan::Code(code::plan(logical, catalog)?),
        Target::Live(catalog) => RemotePlan::Live(live::plan(logical, catalog)?),
    };
    Ok((target, plan))
}

fn target(logical: &LogicalPlan, model: &Arc<Model>) -> Result<Target> {
    let entity = logical
        .nodes()
        .find_map(|node| match logical.operation(node) {
            LogicalOperation::Entities(entity) => Some(*entity),
            _ => None,
        })
        .ok_or_else(|| QueryError::Validation("the query reads no entity".into()))?;
    let structural = logical
        .nodes()
        .find_map(|node| match logical.operation(node) {
            LogicalOperation::Structural(structural) => Some(structural.target),
            _ => None,
        });
    let connections: BTreeSet<&str> = model
        .stores()
        .iter()
        .map(|store| store.connection.as_str())
        .collect();
    for connection in connections {
        if let Some(query_data_model::plan::logical::Expr::Property { property, .. }) = structural
            && let Ok(code) = model.physical::<OrbitCode>(connection)
            && searches(&code, entity, property)
        {
            return Ok(Target::Code(code));
        }
        if structural.is_none()
            && let Ok(live) = model.physical::<Live>(connection)
            && live.endpoint(entity).is_some()
        {
            return Ok(Target::Live(live));
        }
    }
    Err(QueryError::Validation(format!(
        "no {} or {} connection serves {}",
        OrbitCode::KIND,
        Live::KIND,
        model.graph().entity(entity).name
    )))
}

fn searches(code: &Catalog<OrbitCode>, entity: EntityId, property: PropertyId) -> bool {
    code.home(entity)
        .is_some_and(|(_, facts)| facts.structural(entity) == Some(property))
}

fn unsupported(backend: &str, what: &str) -> QueryError {
    QueryError::Validation(format!("the {backend} backend cannot {what}"))
}
