//! Compiles a GQL query for a connection that is not SQL into its backend's request:
//! parse, plan, security, security check, render.

use std::sync::Arc;

use query_data_model::Model;
use query_data_model::catalog::Render;
use query_data_model::implementations::live::ExtractionRequest;
use query_data_model::implementations::orbit_code::StructuralSearchRequest;
use query_data_model::plan::logical::LogicalPlan;
use serde::Serialize;

pub use crate::passes::plan::remote::{RemotePlan, Target, plan};
pub use crate::passes::remote_security::{Namespaces, check, secure};

pub use crate::passes::scope_lookup::{Resolves, ScopeLookup, lookups, namespaces};

use crate::error::{QueryError, Result};
use orbit_authorization::Grants;
use query_data_model::catalog::{Check, Row};
use query_data_model::plan::logical::LogicalOperation;

/// A row check the remote rows of a read go through, on the field that holds each row's key.
#[derive(Debug, Clone, PartialEq)]
pub struct RowCheck {
    pub provider: String,
    pub permission: String,
    pub resource: String,
    pub field: String,
}

/// The row checks of the entity the plan reads.
pub fn row_checks(plan: &LogicalPlan, model: &Model) -> Vec<RowCheck> {
    let access = model.access();
    let graph = model.graph();
    plan.nodes()
        .filter_map(|node| match plan.operation(node) {
            LogicalOperation::Entities(entity) => Some(*entity),
            _ => None,
        })
        .flat_map(|entity| &access.entity(entity).checks)
        .filter_map(|check| match check {
            Check::Rows {
                provider,
                permission,
                resource,
                id,
                ..
            } => Some(RowCheck {
                provider: provider.clone(),
                permission: access.permission(*permission).name.clone(),
                resource: resource.clone(),
                field: graph.property(*id).name.clone(),
            }),
            Check::Delegated { .. } => None,
        })
        .collect()
}

/// The rows whose key the check allows; a row without the key fails closed.
pub fn apply_check(
    rows: Vec<Row>,
    check: &RowCheck,
    allowed: &std::collections::BTreeSet<i64>,
) -> Vec<Row> {
    rows.into_iter()
        .filter(|row| {
            row.get(&check.field)
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|id| allowed.contains(&id))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum RemoteRequest {
    Code(StructuralSearchRequest),
    Live(ExtractionRequest),
}

pub fn parse(raw: &str, model: &Model, grants: &Grants) -> Result<LogicalPlan> {
    let catalog = match grants.caller().admin() {
        true => model.logical().as_admin(),
        false => model.logical(),
    };
    Ok(query_data_model::surface::gql::parse(raw, catalog)?.plan)
}

pub fn render(target: &Target, plan: &RemotePlan) -> Result<RemoteRequest> {
    match (target, plan) {
        (Target::Code(code), RemotePlan::Code(plan)) => Ok(RemoteRequest::Code(code.render(plan)?)),
        (Target::Live(live), RemotePlan::Live(plan)) => Ok(RemoteRequest::Live(live.render(plan)?)),
        _ => Err(QueryError::PipelineInvariant(
            "the plan does not match its target".into(),
        )),
    }
}

pub fn compile(
    raw: &str,
    model: &Arc<Model>,
    grants: &Grants,
    namespaces: &Namespaces,
) -> Result<RemoteRequest> {
    let logical = parse(raw, model, grants)?;
    let (target, planned) = plan(&logical, model)?;
    let emitted = secure(&target, planned, grants, namespaces)?;
    check(&emitted)?;
    render(&target, &emitted)
}
