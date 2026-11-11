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
use orbit_server_config::QueryLimits;
use query_data_model::catalog::{Check, Row};
use query_data_model::plan::logical::LogicalOperation;

/// A row check the remote rows of a read go through, on the field that holds each row's key.
#[derive(Debug, Clone, PartialEq)]
pub struct RowCheck {
    pub provider: String,
    pub permission: String,
    pub resource: String,
    pub field: String,
    /// The type of the key the check reads from `field`.
    pub kind: crate::KeyKind,
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
                kind: crate::KeyKind::of(graph.property(*id).data_type),
            }),
            Check::Delegated { .. } => None,
        })
        .collect()
}

/// The rows whose key the check allows; a row without the key fails closed.
pub fn apply_check(
    rows: Vec<Row>,
    check: &RowCheck,
    allowed: &std::collections::BTreeSet<orbit_authorization::Key>,
) -> Vec<Row> {
    rows.into_iter()
        .filter(|row| {
            row.get(&check.field)
                .and_then(|value| row_key(check.kind, value))
                .is_some_and(|key| allowed.contains(&key))
        })
        .collect()
}

/// A remote row's field as the key of a check, in the type the check keys by.
fn row_key(kind: crate::KeyKind, value: &serde_json::Value) -> Option<orbit_authorization::Key> {
    use orbit_authorization::Key;
    match (kind, value) {
        (crate::KeyKind::Int, serde_json::Value::Number(id)) => id.as_i64().map(Key::Int),
        (crate::KeyKind::Text, serde_json::Value::String(id)) => Some(Key::Text(id.clone())),
        (crate::KeyKind::Uuid, serde_json::Value::String(id)) => Some(Key::Uuid(id.clone())),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum RemoteRequest {
    Code(StructuralSearchRequest),
    Live(ExtractionRequest),
}

pub fn parse(
    raw: &str,
    model: &Model,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<LogicalPlan> {
    let catalog = match grants.caller().admin() {
        true => model.logical(limits).as_admin(),
        false => model.logical(limits),
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
    limits: &QueryLimits,
) -> Result<RemoteRequest> {
    let logical = parse(raw, model, grants, limits)?;
    let (target, planned) = plan(&logical, model)?;
    let emitted = secure(&target, planned, grants, namespaces)?;
    check(&emitted)?;
    render(&target, &emitted)
}
