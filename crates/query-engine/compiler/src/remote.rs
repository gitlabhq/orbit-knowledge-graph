//! The pieces of code and live stages the DAG runner reads: the served targets, the scope
//! lookups their reads need, and the row checks their rows go through.

use query_data_model::Model;
use query_data_model::plan::logical::LogicalPlan;

pub use crate::passes::plan::remote::{RemotePlan, Target};
pub use crate::passes::remote_security::Namespaces;

pub use crate::passes::scope_lookup::{KEY, Resolves, ScopeLookup, lookups, namespaces};

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
