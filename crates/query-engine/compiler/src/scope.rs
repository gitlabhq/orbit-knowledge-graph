//! Scope-key derivation for traversal-path resolution.
//!
//! The querying pipeline's path-resolution stage uses these to read which
//! Project/Group scope a node pins (by id or full_path), look the tight
//! traversal_path prefix up in the graph DB, and attach it to the
//! `SecurityContext` as scope metadata. Pure derivation, no DB calls.

use ontology::TraversalPathKind;

use crate::input::{FilterOp, InputFilter, InputNode};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathScopeId {
    Numeric(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathResolutionKey {
    pub entity: String,
    pub kind: TraversalPathKind,
    pub value: PathScopeId,
}

impl PathResolutionKey {
    pub fn id(entity: impl Into<String>, id: i64) -> Self {
        Self {
            entity: entity.into(),
            kind: TraversalPathKind::Id,
            value: PathScopeId::Numeric(id),
        }
    }

    pub fn full_path(entity: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            kind: TraversalPathKind::FullPath,
            value: PathScopeId::Text(value.into()),
        }
    }
}

pub fn scope_keys(node: &InputNode) -> Vec<PathResolutionKey> {
    let Some(entity) = node.entity.as_deref() else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    if let Some(id) = single_id(node) {
        keys.push(PathResolutionKey::id(entity, id));
    }
    if let Some(value) = single_full_path(node) {
        keys.push(PathResolutionKey::full_path(entity, value));
    }
    keys
}

fn single_id(node: &InputNode) -> Option<i64> {
    if node.node_ids.len() == 1 {
        return Some(node.node_ids[0]);
    }
    if !node.node_ids.is_empty() {
        return None;
    }
    eq_value(node.filters.get("id")?)?.as_i64()
}

fn single_full_path(node: &InputNode) -> Option<String> {
    eq_value(node.filters.get("full_path")?)?
        .as_str()
        .map(str::to_string)
}

fn eq_value(filters: &[InputFilter]) -> Option<&serde_json::Value> {
    let [filter] = filters else { return None };
    matches!(filter.op, Some(FilterOp::Eq))
        .then_some(filter.value.as_ref())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn project_node(id: &str) -> InputNode {
        InputNode {
            id: id.to_string(),
            entity: Some("Project".to_string()),
            has_traversal_path: true,
            node_ids: vec![42],
            ..Default::default()
        }
    }

    #[test]
    fn node_ids_single_yields_id_key() {
        assert_eq!(
            scope_keys(&project_node("p")),
            vec![PathResolutionKey::id("Project", 42)]
        );
    }

    #[test]
    fn multi_id_yields_no_key() {
        let mut node = project_node("p");
        node.node_ids = vec![1, 2, 3];
        assert!(scope_keys(&node).is_empty());
    }

    #[test]
    fn eq_full_path_filter_yields_full_path_key() {
        let mut node = InputNode {
            id: "p".to_string(),
            entity: Some("Project".to_string()),
            has_traversal_path: true,
            ..Default::default()
        };
        node.filters.insert(
            "full_path".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(json!("group/project")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node),
            vec![PathResolutionKey::full_path("Project", "group/project")]
        );
    }

    #[test]
    fn entityless_node_yields_no_key() {
        let node = InputNode {
            id: "p".to_string(),
            entity: None,
            node_ids: vec![42],
            ..Default::default()
        };
        assert!(scope_keys(&node).is_empty());
    }
}
