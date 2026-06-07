//! Scope-key derivation for traversal-path resolution.
//!
//! The querying pipeline's path-resolution stage uses these to read which
//! Project/Group scope a node pins (by id or full_path), look the tight
//! traversal_path prefix up in the graph DB, and attach it to the
//! `SecurityContext` as scope metadata. Pure derivation, no DB calls.

use ontology::{ScopeEdge, TraversalPathKind};

use crate::input::{FilterOp, Input, InputFilter, InputNode};

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

pub fn scope_keys(node: &InputNode, anchor_fks: &[(&str, &str)]) -> Vec<PathResolutionKey> {
    let Some(entity) = node.entity.as_deref() else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    if node.node_ids.len() > 1 {
        for &id in &node.node_ids {
            keys.push(PathResolutionKey::id(entity, id));
        }
    } else if let Some(id) = single_id(node) {
        keys.push(PathResolutionKey::id(entity, id));
    }
    if let Some(value) = single_full_path(node) {
        keys.push(PathResolutionKey::full_path(entity, value));
    }
    // A node filtered by an anchor FK (e.g. `project_id = N`) lives under that
    // anchor's traversal_path even though the node itself is not the anchor, so
    // the resolvable scope is the anchor entity's path. The `(fk, anchor)` pairs
    // come from the ontology's `namespace_anchor` edge annotations
    // (`Ontology::anchor_fk_mappings`), not a hardcoded list.
    for (column, anchor) in anchor_fks {
        if let Some(id) = single_eq_id(node, column) {
            keys.push(PathResolutionKey::id(*anchor, id));
        }
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

fn single_eq_id(node: &InputNode, column: &str) -> Option<i64> {
    eq_value(node.filters.get(column)?)?.as_i64()
}

fn entity_of<'a>(input: &'a Input, alias: &str) -> &'a str {
    input
        .nodes
        .iter()
        .find(|n| n.id == alias)
        .and_then(|n| n.entity.as_deref())
        .unwrap_or("")
}

/// Build the [`ScopeEdge`] view of a query's relationships for
/// [`ontology::Ontology::propagate_scope_prefixes`]. Each relationship becomes
/// one edge carrying its endpoint aliases, relationship kinds, and the endpoint
/// entity kinds the ontology needs to select the exact scope-preserving variant
/// (so mixed-variant edges like `CONTAINS` resolve correctly).
pub fn scope_edges(input: &Input) -> Vec<ScopeEdge<'_>> {
    input
        .relationships
        .iter()
        .map(|r| ScopeEdge {
            from: &r.from,
            to: &r.to,
            types: &r.types,
            source_kind: entity_of(input, &r.from),
            target_kind: entity_of(input, &r.to),
        })
        .collect()
}

fn eq_value(filters: &[InputFilter]) -> Option<&serde_json::Value> {
    let [filter] = filters else { return None };
    match filter.op {
        None | Some(FilterOp::Eq) => filter.value.as_ref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ANCHOR_FKS: &[(&str, &str)] = &[("project_id", "Project"), ("group_id", "Group")];

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
            scope_keys(&project_node("p"), ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 42)]
        );
    }

    #[test]
    fn multi_id_yields_one_key_per_id_for_lcp_fold() {
        let mut node = project_node("p");
        node.node_ids = vec![1, 2, 3];
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![
                PathResolutionKey::id("Project", 1),
                PathResolutionKey::id("Project", 2),
                PathResolutionKey::id("Project", 3),
            ]
        );
    }

    #[test]
    fn bare_full_path_shorthand_yields_full_path_key() {
        let mut node = InputNode {
            id: "p".to_string(),
            entity: Some("Project".to_string()),
            has_traversal_path: true,
            ..Default::default()
        };
        node.filters.insert(
            "full_path".to_string(),
            vec![InputFilter {
                op: None,
                value: Some(json!("group/project")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::full_path("Project", "group/project")]
        );
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
            scope_keys(&node, ANCHOR_FKS),
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
        assert!(scope_keys(&node, ANCHOR_FKS).is_empty());
    }

    fn node_with_filter(entity: &str, column: &str, value: serde_json::Value) -> InputNode {
        let mut node = InputNode {
            id: "n".to_string(),
            entity: Some(entity.to_string()),
            ..Default::default()
        };
        node.filters.insert(
            column.to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(value),
                ..Default::default()
            }],
        );
        node
    }

    #[test]
    fn project_id_filter_resolves_to_project_anchor() {
        let node = node_with_filter("MergeRequest", "project_id", json!(278964));
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 278964)]
        );
    }

    #[test]
    fn group_id_filter_resolves_to_group_anchor() {
        let node = node_with_filter("MergeRequest", "group_id", json!(9970));
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Group", 9970)]
        );
    }

    // The customer-zero query pins MergeRequest by project_id alongside state and
    // merged_at filters; the extra predicates must not suppress the anchor key.
    #[test]
    fn project_id_anchor_survives_sibling_filters() {
        let mut node = node_with_filter("MergeRequest", "project_id", json!(278964));
        node.filters.insert(
            "state".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(json!("merged")),
                ..Default::default()
            }],
        );
        node.filters.insert(
            "merged_at".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Gte),
                value: Some(json!("2026-03-05T00:00:00Z")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 278964)]
        );
    }

    #[test]
    fn project_id_in_list_yields_no_anchor() {
        let mut node = InputNode {
            id: "n".to_string(),
            entity: Some("MergeRequest".to_string()),
            ..Default::default()
        };
        node.filters.insert(
            "project_id".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::In),
                value: Some(json!([1, 2, 3])),
                ..Default::default()
            }],
        );
        assert!(scope_keys(&node, ANCHOR_FKS).is_empty());
    }

    // scope_edges resolves each relationship's endpoint entity kinds from the
    // query's nodes so the ontology can pick the exact scope-preserving variant.
    #[test]
    fn scope_edges_carries_endpoint_entity_kinds() {
        let input = crate::parse_input(
            r#"{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"diff","entity":"MergeRequestDiff"}],"relationships":[{"type":"HAS_DIFF","from":"mr","to":"diff"}],"limit":1}"#,
        )
        .unwrap();
        let edges = scope_edges(&input);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].from, "mr");
        assert_eq!(edges[0].to, "diff");
        assert_eq!(edges[0].source_kind, "MergeRequest");
        assert_eq!(edges[0].target_kind, "MergeRequestDiff");
    }
}
