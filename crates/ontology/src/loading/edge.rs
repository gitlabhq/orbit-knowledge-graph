use serde::Deserialize;
use std::collections::BTreeMap;

use crate::OntologyError;
use crate::entities::{
    EdgeEndpoint, EdgeEndpointType, EdgeEntity, EdgeSourceEtlConfig, EdgeVariantScope,
};
use crate::etl::{EdgeDirection, EdgeMapping, EdgeTarget, EtlScope};

use super::EtlSettings;

#[derive(Debug, Deserialize)]
pub(crate) struct EdgeYaml {
    #[serde(default)]
    pub description: Option<String>,
    /// Optional override for the ClickHouse table storing this edge type.
    /// Defaults to the global `edge_table` from settings.
    #[serde(default)]
    pub table: Option<String>,
    /// Default traversal scope applied to every `edges:` entry that does not
    /// set its own. Variant-form (`variants:`) edges carry scope per entry.
    #[serde(default)]
    scope: Option<EdgeVariantScope>,
    /// Unified graph-native form: each entry is one relationship declared once
    /// in graph terms. The query entity and the ETL node binding both derive
    /// from it — the siphon extract column is resolved through the owning
    /// node's property map, never written here.
    #[serde(default)]
    edges: Vec<UnifiedEdgeYaml>,
    #[serde(default)]
    variants: Vec<EdgeVariantYaml>,
    /// Every producer of this relationship: join tables, node rows, transforms.
    #[serde(default)]
    sources: Vec<EdgeSourceYaml>,
}

/// One relationship in the unified `edges:` form. Exactly one endpoint is the
/// foreign-key owner (its `key` is a graph column other than `id`); the other
/// anchors on `id`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnifiedEdgeYaml {
    from: UnifiedEndpoint,
    to: UnifiedEndpoint,
    /// Overrides the file-level `scope:` for this entry.
    #[serde(default)]
    scope: Option<EdgeVariantScope>,
    /// Opts the edge into the stale-edge reconciliation sweep.
    #[serde(default)]
    mutable: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnifiedEndpoint {
    node: String,
    /// Graph column carrying this endpoint's id. `id` for the anchor; the FK
    /// graph column for the owner.
    key: String,
}

/// The foreign-key owner of a unified entry (the endpoint whose `key` is not
/// `id`), the anchor it points at, and the binding direction relative to the
/// owner's own row.
struct UnifiedSplit<'a> {
    owner: &'a UnifiedEndpoint,
    anchor: &'a UnifiedEndpoint,
    direction: EdgeDirection,
}

fn split_unified<'a>(
    relationship_kind: &str,
    entry: &'a UnifiedEdgeYaml,
) -> Result<UnifiedSplit<'a>, OntologyError> {
    let from_anchor = entry.from.key == "id";
    let to_anchor = entry.to.key == "id";
    match (from_anchor, to_anchor) {
        (false, true) => Ok(UnifiedSplit {
            owner: &entry.from,
            anchor: &entry.to,
            direction: EdgeDirection::Outgoing,
        }),
        (true, false) => Ok(UnifiedSplit {
            owner: &entry.to,
            anchor: &entry.from,
            direction: EdgeDirection::Incoming,
        }),
        (true, true) => Err(OntologyError::Validation(format!(
            "edge '{relationship_kind}': both endpoints use key 'id'; one must \
             name the foreign-key graph column"
        ))),
        (false, false) => Err(OntologyError::Validation(format!(
            "edge '{relationship_kind}': both endpoints carry a non-'id' key; \
             exactly one endpoint is the foreign-key owner"
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct EdgeVariantYaml {
    from_node: EdgeNodeRef,
    to_node: EdgeNodeRef,
    #[serde(default)]
    fk_column: Option<String>,
    #[serde(default)]
    scope: Option<EdgeVariantScope>,
}

#[derive(Debug, Deserialize)]
struct EdgeNodeRef {
    #[serde(rename = "type")]
    node_type: String,
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EdgeSourceYaml {
    /// Rows of a join table, one edge per row.
    Table {
        table: String,
        scope: EtlScope,
        order_by: Vec<String>,
        from: EndpointYaml,
        to: EndpointYaml,
    },
    /// Rows (or array elements of rows) of a node's extraction output.
    Node {
        node: String,
        from: EndpointYaml,
        to: EndpointYaml,
        /// Array column to unnest; endpoint ids then resolve per element.
        #[serde(default)]
        unnest: Option<String>,
        #[serde(default)]
        mutable: bool,
    },
    /// Emitted by a named Rust transform on a derived entity.
    Transform { transform: String },
}

#[derive(Debug, Deserialize)]
struct EndpointYaml {
    id: String,
    #[serde(rename = "type")]
    type_literal: Option<String>,
    #[serde(rename = "type_column")]
    type_column: Option<String>,
    #[serde(default)]
    type_mapping: BTreeMap<String, String>,
    /// Columns to enrich from this endpoint's node datalake table.
    /// Only valid on table sources.
    #[serde(default)]
    enrich: Vec<String>,
}

/// A node-sourced binding the loader attaches to the producing node's ETL.
#[derive(Debug)]
pub(crate) struct NodeEdgeBinding {
    pub node: String,
    pub column: String,
    pub mapping: EdgeMapping,
}

/// All producers declared by one relationship file.
#[derive(Debug, Default)]
pub(crate) struct EdgeSources {
    pub table_etls: Vec<EdgeSourceEtlConfig>,
    pub node_bindings: Vec<NodeEdgeBinding>,
    pub transforms: Vec<String>,
}

impl EdgeYaml {
    pub(crate) fn to_entities(
        &self,
        relationship_kind: String,
        default_table: &str,
    ) -> Result<Vec<EdgeEntity>, OntologyError> {
        let table = self.table.as_deref().unwrap_or(default_table).to_string();
        let mut entities: Vec<EdgeEntity> = self
            .variants
            .iter()
            .map(|v| EdgeEntity {
                relationship_kind: relationship_kind.clone(),
                source: v.from_node.id.clone(),
                source_kind: v.from_node.node_type.clone(),
                target: v.to_node.id.clone(),
                target_kind: v.to_node.node_type.clone(),
                destination_table: table.clone(),
                fk_column: v.fk_column.clone(),
                scope: v.scope,
            })
            .collect();

        for entry in &self.edges {
            let split = split_unified(&relationship_kind, entry)?;
            entities.push(EdgeEntity {
                relationship_kind: relationship_kind.clone(),
                source: "id".to_string(),
                source_kind: entry.from.node.clone(),
                target: "id".to_string(),
                target_kind: entry.to.node.clone(),
                destination_table: table.clone(),
                // The graph FK column (owner's key); the compiler joins node
                // tables on it directly instead of scanning the edge table.
                fk_column: Some(split.owner.key.clone()),
                scope: entry.scope.or(self.scope),
            });
        }
        Ok(entities)
    }

    /// `resolve_siphon(node, graph_key)` returns the datalake column that the
    /// node's extract emits for the graph column `graph_key` (the node's
    /// property `source`). This is what keeps the siphon name out of the edge
    /// file — it lives only on the owning node's property.
    pub(crate) fn into_sources(
        self,
        relationship_kind: &str,
        etl_settings: &EtlSettings,
        resolve_siphon: &dyn Fn(&str, &str) -> Result<String, OntologyError>,
    ) -> Result<EdgeSources, OntologyError> {
        let mut sources = EdgeSources::default();

        for entry in &self.edges {
            let split = split_unified(relationship_kind, entry)?;
            let column = resolve_siphon(&split.owner.node, &split.owner.key)?;
            sources.node_bindings.push(NodeEdgeBinding {
                node: split.owner.node.clone(),
                column,
                mapping: EdgeMapping {
                    target: EdgeTarget::Literal(split.anchor.node.clone()),
                    relationship_kind: relationship_kind.to_string(),
                    direction: split.direction,
                    delimiter: None,
                    array_field: None,
                    array: false,
                    mutable: entry.mutable,
                },
            });
        }

        for source in self.sources {
            match source {
                EdgeSourceYaml::Table {
                    table,
                    scope,
                    order_by,
                    from,
                    to,
                } => {
                    let from = convert_endpoint(relationship_kind, from, "from")?;
                    let to = convert_endpoint(relationship_kind, to, "to")?;
                    sources.table_etls.push(EdgeSourceEtlConfig {
                        scope,
                        source: table,
                        watermark: etl_settings.watermark.clone(),
                        deleted: etl_settings.deleted.clone(),
                        order_by,
                        from,
                        to,
                    });
                }
                EdgeSourceYaml::Node {
                    node,
                    from,
                    to,
                    unnest,
                    mutable,
                } => {
                    sources.node_bindings.push(convert_node_binding(
                        relationship_kind,
                        node,
                        from,
                        to,
                        unnest,
                        mutable,
                    )?);
                }
                EdgeSourceYaml::Transform { transform } => {
                    sources.transforms.push(transform);
                }
            }
        }
        Ok(sources)
    }
}

/// Exactly one endpoint must be the node's own row (`{type: <node>, id: id}`);
/// the other carries the FK, which fixes the internal direction.
fn convert_node_binding(
    relationship_kind: &str,
    node: String,
    from: EndpointYaml,
    to: EndpointYaml,
    unnest: Option<String>,
    mutable: bool,
) -> Result<NodeEdgeBinding, OntologyError> {
    let is_self =
        |ep: &EndpointYaml| ep.type_literal.as_deref() == Some(node.as_str()) && ep.id == "id";

    let (self_ep, fk_ep, direction) = match (is_self(&from), is_self(&to)) {
        (true, false) => (from, to, EdgeDirection::Outgoing),
        (false, true) => (to, from, EdgeDirection::Incoming),
        (true, true) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' node source on '{node}': both endpoints are the \
                 node's own row; one must reference another column"
            )));
        }
        (false, false) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' node source on '{node}': one endpoint must be the \
                 node's own row, written as {{type: {node}, id: id}}"
            )));
        }
    };

    if self_ep.type_column.is_some() || !self_ep.type_mapping.is_empty() {
        return Err(OntologyError::Validation(format!(
            "edge '{relationship_kind}' node source on '{node}': the node's own endpoint \
             cannot be polymorphic"
        )));
    }
    for ep in [&self_ep, &fk_ep] {
        if !ep.enrich.is_empty() {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' node source on '{node}': 'enrich' is only valid \
                 on table sources (node properties denormalize via settings.denormalization)"
            )));
        }
    }

    let target = match (fk_ep.type_literal, fk_ep.type_column) {
        (Some(lit), None) => {
            if !fk_ep.type_mapping.is_empty() {
                return Err(OntologyError::Validation(format!(
                    "edge '{relationship_kind}' node source on '{node}': 'type_mapping' \
                     requires 'type_column'"
                )));
            }
            EdgeTarget::Literal(lit)
        }
        (None, Some(col)) => EdgeTarget::Column {
            column: col,
            type_mapping: fk_ep.type_mapping,
        },
        (Some(_), Some(_)) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' node source on '{node}': use 'type' or \
                 'type_column', not both"
            )));
        }
        (None, None) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' node source on '{node}': endpoint requires \
                 'type' or 'type_column'"
            )));
        }
    };

    let (column, array_field) = match unnest {
        Some(array_column) => (array_column, Some(fk_ep.id)),
        None => (fk_ep.id, None),
    };

    Ok(NodeEdgeBinding {
        node,
        column,
        mapping: EdgeMapping {
            target,
            relationship_kind: relationship_kind.to_string(),
            direction,
            delimiter: None,
            array_field,
            array: false,
            mutable,
        },
    })
}

fn convert_endpoint(
    relationship_kind: &str,
    ep: EndpointYaml,
    endpoint_name: &str,
) -> Result<EdgeEndpoint, OntologyError> {
    let node_type = match (ep.type_literal, ep.type_column) {
        (Some(lit), None) => EdgeEndpointType::Literal(lit),
        (None, Some(col)) => EdgeEndpointType::Column {
            column: col,
            type_mapping: ep.type_mapping,
        },
        (Some(_), Some(_)) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' table source endpoint '{endpoint_name}': \
                 use 'type' or 'type_column', not both"
            )));
        }
        (None, None) => {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' table source endpoint '{endpoint_name}': \
                 requires 'type' or 'type_column'"
            )));
        }
    };

    Ok(EdgeEndpoint {
        id_column: ep.id,
        node_type,
        enrich: ep.enrich,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_resolve() -> impl Fn(&str, &str) -> Result<String, OntologyError> {
        |_node: &str, key: &str| Ok(key.to_string())
    }

    #[test]
    fn unified_edge_derives_entity_with_graph_fk_and_scope() {
        let edge = parse(
            r#"
            scope: prune_to_target
            edges:
              - from: {node: User, key: id}
                to:   {node: MergeRequest, key: closed_by_id}
            "#,
        );
        let entities = edge.to_entities("CLOSED".into(), "gl_edge").unwrap();
        assert_eq!(entities.len(), 1);
        let e = &entities[0];
        assert_eq!(e.source_kind, "User");
        assert_eq!(e.target_kind, "MergeRequest");
        assert_eq!(e.fk_column.as_deref(), Some("closed_by_id"));
        assert_eq!(e.scope, Some(EdgeVariantScope::PruneToTarget));
    }

    #[test]
    fn unified_edge_binding_resolves_siphon_column_via_node() {
        let edge = parse(
            r#"
            edges:
              - from: {node: User, key: id}
                to:   {node: MergeRequest, key: closed_by_id}
            "#,
        );
        // MergeRequest.closed_by_id maps to the siphon column below.
        let resolve = |node: &str, key: &str| {
            assert_eq!((node, key), ("MergeRequest", "closed_by_id"));
            Ok("metric_latest_closed_by_id".to_string())
        };
        let sources = edge.into_sources("CLOSED", &settings(), &resolve).unwrap();
        assert_eq!(sources.node_bindings.len(), 1);
        let b = &sources.node_bindings[0];
        assert_eq!(b.node, "MergeRequest");
        assert_eq!(b.column, "metric_latest_closed_by_id");
        assert_eq!(b.mapping.direction, EdgeDirection::Incoming);
        assert_eq!(b.mapping.target, EdgeTarget::Literal("User".to_string()));
    }

    #[test]
    fn unified_edge_outgoing_when_owner_is_source() {
        let edge = parse(
            r#"
            edges:
              - from: {node: Note, key: project_id}
                to:   {node: Project, key: id}
            "#,
        );
        let sources = edge
            .into_sources("IN_PROJECT", &settings(), &id_resolve())
            .unwrap();
        let b = &sources.node_bindings[0];
        assert_eq!(b.node, "Note");
        assert_eq!(b.column, "project_id");
        assert_eq!(b.mapping.direction, EdgeDirection::Outgoing);
        assert_eq!(b.mapping.target, EdgeTarget::Literal("Project".to_string()));
    }

    #[test]
    fn unified_edge_rejects_two_fk_keys() {
        let edge = parse(
            r#"
            edges:
              - from: {node: Note, key: project_id}
                to:   {node: Project, key: namespace_id}
            "#,
        );
        let err = edge
            .to_entities("X".into(), "gl_edge")
            .expect_err("two non-id keys should fail");
        assert!(err.to_string().contains("foreign-key owner"), "got: {err}");
    }

    fn settings() -> EtlSettings {
        EtlSettings {
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        }
    }

    fn parse(yaml: &str) -> EdgeYaml {
        serde_yaml::from_str(yaml).expect("edge yaml should parse")
    }

    #[test]
    fn node_source_outgoing_maps_fk_to_target() {
        let edge = parse(
            r#"
            sources:
              - node: Pipeline
                from: {type: Pipeline, id: id}
                to: {type: Project, id: project_id}
            "#,
        );
        let sources = edge
            .into_sources("IN_PROJECT", &settings(), &id_resolve())
            .unwrap();
        let binding = &sources.node_bindings[0];
        assert_eq!(binding.node, "Pipeline");
        assert_eq!(binding.column, "project_id");
        assert_eq!(binding.mapping.direction, EdgeDirection::Outgoing);
        assert_eq!(
            binding.mapping.target,
            EdgeTarget::Literal("Project".to_string())
        );
    }

    #[test]
    fn node_source_incoming_maps_fk_to_source() {
        let edge = parse(
            r#"
            sources:
              - node: Pipeline
                from: {type: User, id: user_id}
                to: {type: Pipeline, id: id}
            "#,
        );
        let sources = edge
            .into_sources("TRIGGERED", &settings(), &id_resolve())
            .unwrap();
        let binding = &sources.node_bindings[0];
        assert_eq!(binding.column, "user_id");
        assert_eq!(binding.mapping.direction, EdgeDirection::Incoming);
    }

    #[test]
    fn node_source_self_edge_resolves_by_id_column() {
        let edge = parse(
            r#"
            sources:
              - node: Pipeline
                from: {type: Pipeline, id: id}
                to: {type: Pipeline, id: auto_canceled_by_id}
            "#,
        );
        let sources = edge
            .into_sources("AUTO_CANCELED_BY", &settings(), &id_resolve())
            .unwrap();
        let binding = &sources.node_bindings[0];
        assert_eq!(binding.column, "auto_canceled_by_id");
        assert_eq!(binding.mapping.direction, EdgeDirection::Outgoing);
    }

    #[test]
    fn node_source_unnest_sets_array_field() {
        let edge = parse(
            r#"
            sources:
              - node: MergeRequest
                from: {type: User, id: user_id}
                to: {type: MergeRequest, id: id}
                unnest: reviewers
            "#,
        );
        let sources = edge
            .into_sources("REVIEWER", &settings(), &id_resolve())
            .unwrap();
        let binding = &sources.node_bindings[0];
        assert_eq!(binding.column, "reviewers");
        assert_eq!(binding.mapping.array_field.as_deref(), Some("user_id"));
        assert_eq!(binding.mapping.direction, EdgeDirection::Incoming);
    }

    #[test]
    fn node_source_polymorphic_fk_endpoint() {
        let edge = parse(
            r#"
            sources:
              - node: Note
                from:
                  type_column: noteable_type
                  type_mapping:
                    Issue: WorkItem
                  id: noteable_id
                to: {type: Note, id: id}
            "#,
        );
        let sources = edge
            .into_sources("HAS_NOTE", &settings(), &id_resolve())
            .unwrap();
        let binding = &sources.node_bindings[0];
        assert_eq!(binding.column, "noteable_id");
        match &binding.mapping.target {
            EdgeTarget::Column {
                column,
                type_mapping,
            } => {
                assert_eq!(column, "noteable_type");
                assert_eq!(type_mapping.get("Issue").unwrap(), "WorkItem");
            }
            other => panic!("expected polymorphic target, got {other:?}"),
        }
    }

    #[test]
    fn node_source_without_self_endpoint_is_rejected() {
        let edge = parse(
            r#"
            sources:
              - node: Pipeline
                from: {type: User, id: user_id}
                to: {type: Project, id: project_id}
            "#,
        );
        let err = edge
            .into_sources("IN_PROJECT", &settings(), &id_resolve())
            .expect_err("missing self endpoint should fail");
        assert!(err.to_string().contains("node's own row"), "got: {err}");
    }

    #[test]
    fn node_source_rejects_enrich() {
        let edge = parse(
            r#"
            sources:
              - node: Pipeline
                from: {type: Pipeline, id: id}
                to: {type: Project, id: project_id, enrich: [visibility_level]}
            "#,
        );
        let err = edge
            .into_sources("IN_PROJECT", &settings(), &id_resolve())
            .expect_err("enrich on node source should fail");
        assert!(err.to_string().contains("table sources"), "got: {err}");
    }

    #[test]
    fn table_source_converts_to_edge_etl_config() {
        let edge = parse(
            r#"
            sources:
              - table: siphon_members
                scope: namespaced
                order_by: [traversal_path, id]
                from: {type: User, id: user_id, enrich: [state]}
                to:
                  id: source_id
                  type_column: source_type
                  type_mapping:
                    Namespace: Group
            "#,
        );
        let sources = edge
            .into_sources("MEMBER_OF", &settings(), &id_resolve())
            .unwrap();
        assert_eq!(sources.table_etls.len(), 1);
        let etl = &sources.table_etls[0];
        assert_eq!(etl.source, "siphon_members");
        assert_eq!(etl.watermark, "_siphon_watermark");
        assert_eq!(etl.from.enrich, vec!["state"]);
    }

    #[test]
    fn transform_source_is_collected() {
        let edge = parse(
            r#"
            sources:
              - transform: system_notes
            "#,
        );
        let sources = edge
            .into_sources("MENTIONS", &settings(), &id_resolve())
            .unwrap();
        assert_eq!(sources.transforms, vec!["system_notes"]);
    }

    #[test]
    fn mixed_sources_split_into_kinds() {
        let edge = parse(
            r#"
            sources:
              - table: siphon_merge_request_reviewers
                scope: namespaced
                order_by: [traversal_path, merge_request_id, id]
                from: {type: User, id: user_id}
                to: {type: MergeRequest, id: merge_request_id}
              - node: MergeRequest
                from: {type: User, id: user_id}
                to: {type: MergeRequest, id: id}
                unnest: reviewers
            "#,
        );
        let sources = edge
            .into_sources("REVIEWER", &settings(), &id_resolve())
            .unwrap();
        assert_eq!(sources.table_etls.len(), 1);
        assert_eq!(sources.node_bindings.len(), 1);
        assert!(sources.transforms.is_empty());
    }
}
