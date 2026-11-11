//! The schema description served to agents: the server's `get_graph_schema`, the GQL
//! `CALL db.schema()` route, and the relationship patterns in tool descriptions.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::catalog::Store;
use crate::{Catalog, DuckDb, Entity, EntityId, Model, Property, RelationshipVariant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntrospectionScope {
    All,
    /// Entities stored in the local DuckDB graph.
    Local,
}

impl IntrospectionScope {
    #[must_use]
    pub fn includes(self, model: &Model, name: &str) -> bool {
        model
            .graph()
            .entity_id(name)
            .is_some_and(|entity| self.holds(model, entity))
    }

    fn holds(self, model: &Model, entity: EntityId) -> bool {
        self == Self::All || local_store(model, entity).is_some()
    }

    fn holds_variant(self, model: &Model, variant: &RelationshipVariant) -> bool {
        self.holds(model, variant.source) && self.holds(model, variant.target)
    }

    /// The properties a node lists: the visible ones, or the local columns.
    fn properties<'m>(self, model: &'m Model, entity: &Entity) -> Vec<&'m Property> {
        let local = local_store(model, entity.id);
        entity
            .properties
            .iter()
            .filter(|property| match (self, local) {
                (Self::All, _) => !model.access().property(**property).hidden,
                (Self::Local, Some(store)) => {
                    !store.excludes(**property) && model.lookup(**property).is_none()
                }
                (Self::Local, None) => false,
            })
            .map(|property| model.graph().property(*property))
            .collect()
    }
}

fn local_store(model: &Model, entity: EntityId) -> Option<&Store> {
    model.stores().iter().find(|store| {
        store.connection == Catalog::<DuckDb>::CONNECTION && store.source(entity).is_some()
    })
}

#[derive(Debug, thiserror::Error)]
#[error("unknown node(s): {}. Valid nodes: {}", .unknown.join(", "), .valid.join(", "))]
pub struct UnknownNodes {
    pub unknown: Vec<String>,
    pub valid: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub domains: Vec<SchemaDomain>,
    pub edges: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SchemaDomain {
    pub name: String,
    pub nodes: Vec<SchemaNode>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SchemaNode {
    Name(String),
    Listed {
        name: String,
        props: String,
    },
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
    },
}

/// `expand_nodes`: pass `["*"]` to expand every node, or specific names.
pub fn build_schema_response(
    model: &Model,
    scope: IntrospectionScope,
    expand_nodes: &[String],
) -> Result<SchemaResponse, UnknownNodes> {
    let unknown: Vec<String> = expand_nodes
        .iter()
        .filter(|name| *name != "*" && !scope.includes(model, name))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        let valid = entities(model, scope)
            .map(|entity| entity.name.clone())
            .collect();
        return Err(UnknownNodes { unknown, valid });
    }
    Ok(SchemaResponse {
        domains: build_domains(model, scope, expand_nodes, None, format_property),
        edges: relationship_names(model, scope),
    })
}

#[must_use]
pub fn build_compact_schema_response(
    model: &Model,
    scope: IntrospectionScope,
    node: Option<&str>,
) -> SchemaResponse {
    match node {
        None => SchemaResponse {
            domains: build_overview_domains(model, scope),
            edges: build_relationship_patterns(model, scope),
        },
        Some(node) => SchemaResponse {
            domains: build_domains(model, scope, &[], Some(node), format_compact_property),
            edges: Vec::new(),
        },
    }
}

/// One `(Source)-[:TYPE]->(Target|Target)` line per source node and relationship.
#[must_use]
pub fn build_relationship_patterns(model: &Model, scope: IntrospectionScope) -> Vec<String> {
    let graph = model.graph();
    let mut targets: BTreeMap<(&str, &str), Vec<&str>> = BTreeMap::new();
    for variant in graph.variants() {
        if !scope.holds_variant(model, variant) {
            continue;
        }
        targets
            .entry((
                graph.entity(variant.source).name.as_str(),
                graph.relationship(variant.relationship).name.as_str(),
            ))
            .or_default()
            .push(graph.entity(variant.target).name.as_str());
    }
    targets
        .into_iter()
        .map(|((source, relationship), mut kinds)| {
            kinds.sort_unstable();
            kinds.dedup();
            format!("({source})-[:{relationship}]->({})", kinds.join("|"))
        })
        .collect()
}

fn entities(model: &Model, scope: IntrospectionScope) -> impl Iterator<Item = &Entity> {
    model
        .graph()
        .entities()
        .filter(move |entity| scope.holds(model, entity.id))
}

fn domain_name(entity: &Entity) -> String {
    match entity.domain.as_deref() {
        None | Some("") => "other".to_string(),
        Some(domain) => domain.to_string(),
    }
}

fn build_overview_domains(model: &Model, scope: IntrospectionScope) -> Vec<SchemaDomain> {
    let mut domains = build_domains(model, scope, &[], None, format_compact_property);
    let overview: Vec<&str> = model
        .graph()
        .domains()
        .filter(|domain| domain.overview)
        .map(|domain| domain.name.as_str())
        .collect();
    for domain in &mut domains {
        if !overview.contains(&domain.name.as_str()) {
            continue;
        }
        for node in &mut domain.nodes {
            let SchemaNode::Name(name) = node else {
                continue;
            };
            let Some(entity) = model.graph().entity_named(name) else {
                continue;
            };
            let props: Vec<String> = scope
                .properties(model, entity)
                .into_iter()
                .map(format_listed_property)
                .collect();
            *node = SchemaNode::Listed {
                name: name.clone(),
                props: props.join(", "),
            };
        }
    }
    domains
}

fn build_domains(
    model: &Model,
    scope: IntrospectionScope,
    expand_nodes: &[String],
    only_node: Option<&str>,
    format: fn(&Property) -> String,
) -> Vec<SchemaDomain> {
    let mut domains: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();
    for entity in entities(model, scope) {
        if only_node.is_some_and(|name| name != entity.name) {
            continue;
        }
        let expand = only_node.is_some()
            || expand_nodes
                .iter()
                .any(|name| name == "*" || *name == entity.name);
        let node = match expand {
            true => {
                let (out, r#in) = node_relationships(model, scope, entity.id);
                SchemaNode::Expanded {
                    name: entity.name.clone(),
                    props: scope
                        .properties(model, entity)
                        .into_iter()
                        .map(format)
                        .collect(),
                    out,
                    r#in,
                }
            }
            false => SchemaNode::Name(entity.name.clone()),
        };
        domains.entry(domain_name(entity)).or_default().push(node);
    }
    domains
        .into_iter()
        .map(|(name, nodes)| SchemaDomain { name, nodes })
        .collect()
}

/// Relationship names in name order, with at least one variant in scope.
fn relationship_names(model: &Model, scope: IntrospectionScope) -> Vec<String> {
    let graph = model.graph();
    let mut names: Vec<String> = graph
        .relationships()
        .filter(|relationship| {
            relationship
                .variants
                .iter()
                .any(|variant| scope.holds_variant(model, graph.variant(*variant)))
        })
        .map(|relationship| relationship.name.clone())
        .collect();
    names.sort();
    names
}

fn node_relationships(
    model: &Model,
    scope: IntrospectionScope,
    entity: EntityId,
) -> (Vec<String>, Vec<String>) {
    let graph = model.graph();
    let mut outgoing = Vec::new();
    let mut incoming = Vec::new();
    for relationship in graph.relationships() {
        let variants: Vec<&RelationshipVariant> = relationship
            .variants
            .iter()
            .map(|variant| graph.variant(*variant))
            .filter(|variant| scope.holds_variant(model, variant))
            .collect();
        let ends = |end: fn(&RelationshipVariant) -> (EntityId, EntityId)| {
            let mut names: Vec<&str> = variants
                .iter()
                .map(|variant| end(variant))
                .filter(|(near, _)| *near == entity)
                .map(|(_, far)| graph.entity(far).name.as_str())
                .collect();
            names.sort_unstable();
            names.dedup();
            names
        };
        let targets = ends(|variant| (variant.source, variant.target));
        let sources = ends(|variant| (variant.target, variant.source));
        if !targets.is_empty() {
            outgoing.push(format!("{} → [{}]", relationship.name, targets.join(", ")));
        }
        if !sources.is_empty() {
            incoming.push(format!("{} ← [{}]", relationship.name, sources.join(", ")));
        }
    }
    outgoing.sort();
    incoming.sort();
    (outgoing, incoming)
}

fn enum_values(property: &Property) -> Option<String> {
    let values = property.enum_values.as_ref()?;
    Some(
        values
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("|"),
    )
}

fn type_text(property: &Property) -> String {
    let nullable = if property.nullable { "?" } else { "" };
    format!(
        "{}:{}{nullable}",
        property.name,
        property.data_type.to_string().to_lowercase()
    )
}

fn format_listed_property(property: &Property) -> String {
    match enum_values(property) {
        Some(values) => format!("{} ({values})", property.name),
        None => property.name.clone(),
    }
}

fn format_compact_property(property: &Property) -> String {
    match enum_values(property) {
        Some(values) => format!("{} ({values})", type_text(property)),
        None => type_text(property),
    }
}

fn format_property(property: &Property) -> String {
    match &property.description {
        Some(description) => format!("{} — {description}", type_text(property)),
        None => type_text(property),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRAPH: &str = "
kind: graph
package: test
domain: core
domain_overview: true
entities:
  Project:
    label: name
    properties:
      id: {type: int64, description: Unique ID.}
      name: string
      traversal_path: string
  MergeRequest:
    label: title
    properties:
      id: {type: int64, description: Unique ID.}
      state: {type: enum, values: {1: opened, 2: merged}}
      title: string?
      body: string
      project_id: int64
      traversal_path: string
relationships:
  IN_PROJECT: {variants: {MergeRequest -> Project: {cardinality: many_to_one}}}
  RELATES_TO:
    variants:
      MergeRequest -> MergeRequest: {cardinality: many_to_many}
      MergeRequest -> Project: {cardinality: many_to_many}
";

    const AUTHORIZATION: &str = "
kind: authorization
package: test
entities:
  Project: {scope: namespace, role: reporter, redaction: {resource: project, ability: read_project}}
  MergeRequest: {scope: namespace, role: reporter, redaction: {resource: merge_request, ability: read_merge_request}}
relationships:
  IN_PROJECT: namespace_anchor
  RELATES_TO: same_namespace
properties:
  Project.traversal_path: {hidden: true, filterable: false}
  MergeRequest.traversal_path: {hidden: true, filterable: false}
";

    const GRAPH_STORE: &str = "
kind: storage
package: test
store: graph
engine:
  kind: clickhouse
  connection: main
  path_column: traversal_path
  backend:
    version_column: _version
    deleted_column: _deleted
    edge_tables:
      gl_edge: {shared: true, sort_key: [traversal_path, source_id, relationship_kind, target_id]}
entities:
  Project:
    table: gl_project
    columns:
      id: {immutable: true, backend: {type: Int64}}
      name: {backend: {type: String}}
      traversal_path: {backend: {type: String}}
    access: {order: [id]}
  MergeRequest:
    table: gl_merge_request
    columns:
      id: {immutable: true, backend: {type: Int64}}
      state: {backend: {type: String}}
      title: {backend: {type: Nullable(String)}}
      body: {backend: {type: String}}
      project_id: {backend: {type: Int64}}
      traversal_path: {backend: {type: String}}
    access: {order: [id]}
relationships:
  IN_PROJECT:
    MergeRequest -> Project: {fk: {on: source, column: project_id}, edge_table: gl_edge}
  RELATES_TO:
    MergeRequest -> MergeRequest: {edge_table: gl_edge}
    MergeRequest -> Project: {edge_table: gl_edge}
";

    const LOCAL_STORE: &str = "
kind: storage
package: test
store: local
engine:
  kind: duckdb
  connection: local
  trusted: true
  path_column: traversal_path
  backend:
    edge_tables:
      gl_edge:
        shared: true
        sort_key: [traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind]
        columns:
          source_id: int64
          source_kind: string
          relationship_kind: string
          target_id: int64
          target_kind: string
          traversal_path: string
entities:
  MergeRequest:
    table: gl_merge_request
    columns:
      id: {immutable: true, backend: {type: BIGINT}}
      state: {backend: {type: VARCHAR}}
      title: {backend: {type: VARCHAR}}
      project_id: {backend: {type: BIGINT}}
      traversal_path: {backend: {type: VARCHAR}}
    exclude: [body]
    access: {order: [id]}
relationships:
  RELATES_TO:
    MergeRequest -> MergeRequest: {edge_table: gl_edge}
";

    fn model() -> Model {
        Model::from_yaml([
            ("graph.yaml", GRAPH),
            ("authorization.yaml", AUTHORIZATION),
            ("storage/clickhouse.yaml", GRAPH_STORE),
            ("storage/duckdb.yaml", LOCAL_STORE),
        ])
        .unwrap()
    }

    fn json(response: &SchemaResponse) -> serde_json::Value {
        serde_json::to_value(response).unwrap()
    }

    #[test]
    fn full_form_expands_named_nodes_with_descriptions() {
        let response =
            build_schema_response(&model(), IntrospectionScope::All, &["MergeRequest".into()])
                .unwrap();
        assert_eq!(
            json(&response),
            serde_json::json!({
                "domains": [{"name": "core", "nodes": [
                    {
                        "name": "MergeRequest",
                        "props": ["id:int — Unique ID.", "state:enum", "title:string?", "body:string", "project_id:int"],
                        "out": ["IN_PROJECT → [Project]", "RELATES_TO → [MergeRequest, Project]"],
                        "in": ["RELATES_TO ← [MergeRequest]"],
                    },
                    "Project",
                ]}],
                "edges": ["IN_PROJECT", "RELATES_TO"],
            })
        );
    }

    #[test]
    fn compact_form_lists_types_and_enum_values() {
        let response =
            build_compact_schema_response(&model(), IntrospectionScope::All, Some("MergeRequest"));
        assert_eq!(
            json(&response),
            serde_json::json!({
                "domains": [{"name": "core", "nodes": [{
                    "name": "MergeRequest",
                    "props": ["id:int", "state:enum (opened|merged)", "title:string?", "body:string", "project_id:int"],
                    "out": ["IN_PROJECT → [Project]", "RELATES_TO → [MergeRequest, Project]"],
                    "in": ["RELATES_TO ← [MergeRequest]"],
                }]}],
                "edges": [],
            })
        );
    }

    #[test]
    fn local_scope_lists_local_entities_and_their_store_columns() {
        let model = model();
        assert_eq!(
            json(&build_compact_schema_response(
                &model,
                IntrospectionScope::All,
                None
            )),
            serde_json::json!({
                "domains": [{"name": "core", "nodes": [
                    {"name": "MergeRequest", "props": "id, state (opened|merged), title, body, project_id"},
                    {"name": "Project", "props": "id, name"},
                ]}],
                "edges": [
                    "(MergeRequest)-[:IN_PROJECT]->(Project)",
                    "(MergeRequest)-[:RELATES_TO]->(MergeRequest|Project)",
                ],
            })
        );
        assert_eq!(
            json(&build_compact_schema_response(
                &model,
                IntrospectionScope::Local,
                None
            )),
            serde_json::json!({
                "domains": [{"name": "core", "nodes": [
                    {"name": "MergeRequest", "props": "id, state (opened|merged), title, project_id, traversal_path"},
                ]}],
                "edges": ["(MergeRequest)-[:RELATES_TO]->(MergeRequest)"],
            })
        );
    }

    #[test]
    fn unknown_and_out_of_scope_nodes_fail_alike() {
        let model = model();
        let error = |scope, name: &str| {
            build_schema_response(&model, scope, &[name.into()])
                .unwrap_err()
                .to_string()
        };
        assert_eq!(
            error(IntrospectionScope::Local, "Project"),
            "unknown node(s): Project. Valid nodes: MergeRequest"
        );
        assert_eq!(
            error(IntrospectionScope::Local, "Nope"),
            "unknown node(s): Nope. Valid nodes: MergeRequest"
        );
        assert_eq!(
            error(IntrospectionScope::All, "Nope"),
            "unknown node(s): Nope. Valid nodes: MergeRequest, Project"
        );
        assert!(
            build_schema_response(&model, IntrospectionScope::All, &["Project".into()]).is_ok()
        );
    }
}
