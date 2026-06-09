//! Schema introspection: generate an LLM- or user-readable description of the
//! nodes and edges defined in an [`Ontology`].
//!
//! This module is consumed by both the `gkg-server` MCP `get_graph_schema`
//! tool (full ontology) and the local `orbit` CLI `schema` subcommand
//! (filtered to entities present in the local DuckDB graph).

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{EdgeEntity, Field, Ontology};

/// Which slice of the ontology to describe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IntrospectionScope {
    /// Every node and edge defined in the ontology.
    #[default]
    All,
    /// Only entities and edges present in the local DuckDB graph
    /// (driven by `settings.local_db.entities` in the ontology YAML).
    Local,
}

/// Top-level schema response — a list of domains (each grouping its nodes)
/// and edge names for orientation. Expand specific nodes to see which types
/// each edge connects.
#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub domains: Vec<SchemaDomain>,
    pub edges: Vec<String>,
}

/// One domain from the ontology (e.g. `source_code`, `ci`) plus its nodes.
#[derive(Debug, Serialize)]
pub struct SchemaDomain {
    pub name: String,
    pub nodes: Vec<SchemaNode>,
}

/// A node in the schema. Condensed by default (name only); expanded when the
/// caller passes the node name (or `"*"`) in `expand_nodes`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SchemaNode {
    Name(String),
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
    },
}

/// Build a schema response for the given ontology and scope.
///
/// `expand_nodes` controls which nodes get expanded to props/in/out; pass
/// `["*"]` to expand everything, or specific names like `["User"]`.
#[must_use]
pub fn build_schema_response(
    ontology: &Ontology,
    scope: IntrospectionScope,
    expand_nodes: &[String],
) -> SchemaResponse {
    SchemaResponse {
        domains: build_domains(ontology, scope, expand_nodes),
        edges: build_edge_names(ontology, scope),
    }
}

fn build_domains(
    ontology: &Ontology,
    scope: IntrospectionScope,
    expand_nodes: &[String],
) -> Vec<SchemaDomain> {
    let mut domain_map: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();

    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    for node in ontology.nodes() {
        if scope == IntrospectionScope::Local && !local_names.contains(&node.name.as_str()) {
            continue;
        }

        let domain_name = if node.domain.is_empty() {
            "other".to_string()
        } else {
            node.domain.clone()
        };

        let should_expand = expand_nodes.iter().any(|n| n == "*" || n == &node.name);

        let node_info = if should_expand {
            let fields: Vec<&Field> = match scope {
                IntrospectionScope::Local => {
                    ontology.local_entity_fields(&node.name).unwrap_or_default()
                }
                IntrospectionScope::All => node.fields.iter().collect(),
            };

            let props: Vec<String> = fields.iter().map(|f| format_property(f)).collect();

            let (outgoing, incoming) = node_relationships(ontology, scope, &node.name);

            SchemaNode::Expanded {
                name: node.name.clone(),
                props,
                out: outgoing,
                r#in: incoming,
            }
        } else {
            SchemaNode::Name(node.name.clone())
        };

        domain_map.entry(domain_name).or_default().push(node_info);
    }

    domain_map
        .into_iter()
        .map(|(name, nodes)| SchemaDomain { name, nodes })
        .collect()
}

fn build_edge_names(ontology: &Ontology, scope: IntrospectionScope) -> Vec<String> {
    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    ontology
        .edge_names()
        .filter(|edge_name| {
            let variants = ontology.get_edge(edge_name).unwrap_or(&[]);
            !filter_variants(variants, scope, &local_names).is_empty()
        })
        .map(|name| name.to_string())
        .collect()
}

fn filter_variants<'a>(
    variants: &'a [EdgeEntity],
    scope: IntrospectionScope,
    local_names: &[&str],
) -> Vec<&'a EdgeEntity> {
    match scope {
        IntrospectionScope::All => variants.iter().collect(),
        IntrospectionScope::Local => variants
            .iter()
            .filter(|e| {
                local_names.contains(&e.source_kind.as_str())
                    && local_names.contains(&e.target_kind.as_str())
            })
            .collect(),
    }
}

fn node_relationships(
    ontology: &Ontology,
    scope: IntrospectionScope,
    node_name: &str,
) -> (Vec<String>, Vec<String>) {
    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    let mut outgoing = Vec::new();
    let mut incoming = Vec::new();

    for edge_name in ontology.edge_names() {
        let Some(variants) = ontology.get_edge(edge_name) else {
            continue;
        };
        let filtered = filter_variants(variants, scope, &local_names);

        let mut out_targets: Vec<&str> = filtered
            .iter()
            .filter(|e| e.source_kind == node_name)
            .map(|e| e.target_kind.as_str())
            .collect();
        out_targets.sort();
        out_targets.dedup();

        let mut in_sources: Vec<&str> = filtered
            .iter()
            .filter(|e| e.target_kind == node_name)
            .map(|e| e.source_kind.as_str())
            .collect();
        in_sources.sort();
        in_sources.dedup();

        if !out_targets.is_empty() {
            outgoing.push(format!("{} → [{}]", edge_name, out_targets.join(", ")));
        }
        if !in_sources.is_empty() {
            incoming.push(format!("{} ← [{}]", edge_name, in_sources.join(", ")));
        }
    }

    outgoing.sort();
    incoming.sort();
    (outgoing, incoming)
}

fn format_property(field: &Field) -> String {
    let nullable = if field.nullable { "?" } else { "" };
    let mut tags = Vec::new();
    if field.immutable {
        tags.push("immutable".to_string());
    }
    if let Some(ref tv) = field.terminal_values {
        tags.push(format!("terminal: {}", tv.join(", ")));
    }
    let tag_suffix = if tags.is_empty() {
        String::new()
    } else {
        format!(" [{}]", tags.join("; "))
    };
    match &field.description {
        Some(desc) => format!(
            "{}:{}{} — {}{}",
            field.name,
            field.data_type.to_string().to_lowercase(),
            nullable,
            desc,
            tag_suffix
        ),
        None => format!(
            "{}:{}{}{}",
            field.name,
            field.data_type.to_string().to_lowercase(),
            nullable,
            tag_suffix
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology loads")
    }

    #[test]
    fn local_scope_has_only_local_entities() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::Local, &[]);

        let all_node_names: Vec<String> = response
            .domains
            .iter()
            .flat_map(|d| {
                d.nodes.iter().map(|n| match n {
                    SchemaNode::Name(s) => s.clone(),
                    SchemaNode::Expanded { name, .. } => name.clone(),
                })
            })
            .collect();

        let expected: Vec<&str> = ont.local_entity_names();
        assert_eq!(all_node_names.len(), expected.len());
        for name in expected {
            assert!(
                all_node_names.iter().any(|n| n == name),
                "expected {name} in local scope, got {all_node_names:?}"
            );
        }
        for forbidden in ["User", "Project", "MergeRequest", "WorkItem"] {
            assert!(
                !all_node_names.iter().any(|n| n == forbidden),
                "unexpected {forbidden} in local scope"
            );
        }
    }

    #[test]
    fn local_scope_edges_are_present() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::Local, &[]);

        assert!(
            !response.edges.is_empty(),
            "expected at least one local edge"
        );
        for edge in &response.edges {
            assert!(!edge.is_empty(), "edge name should not be empty");
        }
    }

    #[test]
    fn local_expand_definition_includes_traversal_path() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["Definition".to_string()]);

        let props = response
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .find_map(|n| match n {
                SchemaNode::Expanded { name, props, .. } if name == "Definition" => Some(props),
                _ => None,
            })
            .expect("Definition should be expanded");

        assert!(
            props.iter().any(|p| p.starts_with("traversal_path:")),
            "traversal_path should be included in local scope for hydration TP narrowing"
        );
    }

    #[test]
    fn all_scope_contains_server_entities() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::All, &[]);
        let names: Vec<String> = response
            .domains
            .iter()
            .flat_map(|d| {
                d.nodes.iter().map(|n| match n {
                    SchemaNode::Name(s) => s.clone(),
                    SchemaNode::Expanded { name, .. } => name.clone(),
                })
            })
            .collect();
        assert!(names.iter().any(|n| n == "User"));
        assert!(response.edges.iter().any(|e| e == "AUTHORED"));
    }

    #[test]
    fn wildcard_expands_every_node() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::Local, &["*".to_string()]);
        for domain in &response.domains {
            for node in &domain.nodes {
                assert!(
                    matches!(node, SchemaNode::Expanded { .. }),
                    "wildcard should expand all nodes"
                );
            }
        }
    }

    #[test]
    fn expanded_nodes_list_relationships() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["File".to_string()]);

        let file = response
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .find_map(|n| match n {
                SchemaNode::Expanded {
                    name,
                    out,
                    r#in,
                    props,
                } if name == "File" => Some((out.clone(), r#in.clone(), props.clone())),
                _ => None,
            })
            .expect("File should be expanded");

        assert!(!file.2.is_empty(), "File should have props");
        assert!(
            file.0
                .iter()
                .any(|e| e.starts_with("DEFINES") || e.starts_with("IMPORTS")),
            "File should have outgoing DEFINES or IMPORTS: {:?}",
            file.0
        );
        assert!(
            file.1.iter().any(|e| e.starts_with("CONTAINS")),
            "File should have incoming CONTAINS: {:?}",
            file.1
        );
    }

    #[test]
    fn property_format_is_name_colon_type() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["File".to_string()]);
        let props = response
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .find_map(|n| match n {
                SchemaNode::Expanded { name, props, .. } if name == "File" => Some(props),
                _ => None,
            })
            .expect("File expanded");
        assert!(
            props.iter().any(|p| p.starts_with("path:string")),
            "expected path:string in {props:?}"
        );
    }
}
