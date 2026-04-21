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
/// and a flat list of edges.
#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub domains: Vec<SchemaDomain>,
    pub edges: Vec<SchemaEdge>,
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

/// A relationship (edge) with all source/target node kinds it connects.
#[derive(Debug, Serialize)]
pub struct SchemaEdge {
    pub name: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
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
        edges: build_edges(ontology, scope),
    }
}

/// Build just the edges list for the given ontology and scope, skipping
/// the domain/node construction.
#[must_use]
pub fn build_schema_edges(ontology: &Ontology, scope: IntrospectionScope) -> Vec<SchemaEdge> {
    build_edges(ontology, scope)
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

fn build_edges(ontology: &Ontology, scope: IntrospectionScope) -> Vec<SchemaEdge> {
    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    ontology
        .edge_names()
        .filter_map(|edge_name| {
            let variants = ontology.get_edge(edge_name).unwrap_or(&[]);
            let filtered = filter_variants(variants, scope, &local_names);
            if filtered.is_empty() {
                return None;
            }

            let mut sources: Vec<String> = filtered.iter().map(|e| e.source_kind.clone()).collect();
            sources.sort();
            sources.dedup();

            let mut targets: Vec<String> = filtered.iter().map(|e| e.target_kind.clone()).collect();
            targets.sort();
            targets.dedup();

            Some(SchemaEdge {
                name: edge_name.to_string(),
                from: sources,
                to: targets,
            })
        })
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

        let has_outgoing = filtered.iter().any(|e| e.source_kind == node_name);
        let has_incoming = filtered.iter().any(|e| e.target_kind == node_name);

        if has_outgoing {
            outgoing.push(edge_name.to_string());
        }
        if has_incoming {
            incoming.push(edge_name.to_string());
        }
    }

    outgoing.sort();
    incoming.sort();
    (outgoing, incoming)
}

fn format_property(field: &Field) -> String {
    let nullable = if field.nullable { "?" } else { "" };
    format!(
        "{}:{}{}",
        field.name,
        field.data_type.to_string().to_lowercase(),
        nullable
    )
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
    fn local_scope_edges_only_connect_local_entities() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::Local, &[]);

        let local: Vec<&str> = ont.local_entity_names();
        for edge in &response.edges {
            for s in &edge.from {
                assert!(
                    local.contains(&s.as_str()),
                    "edge {} source {} not in local scope",
                    edge.name,
                    s
                );
            }
            for t in &edge.to {
                assert!(
                    local.contains(&t.as_str()),
                    "edge {} target {} not in local scope",
                    edge.name,
                    t
                );
            }
        }
        assert!(
            !response.edges.is_empty(),
            "expected at least one local edge"
        );
    }

    #[test]
    fn local_expand_definition_omits_traversal_path() {
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

        for p in props {
            assert!(
                !p.starts_with("traversal_path:"),
                "traversal_path should be excluded from local scope, got {p}"
            );
        }
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
        assert!(response.edges.iter().any(|e| e.name == "AUTHORED"));
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
        assert!(file.0.contains(&"DEFINES".to_string()) || file.0.contains(&"IMPORTS".to_string()));
        assert!(file.1.contains(&"CONTAINS".to_string()));
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
