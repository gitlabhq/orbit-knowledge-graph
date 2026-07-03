//! Builds the `get_graph_schema` response for the `gkg-server` MCP tool.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::etl::EdgeDirection;
use crate::{Adjacency, Field, Ontology, OntologyGraph};

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
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
    },
}

/// `expand_nodes`: pass `["*"]` to expand every node, or specific names.
#[must_use]
pub fn build_schema_response(ontology: &Ontology, expand_nodes: &[String]) -> SchemaResponse {
    SchemaResponse {
        domains: build_domains(ontology, expand_nodes),
        edges: ontology.edge_names().map(str::to_string).collect(),
    }
}

fn build_domains(ontology: &Ontology, expand_nodes: &[String]) -> Vec<SchemaDomain> {
    let mut domain_map: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();
    let graph = ontology.graph();

    for node in ontology.nodes() {
        let domain_name = if node.domain.is_empty() {
            "other".to_string()
        } else {
            node.domain.clone()
        };

        let should_expand = expand_nodes.iter().any(|n| n == "*" || n == &node.name);

        let node_info = if should_expand {
            let props: Vec<String> = node.fields.iter().map(format_property).collect();
            let (outgoing, incoming) = node_relationships(&graph, &node.name);

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

fn node_relationships(graph: &OntologyGraph, node_name: &str) -> (Vec<String>, Vec<String>) {
    let group = |adjacencies: &[Adjacency], arrow: char| {
        let mut by_kind: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for adj in adjacencies {
            by_kind
                .entry(&adj.relationship_kind)
                .or_default()
                .insert(&adj.neighbor_kind);
        }
        by_kind
            .into_iter()
            .map(|(kind, neighbors)| {
                format!(
                    "{kind} {arrow} [{}]",
                    neighbors.into_iter().collect::<Vec<_>>().join(", ")
                )
            })
            .collect()
    };

    let outgoing = group(&graph.neighbors(node_name, EdgeDirection::Outgoing), '→');
    let incoming = group(&graph.neighbors(node_name, EdgeDirection::Incoming), '←');
    (outgoing, incoming)
}

fn format_property(field: &Field) -> String {
    let nullable = if field.nullable { "?" } else { "" };
    match &field.description {
        Some(desc) => format!(
            "{}:{}{} — {}",
            field.name,
            field.data_type.to_string().to_lowercase(),
            nullable,
            desc
        ),
        None => format!(
            "{}:{}{}",
            field.name,
            field.data_type.to_string().to_lowercase(),
            nullable
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
    fn contains_server_entities_and_edges() {
        let ont = load();
        let response = build_schema_response(&ont, &[]);
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
    fn expand_definition_includes_traversal_path() {
        let ont = load();
        let response = build_schema_response(&ont, &["Definition".to_string()]);

        let props = response
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .find_map(|n| match n {
                SchemaNode::Expanded { name, props, .. } if name == "Definition" => Some(props),
                _ => None,
            })
            .expect("Definition should be expanded");

        assert!(props.iter().any(|p| p.starts_with("traversal_path:")));
    }

    #[test]
    fn wildcard_expands_every_node() {
        let ont = load();
        let response = build_schema_response(&ont, &["*".to_string()]);
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
        let response = build_schema_response(&ont, &["File".to_string()]);

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
        let response = build_schema_response(&ont, &["File".to_string()]);
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
