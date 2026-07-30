//! Builds the `get_graph_schema` response for the `gkg-server` MCP tool.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{Adjacency, Channel, Dir, Field, Ontology, OntologyGraph};

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
    build_schema_response_for_channel(ontology, expand_nodes, None)
}

/// ADR 013: builds the schema the caller on `channel` sees. It runs against the
/// channel-scoped graph, so entities not visible on the channel are absent
/// (their nodes and any touching edges never appear); the caller cannot tell
/// they exist (§5). `None` returns the full schema.
#[must_use]
pub fn build_schema_response_for_channel(
    ontology: &Ontology,
    expand_nodes: &[String],
    channel: Option<Channel>,
) -> SchemaResponse {
    let graph = ontology.graph_for(channel);
    let edges = ontology
        .edges()
        .filter(|e| {
            graph.node_template(&e.source_kind).is_some()
                && graph.node_template(&e.target_kind).is_some()
        })
        .map(|e| e.relationship_kind.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    SchemaResponse {
        domains: build_domains(ontology, graph, expand_nodes),
        edges,
    }
}

fn build_domains(
    ontology: &Ontology,
    graph: &OntologyGraph,
    expand_nodes: &[String],
) -> Vec<SchemaDomain> {
    let mut domain_map: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();

    for node in ontology.nodes() {
        if graph.node_template(&node.name).is_none() {
            continue;
        }
        let domain_name = if node.domain.is_empty() {
            "other".to_string()
        } else {
            node.domain.clone()
        };

        let should_expand = expand_nodes.iter().any(|n| n == "*" || n == &node.name);

        let node_info = if should_expand {
            let props: Vec<String> = node.fields.iter().map(format_property).collect();
            let (outgoing, incoming) = node_relationships(graph, &node.name);

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

    let outgoing = group(
        &graph.neighbors(node_name, Dir::Outgoing).adjacencies(),
        '→',
    );
    let incoming = group(
        &graph.neighbors(node_name, Dir::Incoming).adjacencies(),
        '←',
    );
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

    fn node_names(response: &SchemaResponse) -> Vec<String> {
        response
            .domains
            .iter()
            .flat_map(|d| {
                d.nodes.iter().map(|n| match n {
                    SchemaNode::Name(s) => s.clone(),
                    SchemaNode::Expanded { name, .. } => name.clone(),
                })
            })
            .collect()
    }

    #[test]
    fn channel_filter_omits_entities_hidden_from_channel() {
        let ont = Ontology::new()
            .with_nodes(["User", "Project"])
            .with_edges(["CONTAINS"])
            .with_edge_variant(crate::EdgeEntity {
                relationship_kind: "CONTAINS".into(),
                source: "Project".into(),
                source_kind: "Project".into(),
                target: "User".into(),
                target_kind: "User".into(),
                destination_table: crate::EDGE_TABLE.into(),
                fk_column: None,
                scope: None,
            })
            .with_node_channels("User", [Channel::CoreFeature])
            .with_node_channels("Project", Channel::ALL);

        let hidden = build_schema_response_for_channel(&ont, &[], Some(Channel::ExternalAgent));
        assert_eq!(node_names(&hidden), vec!["Project"]);
        assert!(
            hidden.edges.is_empty(),
            "CONTAINS touches the hidden User, so it drops: {:?}",
            hidden.edges
        );

        let visible = build_schema_response_for_channel(&ont, &[], Some(Channel::CoreFeature));
        assert!(node_names(&visible).contains(&"User".to_string()));
        assert_eq!(visible.edges, vec!["CONTAINS"]);
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
