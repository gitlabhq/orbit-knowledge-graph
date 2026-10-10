//! Consumed by both the `orbit-server` MCP `get_graph_schema` tool (full
//! ontology) and the local `orbit` CLI `schema` subcommand (filtered to
//! entities present in the local DuckDB graph).

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{EdgeEntity, Field, Ontology};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IntrospectionScope {
    #[default]
    All,
    /// Driven by `settings.local_db.entities` in the ontology YAML.
    Local,
}

impl IntrospectionScope {
    #[must_use]
    pub fn includes(self, ontology: &Ontology, name: &str) -> bool {
        ontology.get_node(name).is_some()
            && (self == Self::All || ontology.local_entity_names().contains(&name))
    }
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
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
    },
}

/// `expand_nodes`: pass `["*"]` to expand every node, or specific names.
pub fn build_schema_response(
    ontology: &Ontology,
    scope: IntrospectionScope,
    expand_nodes: &[String],
) -> Result<SchemaResponse, UnknownNodes> {
    let unknown: Vec<String> = expand_nodes
        .iter()
        .filter(|name| *name != "*" && !scope.includes(ontology, name))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        let valid = ontology
            .node_names()
            .filter(|name| scope.includes(ontology, name))
            .map(str::to_owned)
            .collect();
        return Err(UnknownNodes { unknown, valid });
    }
    Ok(SchemaResponse {
        domains: build_domains(ontology, scope, expand_nodes, None),
        edges: build_edge_names(ontology, scope, None),
    })
}

#[must_use]
pub fn build_node_schema_response(
    ontology: &Ontology,
    scope: IntrospectionScope,
    node: &str,
) -> SchemaResponse {
    SchemaResponse {
        domains: build_domains(ontology, scope, &[], Some(node)),
        edges: build_edge_names(ontology, scope, Some(node)),
    }
}

/// One `(Source)-[:TYPE]->(Target|Target)` line per source node and edge type.
#[must_use]
pub fn build_relationship_patterns(ontology: &Ontology, scope: IntrospectionScope) -> Vec<String> {
    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    let mut targets: BTreeMap<(&str, &str), Vec<&str>> = BTreeMap::new();
    for edge_name in ontology.edge_names() {
        let variants = ontology.get_edge(edge_name).unwrap_or(&[]);
        for edge in filter_variants(variants, scope, &local_names) {
            targets
                .entry((edge.source_kind.as_str(), edge_name))
                .or_default()
                .push(edge.target_kind.as_str());
        }
    }

    targets
        .into_iter()
        .map(|((source, edge_name), mut kinds)| {
            kinds.sort_unstable();
            kinds.dedup();
            format!("({source})-[:{edge_name}]->({})", kinds.join("|"))
        })
        .collect()
}

fn build_domains(
    ontology: &Ontology,
    scope: IntrospectionScope,
    expand_nodes: &[String],
    only_node: Option<&str>,
) -> Vec<SchemaDomain> {
    let mut domain_map: BTreeMap<String, Vec<SchemaNode>> = BTreeMap::new();

    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    for node in ontology.nodes() {
        if only_node.is_some_and(|name| name != node.name)
            || (scope == IntrospectionScope::Local && !local_names.contains(&node.name.as_str()))
        {
            continue;
        }

        let domain_name = if node.domain.is_empty() {
            "other".to_string()
        } else {
            node.domain.clone()
        };

        let should_expand =
            only_node.is_some() || expand_nodes.iter().any(|n| n == "*" || n == &node.name);

        let node_info = if should_expand {
            let fields: Vec<&Field> = match scope {
                IntrospectionScope::Local => {
                    ontology.local_entity_fields(&node.name).unwrap_or_default()
                }
                IntrospectionScope::All => node.fields.iter().collect(),
            };

            let props: Vec<String> = fields
                .iter()
                .filter(|f| !f.hidden)
                .map(|f| format_property(f))
                .collect();

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

fn build_edge_names(
    ontology: &Ontology,
    scope: IntrospectionScope,
    only_node: Option<&str>,
) -> Vec<String> {
    let local_names: Vec<&str> = match scope {
        IntrospectionScope::Local => ontology.local_entity_names(),
        IntrospectionScope::All => Vec::new(),
    };

    ontology
        .edge_names()
        .filter(|edge_name| {
            let variants = ontology.get_edge(edge_name).unwrap_or(&[]);
            filter_variants(variants, scope, &local_names)
                .iter()
                .any(|edge| {
                    only_node
                        .is_none_or(|name| edge.source_kind == name || edge.target_kind == name)
                })
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
    fn local_scope_has_only_local_entities() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::Local, &[]).unwrap();

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
        let response = build_schema_response(&ont, IntrospectionScope::Local, &[]).unwrap();

        assert!(
            !response.edges.is_empty(),
            "expected at least one local edge"
        );
        for edge in &response.edges {
            assert!(!edge.is_empty(), "edge name should not be empty");
        }
    }

    fn expanded_props(response: &SchemaResponse, node: &str) -> Vec<String> {
        response
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .find_map(|n| match n {
                SchemaNode::Expanded { name, props, .. } if name == node => Some(props.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{node} should be expanded"))
    }

    #[test]
    fn local_expand_definition_hides_traversal_path() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["Definition".to_string()])
                .unwrap();

        let props = expanded_props(&response, "Definition");
        assert!(
            !props.iter().any(|p| p.starts_with("traversal_path:")),
            "{props:?}"
        );
        assert!(ont.get_node("Definition").unwrap().has_traversal_path);
    }

    #[test]
    fn hidden_fields_are_left_out_of_schema_and_node_schema() {
        let ont = load();
        let schema =
            build_schema_response(&ont, IntrospectionScope::All, &["WorkItem".to_string()])
                .unwrap();
        let node_schema = build_node_schema_response(&ont, IntrospectionScope::All, "WorkItem");

        for props in [
            expanded_props(&schema, "WorkItem"),
            expanded_props(&node_schema, "WorkItem"),
        ] {
            assert!(props.iter().any(|p| p.starts_with("title:")), "{props:?}");
            assert!(
                !props.iter().any(|p| p.starts_with("traversal_path:")),
                "{props:?}"
            );
        }
    }

    #[test]
    fn all_scope_contains_server_entities() {
        let ont = load();
        let response = build_schema_response(&ont, IntrospectionScope::All, &[]).unwrap();
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
    fn expand_nodes_rejects_names_outside_the_scope() {
        let ont = load();
        let non_local = ont
            .node_names()
            .find(|name| !IntrospectionScope::Local.includes(&ont, name))
            .expect("some node is not local")
            .to_string();

        let error = build_schema_response(
            &ont,
            IntrospectionScope::All,
            &["User".to_string(), "FakeNode".to_string()],
        )
        .unwrap_err();
        assert_eq!(error.unknown, ["FakeNode"]);
        assert!(error.valid.contains(&non_local));

        let error = build_schema_response(
            &ont,
            IntrospectionScope::Local,
            std::slice::from_ref(&non_local),
        )
        .unwrap_err();
        assert_eq!(error.unknown, [non_local.as_str()]);
        assert!(!error.valid.contains(&non_local));
    }

    #[test]
    fn wildcard_expands_every_node() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["*".to_string()]).unwrap();
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
            build_schema_response(&ont, IntrospectionScope::Local, &["File".to_string()]).unwrap();

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
    fn relationship_patterns_group_targets_per_source_and_edge() {
        let ont = load();
        let patterns = build_relationship_patterns(&ont, IntrospectionScope::All);
        let authored = patterns
            .iter()
            .find(|line| line.starts_with("(User)-[:AUTHORED]->("))
            .expect("User AUTHORED pattern");
        assert!(authored.contains("MergeRequest") && authored.contains('|'));
        let mut sorted = patterns.clone();
        sorted.sort();
        assert_eq!(patterns, sorted);
        for line in &patterns {
            assert!(
                line.starts_with('(') && line.contains(")-[:") && line.ends_with(')'),
                "malformed pattern {line}"
            );
        }
    }

    #[test]
    fn property_format_is_name_colon_type() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["File".to_string()]).unwrap();
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
