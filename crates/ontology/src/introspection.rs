//! Consumed by both the `orbit-server` MCP `get_graph_schema` tool (full
//! ontology) and the local `orbit` CLI `schema` subcommand (filtered to
//! entities present in the local DuckDB graph).

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{DataType, EdgeEntity, Field, NodeEntity, Ontology};

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
    Listed {
        name: String,
        props: String,
    },
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shows: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        search: Option<Vec<String>>,
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
        domains: build_domains(ontology, scope, expand_nodes, None, format_property, false),
        edges: build_edge_names(ontology, scope, None),
    })
}

#[must_use]
pub fn build_compact_schema_response(
    ontology: &Ontology,
    scope: IntrospectionScope,
    node: Option<&str>,
) -> SchemaResponse {
    match node {
        None => SchemaResponse {
            domains: build_overview_domains(ontology, scope),
            edges: build_relationship_patterns(ontology, scope),
        },
        Some(node) => SchemaResponse {
            domains: build_domains(
                ontology,
                scope,
                &[],
                Some(node),
                format_compact_property,
                true,
            ),
            edges: Vec::new(),
        },
    }
}

const OVERVIEW_DOMAINS: [&str; 3] = ["core", "code_review", "plan"];

fn build_overview_domains(ontology: &Ontology, scope: IntrospectionScope) -> Vec<SchemaDomain> {
    let mut domains = build_domains(ontology, scope, &[], None, format_compact_property, false);
    for domain in &mut domains {
        if !OVERVIEW_DOMAINS.contains(&domain.name.as_str()) {
            continue;
        }
        for node in &mut domain.nodes {
            let SchemaNode::Name(name) = node else {
                continue;
            };
            let Some(entity) = ontology.get_node(name) else {
                continue;
            };
            let fields: Vec<&Field> = match scope {
                IntrospectionScope::Local => ontology.local_entity_fields(name).unwrap_or_default(),
                IntrospectionScope::All => entity.listed_fields().collect(),
            };
            let props = fields
                .iter()
                .map(|field| format_listed_property(field))
                .collect::<Vec<_>>();
            *node = SchemaNode::Listed {
                name: name.clone(),
                props: props.join(", "),
            };
        }
    }
    domains
}

fn format_listed_property(field: &Field) -> String {
    match &field.enum_values {
        Some(values) => {
            let values: Vec<&str> = values.values().map(String::as_str).collect();
            format!("{} ({})", field.name, values.join("|"))
        }
        None => field.name.clone(),
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
    format: fn(&Field) -> String,
    with_search: bool,
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
                IntrospectionScope::All => node.listed_fields().collect(),
            };

            let props: Vec<String> = fields.iter().map(|f| format(f)).collect();

            let (outgoing, incoming) = node_relationships(ontology, scope, &node.name);

            let (label, shows, search) = if with_search {
                (
                    Some(node.label.clone()),
                    Some(shown_fields(node, &fields)),
                    Some(search_fields(node, &fields)),
                )
            } else {
                (None, None, None)
            };

            SchemaNode::Expanded {
                name: node.name.clone(),
                props,
                out: outgoing,
                r#in: incoming,
                label,
                shows,
                search,
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

fn shown_fields(node: &NodeEntity, fields: &[&Field]) -> Vec<String> {
    let shown = fields
        .iter()
        .filter(|field| !field.hidden && !field.admin_only);
    if node.default_columns.is_empty() {
        return shown.map(|field| field.name.clone()).collect();
    }
    let shown: Vec<&str> = shown.map(|field| field.name.as_str()).collect();
    node.default_columns
        .iter()
        .filter(|name| shown.contains(&name.as_str()))
        .cloned()
        .collect()
}

/// Fields every caller may filter with CONTAINS, STARTS WITH, or ENDS WITH.
#[must_use]
pub fn is_text_searchable(field: &Field) -> bool {
    field.data_type == DataType::String
        && field.like_allowed
        && field.filterable
        && !field.admin_only
        && !field.hidden
        && !field.is_virtual()
}

fn search_fields(node: &NodeEntity, fields: &[&Field]) -> Vec<String> {
    let mut search: Vec<String> = fields
        .iter()
        .filter(|field| is_text_searchable(field))
        .map(|field| field.name.clone())
        .collect();
    if let Some(index) = search.iter().position(|name| *name == node.label) {
        let label = search.remove(index);
        search.insert(0, label);
    }
    search
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

fn format_compact_property(field: &Field) -> String {
    let nullable = if field.nullable { "?" } else { "" };
    let mut text = format!(
        "{}:{}{}",
        field.name,
        field.data_type.to_string().to_lowercase(),
        nullable
    );
    if let Some(values) = &field.enum_values {
        let values: Vec<&str> = values.values().map(String::as_str).collect();
        text.push_str(&format!(" ({})", values.join("|")));
    }
    text
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
                    SchemaNode::Listed { name, .. } | SchemaNode::Expanded { name, .. } => {
                        name.clone()
                    }
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
    fn local_expand_definition_includes_traversal_path() {
        let ont = load();
        let response =
            build_schema_response(&ont, IntrospectionScope::Local, &["Definition".to_string()])
                .unwrap();

        let props = expanded_props(&response, "Definition");
        assert!(
            props.iter().any(|p| p.starts_with("traversal_path:")),
            "traversal_path should be included in local scope for hydration TP narrowing"
        );
    }

    #[test]
    fn hidden_fields_are_left_out_of_schema_and_node_schema() {
        let ont = load();
        let schema =
            build_schema_response(&ont, IntrospectionScope::All, &["WorkItem".to_string()])
                .unwrap();
        let node_schema =
            build_compact_schema_response(&ont, IntrospectionScope::All, Some("WorkItem"));

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
    fn compact_schema_lists_relationship_patterns_and_enum_values() {
        let ont = load();
        let overview = build_compact_schema_response(&ont, IntrospectionScope::All, None);
        assert!(
            overview
                .edges
                .iter()
                .any(|edge| edge.starts_with("(User)-[:AUTHORED]->(")),
            "{:?}",
            overview.edges
        );
        let listed = |wanted: &str| {
            overview
                .domains
                .iter()
                .flat_map(|domain| &domain.nodes)
                .find_map(|node| match node {
                    SchemaNode::Listed { name, props } if name == wanted => Some(props.clone()),
                    _ => None,
                })
        };
        assert!(listed("User").unwrap().contains("username"));
        assert!(
            listed("WorkItem")
                .unwrap()
                .contains("state (opened|closed)")
        );
        assert!(listed("Pipeline").is_none());

        let node = build_compact_schema_response(&ont, IntrospectionScope::All, Some("WorkItem"));
        let props = expanded_props(&node, "WorkItem");
        let state = props.iter().find(|p| p.starts_with("state:")).unwrap();
        assert!(state.contains("(opened|closed)"), "{state}");
        assert!(!props.iter().any(|p| p.contains(" — ")), "{props:?}");
        assert!(node.edges.is_empty());
    }

    fn search_hints(
        response: &SchemaResponse,
    ) -> (Option<String>, Option<Vec<String>>, Option<Vec<String>>) {
        match &response.domains[0].nodes[0] {
            SchemaNode::Expanded {
                label,
                shows,
                search,
                ..
            } => (label.clone(), shows.clone(), search.clone()),
            other => panic!("expected an expanded node, got {other:?}"),
        }
    }

    #[test]
    fn node_schema_says_how_to_search_each_node() {
        let ont = load();
        let pipeline =
            build_compact_schema_response(&ont, IntrospectionScope::All, Some("Pipeline"));
        let (label, shows, search) = search_hints(&pipeline);
        assert_eq!(label.as_deref(), Some("id"));
        assert_eq!(shows.unwrap(), ["id", "iid", "status", "ref", "source"]);
        let search = search.unwrap();
        assert!(search.contains(&"ref".to_string()), "{search:?}");
        assert!(!search.contains(&"status".to_string()), "{search:?}");

        let (label, _, search) = search_hints(&build_compact_schema_response(
            &ont,
            IntrospectionScope::All,
            Some("Vulnerability"),
        ));
        assert_eq!(label.as_deref(), Some("title"));
        assert!(!search.unwrap().contains(&"title".to_string()));

        let (_, shows, search) = search_hints(&build_compact_schema_response(
            &ont,
            IntrospectionScope::All,
            Some("User"),
        ));
        assert_eq!(search.as_ref().unwrap()[0], "username");
        assert!(!search.unwrap().contains(&"email".to_string()));
        assert_eq!(shows.unwrap(), ["id", "username", "name", "state"]);

        let (_, _, search) = search_hints(&build_compact_schema_response(
            &ont,
            IntrospectionScope::All,
            Some("MergeRequest"),
        ));
        assert_eq!(search.unwrap()[0], "title");
    }

    #[test]
    fn search_hints_only_list_fields_the_schema_shows() {
        let ont = load();
        for scope in [IntrospectionScope::All, IntrospectionScope::Local] {
            for name in ont.node_names().filter(|name| scope.includes(&ont, name)) {
                let response = build_compact_schema_response(&ont, scope, Some(name));
                let props = expanded_props(&response, name);
                let (label, shows, search) = search_hints(&response);
                assert_eq!(label.unwrap(), ont.get_node(name).unwrap().label);
                for field in shows.unwrap().iter().chain(&search.unwrap()) {
                    assert!(
                        props.iter().any(|p| p.starts_with(&format!("{field}:"))),
                        "{name}.{field} is not in {props:?}"
                    );
                    assert_ne!(field, "traversal_path", "{name}");
                }
            }
        }
    }

    #[test]
    fn overview_and_full_schema_leave_out_search_hints() {
        let ont = load();
        let overview = serde_json::to_string(&build_compact_schema_response(
            &ont,
            IntrospectionScope::All,
            None,
        ))
        .unwrap();
        let full = serde_json::to_string(
            &build_schema_response(&ont, IntrospectionScope::All, &["*".to_string()]).unwrap(),
        )
        .unwrap();
        for text in [overview, full] {
            for key in ["\"label\"", "\"shows\"", "\"search\""] {
                assert!(!text.contains(key), "{key}");
            }
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
                    SchemaNode::Listed { name, .. } | SchemaNode::Expanded { name, .. } => {
                        name.clone()
                    }
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
                    ..
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
