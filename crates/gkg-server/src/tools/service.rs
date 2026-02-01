use std::collections::BTreeMap;
use std::sync::Arc;

use ontology::Ontology;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use toon_format::{EncodeOptions, encode};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

impl ExecutorError {
    pub fn code(&self) -> String {
        match self {
            Self::NotFound(_) => "tool_not_found".to_string(),
            Self::InvalidArguments(_) => "invalid_arguments".to_string(),
        }
    }
}

pub enum ToolPlan {
    RunGraphQuery { query_json: String },
    Immediate { result: Value },
}

#[derive(Debug, Clone)]
pub struct ToolService {
    ontology: Arc<Ontology>,
}

impl ToolService {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        Self { ontology }
    }

    pub fn resolve(
        &self,
        tool_name: &str,
        arguments_json: &str,
    ) -> Result<ToolPlan, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        match tool_name {
            "query_graph" => self.resolve_query_graph(&arguments),
            "get_graph_entities" => self.execute_get_graph_entities(&arguments),
            _ => Err(ExecutorError::NotFound(tool_name.to_string())),
        }
    }

    fn resolve_query_graph(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let query_json = serde_json::to_string(arguments)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        Ok(ToolPlan::RunGraphQuery { query_json })
    }

    fn execute_get_graph_entities(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let args: GetGraphEntitiesArgs = serde_json::from_value(arguments.clone())
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        let response = self.build_graph_entities_response(&args)?;
        let result = self.format_as_toon(&response)?;

        Ok(ToolPlan::Immediate { result })
    }

    fn build_graph_entities_response(
        &self,
        args: &GetGraphEntitiesArgs,
    ) -> Result<GraphEntitiesResponse, ExecutorError> {
        let expand_nodes = args.expand_nodes.as_deref().unwrap_or(&[]);

        let domains = self.build_domains(expand_nodes);
        let edges = self.build_edges();

        Ok(GraphEntitiesResponse { domains, edges })
    }

    fn build_domains(&self, expand_nodes: &[String]) -> Vec<DomainInfo> {
        let mut domain_map: BTreeMap<String, Vec<NodeInfo>> = BTreeMap::new();

        for node in self.ontology.nodes() {
            let domain_name = if node.domain.is_empty() {
                "other".to_string()
            } else {
                node.domain.clone()
            };

            let should_expand = expand_nodes.iter().any(|n| n == &node.name);

            let node_info = if should_expand {
                let props: Vec<String> = node
                    .fields
                    .iter()
                    .map(|f| {
                        let nullable = if f.nullable { "?" } else { "" };
                        format!(
                            "{}:{}{}",
                            f.name,
                            f.data_type.to_string().to_lowercase(),
                            nullable
                        )
                    })
                    .collect();

                let rels = self.get_node_relationships(&node.name);

                NodeInfo::Expanded {
                    name: node.name.clone(),
                    props,
                    out: rels.outgoing,
                    r#in: rels.incoming,
                }
            } else {
                NodeInfo::Name(node.name.clone())
            };

            domain_map.entry(domain_name).or_default().push(node_info);
        }

        domain_map
            .into_iter()
            .map(|(name, nodes)| DomainInfo { name, nodes })
            .collect()
    }

    fn build_edges(&self) -> Vec<EdgeInfo> {
        self.ontology
            .edge_names()
            .map(|edge_name| {
                let variants = self.ontology.get_edge(edge_name).unwrap_or(&[]);

                let mut sources: Vec<String> =
                    variants.iter().map(|e| e.source_kind.clone()).collect();
                sources.sort();
                sources.dedup();

                let mut targets: Vec<String> =
                    variants.iter().map(|e| e.target_kind.clone()).collect();
                targets.sort();
                targets.dedup();

                EdgeInfo {
                    name: edge_name.to_string(),
                    from: sources,
                    to: targets,
                }
            })
            .collect()
    }

    fn get_node_relationships(&self, node_name: &str) -> NodeRelationships {
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();

        for edge_name in self.ontology.edge_names() {
            if let Some(edges) = self.ontology.get_edge(edge_name) {
                let mut has_outgoing = false;
                let mut has_incoming = false;

                for edge in edges {
                    if edge.source_kind == node_name {
                        has_outgoing = true;
                    }
                    if edge.target_kind == node_name {
                        has_incoming = true;
                    }
                }

                if has_outgoing {
                    outgoing.push(edge_name.to_string());
                }
                if has_incoming {
                    incoming.push(edge_name.to_string());
                }
            }
        }

        outgoing.sort();
        incoming.sort();

        NodeRelationships { outgoing, incoming }
    }

    fn format_as_toon(&self, response: &GraphEntitiesResponse) -> Result<Value, ExecutorError> {
        let options = EncodeOptions::default();
        let toon_str = encode(response, &options).map_err(|e| {
            ExecutorError::InvalidArguments(format!("Failed to encode as toon: {e}"))
        })?;

        Ok(json!(toon_str))
    }
}

#[derive(Debug, Deserialize)]
struct GetGraphEntitiesArgs {
    #[serde(default)]
    expand_nodes: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct GraphEntitiesResponse {
    domains: Vec<DomainInfo>,
    edges: Vec<EdgeInfo>,
}

#[derive(Debug, Serialize)]
struct DomainInfo {
    name: String,
    nodes: Vec<NodeInfo>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum NodeInfo {
    Name(String),
    Expanded {
        name: String,
        props: Vec<String>,
        out: Vec<String>,
        r#in: Vec<String>,
    },
}

#[derive(Debug, Serialize)]
struct EdgeInfo {
    name: String,
    from: Vec<String>,
    to: Vec<String>,
}

struct NodeRelationships {
    outgoing: Vec<String>,
    incoming: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_toon_output(args: &str) -> String {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("get_graph_entities", args)
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => result.as_str().unwrap().to_string(),
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn test_base_call_contains_domains_and_edges() {
        let output = get_toon_output("{}");

        assert!(output.contains("domains"), "Missing domains section");
        assert!(output.contains("edges"), "Missing edges section");
    }

    #[test]
    fn test_base_call_contains_known_domains() {
        let output = get_toon_output("{}");

        assert!(output.contains("core"), "Missing core domain");
        assert!(output.contains("plan"), "Missing plan domain");
        assert!(output.contains("ci"), "Missing ci domain");
    }

    #[test]
    fn test_base_call_contains_known_nodes() {
        let output = get_toon_output("{}");

        assert!(output.contains("User"), "Missing User node");
        assert!(output.contains("Project"), "Missing Project node");
        assert!(output.contains("MergeRequest"), "Missing MergeRequest node");
        assert!(output.contains("WorkItem"), "Missing WorkItem node");
    }

    #[test]
    fn test_base_call_contains_known_edges() {
        let output = get_toon_output("{}");

        assert!(output.contains("AUTHORED"), "Missing AUTHORED edge");
        assert!(output.contains("CONTAINS"), "Missing CONTAINS edge");
    }

    #[test]
    fn test_edges_show_source_and_target_nodes() {
        let output = get_toon_output("{}");

        assert!(
            output.contains("from") && output.contains("to"),
            "Edges should have from/to fields"
        );
    }

    #[test]
    fn test_expand_nodes_shows_properties() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(output.contains("props"), "Expanded node should have props");
        assert!(
            output.contains("username"),
            "User should have username property"
        );
        assert!(output.contains("id"), "User should have id property");
    }

    #[test]
    fn test_expand_nodes_shows_relationships() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(
            output.contains("out") || output.contains("in"),
            "Expanded node should have relationship info"
        );
    }

    #[test]
    fn test_property_format_has_type() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(
            output.contains("id:int") || output.contains("id:integer"),
            "Properties should include type: {}",
            output
        );
    }

    #[test]
    fn test_unexpanded_nodes_are_compact() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        let project_in_output = output.contains("Project");
        assert!(project_in_output, "Project should be in output");

        let project_props = output.contains("Project") && output.contains("Project,{");
        assert!(
            !project_props || output.contains("User,{"),
            "Only expanded nodes should have properties block"
        );
    }

    #[test]
    fn test_output_is_not_json() {
        let output = get_toon_output("{}");

        assert!(
            !output.starts_with('{'),
            "Output should be TOON format, not JSON"
        );
    }

    #[test]
    fn test_query_graph_returns_run_graph_query_plan() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("query_graph", r#"{"match":{}}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::RunGraphQuery { query_json } => {
                assert!(query_json.contains("match"));
            }
            _ => panic!("Expected RunGraphQuery plan"),
        }
    }
}
