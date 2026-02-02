//! Message types for mailbox ingestion.

use etl_engine::types::{Event, Topic};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAILBOX_STREAM: &str = "mailbox-stream";
pub const MAILBOX_SUBJECT: &str = "mailbox.messages";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePayload {
    pub external_id: String,
    pub node_kind: String,
    pub properties: Value,
}

impl NodePayload {
    pub fn new(external_id: impl Into<String>, node_kind: impl Into<String>) -> Self {
        Self {
            external_id: external_id.into(),
            node_kind: node_kind.into(),
            properties: Value::Object(serde_json::Map::new()),
        }
    }

    pub fn with_properties(mut self, properties: Value) -> Self {
        self.properties = properties;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeReference {
    pub node_kind: String,
    pub external_id: String,
}

impl NodeReference {
    pub fn new(node_kind: impl Into<String>, external_id: impl Into<String>) -> Self {
        Self {
            node_kind: node_kind.into(),
            external_id: external_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgePayload {
    pub external_id: String,
    pub relationship_kind: String,
    pub source: NodeReference,
    pub target: NodeReference,
}

impl EdgePayload {
    pub fn new(
        external_id: impl Into<String>,
        relationship_kind: impl Into<String>,
        source: NodeReference,
        target: NodeReference,
    ) -> Self {
        Self {
            external_id: external_id.into(),
            relationship_kind: relationship_kind.into(),
            source,
            target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeReference {
    pub relationship_kind: String,
    pub external_id: String,
}

impl EdgeReference {
    pub fn new(relationship_kind: impl Into<String>, external_id: impl Into<String>) -> Self {
        Self {
            relationship_kind: relationship_kind.into(),
            external_id: external_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxMessage {
    pub message_id: String,
    pub plugin_id: String,
    #[serde(default)]
    pub nodes: Vec<NodePayload>,
    #[serde(default)]
    pub edges: Vec<EdgePayload>,
    #[serde(default)]
    pub delete_nodes: Vec<NodeReference>,
    #[serde(default)]
    pub delete_edges: Vec<EdgeReference>,
}

impl MailboxMessage {
    pub fn new(message_id: impl Into<String>, plugin_id: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            plugin_id: plugin_id.into(),
            nodes: Vec::new(),
            edges: Vec::new(),
            delete_nodes: Vec::new(),
            delete_edges: Vec::new(),
        }
    }

    pub fn with_node(mut self, node: NodePayload) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn with_nodes(mut self, nodes: Vec<NodePayload>) -> Self {
        self.nodes = nodes;
        self
    }

    pub fn with_edge(mut self, edge: EdgePayload) -> Self {
        self.edges.push(edge);
        self
    }

    pub fn with_edges(mut self, edges: Vec<EdgePayload>) -> Self {
        self.edges = edges;
        self
    }

    pub fn with_delete_node(mut self, node_ref: NodeReference) -> Self {
        self.delete_nodes.push(node_ref);
        self
    }

    pub fn with_delete_nodes(mut self, delete_nodes: Vec<NodeReference>) -> Self {
        self.delete_nodes = delete_nodes;
        self
    }

    pub fn with_delete_edge(mut self, edge_ref: EdgeReference) -> Self {
        self.delete_edges.push(edge_ref);
        self
    }

    pub fn with_delete_edges(mut self, delete_edges: Vec<EdgeReference>) -> Self {
        self.delete_edges = delete_edges;
        self
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn delete_node_count(&self) -> usize {
        self.delete_nodes.len()
    }

    pub fn delete_edge_count(&self) -> usize {
        self.delete_edges.len()
    }
}

impl Event for MailboxMessage {
    fn topic() -> Topic {
        Topic::new(MAILBOX_STREAM, MAILBOX_SUBJECT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn message_serde_roundtrip() {
        let message = MailboxMessage::new("msg-001", "security-scanner")
            .with_node(
                NodePayload::new("vuln-001", "security_scanner_Vulnerability")
                    .with_properties(json!({"severity": "high", "score": 8.5})),
            )
            .with_edge(EdgePayload::new(
                "edge-001",
                "security_scanner_AFFECTS",
                NodeReference::new("security_scanner_Vulnerability", "vuln-001"),
                NodeReference::new("Project", "42"),
            ));

        let json = serde_json::to_string_pretty(&message).unwrap();
        let parsed: MailboxMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(message, parsed);
        assert_eq!(parsed.node_count(), 1);
        assert_eq!(parsed.edge_count(), 1);
    }
}
