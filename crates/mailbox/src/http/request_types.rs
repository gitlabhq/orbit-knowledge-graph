//! HTTP request types.

use serde::Deserialize;

use crate::types::{EdgePayload, EdgeReference, NodePayload, NodeReference, PluginSchema};

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterPluginRequest {
    pub plugin_id: String,
    pub namespace_id: i64,
    pub api_key: String,
    pub schema: PluginSchema,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitMessageRequest {
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

impl From<SubmitMessageRequest> for crate::types::MailboxMessage {
    fn from(request: SubmitMessageRequest) -> Self {
        Self {
            message_id: request.message_id,
            plugin_id: request.plugin_id,
            nodes: request.nodes,
            edges: request.edges,
            delete_nodes: request.delete_nodes,
            delete_edges: request.delete_edges,
        }
    }
}
