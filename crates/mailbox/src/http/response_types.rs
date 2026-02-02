//! HTTP response types.

use serde::Serialize;

use crate::types::PluginInfo;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginInfoResponse {
    pub plugin_id: String,
    pub namespace_id: i64,
    pub schema_version: i64,
    pub created_at: String,
}

impl From<PluginInfo> for PluginInfoResponse {
    fn from(info: PluginInfo) -> Self {
        Self {
            plugin_id: info.plugin_id,
            namespace_id: info.namespace_id,
            schema_version: info.schema_version,
            created_at: info.created_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginListResponse {
    pub plugins: Vec<PluginInfoResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageAcceptedResponse {
    pub message_id: String,
    pub status: &'static str,
}

impl MessageAcceptedResponse {
    pub fn new(message_id: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            status: "accepted",
        }
    }
}
