//! Plugin authentication helpers.

use std::sync::Arc;

use axum::http::HeaderMap;

use crate::auth::verify_api_key;
use crate::error::MailboxError;
use crate::storage::PluginStore;
use crate::types::Plugin;

pub const PLUGIN_TOKEN_HEADER: &str = "X-Plugin-Token";
pub const PLUGIN_ID_HEADER: &str = "X-Plugin-Id";

pub struct PluginAuth {
    pub plugin: Plugin,
}

impl PluginAuth {
    pub async fn from_headers(
        headers: &HeaderMap,
        plugin_store: &Arc<PluginStore>,
    ) -> Result<Self, MailboxError> {
        let plugin_id = headers
            .get(PLUGIN_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| MailboxError::Authentication("Missing X-Plugin-Id header".into()))?;

        let token = headers
            .get(PLUGIN_TOKEN_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| MailboxError::Authentication("Missing X-Plugin-Token header".into()))?;

        let plugin =
            plugin_store
                .get(plugin_id)
                .await?
                .ok_or_else(|| MailboxError::PluginNotFound {
                    plugin_id: plugin_id.to_string(),
                })?;

        let is_valid = verify_api_key(token, &plugin.api_key_hash)?;

        if !is_valid {
            return Err(MailboxError::Authentication("Invalid API token".into()));
        }

        Ok(PluginAuth { plugin })
    }
}
