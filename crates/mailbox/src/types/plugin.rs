//! Plugin registration types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::PluginSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plugin {
    pub plugin_id: String,
    pub namespace_id: i64,
    #[serde(skip_serializing)]
    pub api_key_hash: String,
    pub schema: PluginSchema,
    pub schema_version: i64,
    pub created_at: DateTime<Utc>,
}

impl Plugin {
    pub fn new(
        plugin_id: impl Into<String>,
        namespace_id: i64,
        api_key_hash: impl Into<String>,
        schema: PluginSchema,
    ) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            namespace_id,
            api_key_hash: api_key_hash.into(),
            schema,
            schema_version: 1,
            created_at: Utc::now(),
        }
    }

    pub fn table_name_for_node(&self, node_kind: &str) -> String {
        let normalized_kind = node_kind.to_lowercase().replace('-', "_");
        format!(
            "gl_plugin_{}_{}",
            self.plugin_id.replace('-', "_"),
            normalized_kind
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginInfo {
    pub plugin_id: String,
    pub namespace_id: i64,
    pub schema: PluginSchema,
    pub schema_version: i64,
    pub created_at: DateTime<Utc>,
}

impl From<Plugin> for PluginInfo {
    fn from(plugin: Plugin) -> Self {
        Self {
            plugin_id: plugin.plugin_id,
            namespace_id: plugin.namespace_id,
            schema: plugin.schema,
            schema_version: plugin.schema_version,
            created_at: plugin.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeDefinition, PropertyDefinition, PropertyType};

    #[test]
    fn table_name_generation() {
        let schema =
            PluginSchema::new().with_node(NodeDefinition::new("security_scanner_Vulnerability"));

        let plugin = Plugin::new("security-scanner", 42, "hash", schema);

        assert_eq!(
            plugin.table_name_for_node("security_scanner_Vulnerability"),
            "gl_plugin_security_scanner_security_scanner_vulnerability"
        );
    }

    #[test]
    fn plugin_info_excludes_api_key_hash() {
        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("test_Node")
                .with_property(PropertyDefinition::new("name", PropertyType::String)),
        );

        let plugin = Plugin::new("test", 1, "secret_hash", schema);
        let info = PluginInfo::from(plugin);

        let json = serde_json::to_string(&info).unwrap();
        assert!(!json.contains("secret_hash"));
        assert!(!json.contains("api_key_hash"));
    }
}
