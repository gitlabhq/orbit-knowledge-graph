//! Plugin schema definitions.

use serde::{Deserialize, Serialize};

use super::PropertyType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub property_type: PropertyType,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
}

impl PropertyDefinition {
    pub fn new(name: impl Into<String>, property_type: PropertyType) -> Self {
        Self {
            name: name.into(),
            property_type,
            nullable: false,
            enum_values: None,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn with_enum_values(mut self, values: Vec<String>) -> Self {
        self.enum_values = Some(values);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDefinition {
    pub name: String,
    pub properties: Vec<PropertyDefinition>,
}

impl NodeDefinition {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            properties: Vec::new(),
        }
    }

    pub fn with_property(mut self, property: PropertyDefinition) -> Self {
        self.properties.push(property);
        self
    }

    pub fn with_properties(mut self, properties: Vec<PropertyDefinition>) -> Self {
        self.properties = properties;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeDefinition {
    pub relationship_kind: String,
    pub from_node_kinds: Vec<String>,
    pub to_node_kinds: Vec<String>,
}

impl EdgeDefinition {
    pub fn new(relationship_kind: impl Into<String>) -> Self {
        Self {
            relationship_kind: relationship_kind.into(),
            from_node_kinds: Vec::new(),
            to_node_kinds: Vec::new(),
        }
    }

    pub fn from_kinds(mut self, kinds: Vec<String>) -> Self {
        self.from_node_kinds = kinds;
        self
    }

    pub fn to_kinds(mut self, kinds: Vec<String>) -> Self {
        self.to_node_kinds = kinds;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginSchema {
    #[serde(default)]
    pub nodes: Vec<NodeDefinition>,
    #[serde(default)]
    pub edges: Vec<EdgeDefinition>,
}

impl PluginSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_node(mut self, node: NodeDefinition) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn with_edge(mut self, edge: EdgeDefinition) -> Self {
        self.edges.push(edge);
        self
    }

    pub fn get_node(&self, name: &str) -> Option<&NodeDefinition> {
        self.nodes.iter().find(|n| n.name == name)
    }

    pub fn get_edge(&self, relationship_kind: &str) -> Option<&EdgeDefinition> {
        self.edges
            .iter()
            .find(|e| e.relationship_kind == relationship_kind)
    }

    pub fn node_names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|n| n.name.as_str())
    }

    pub fn edge_relationship_kinds(&self) -> impl Iterator<Item = &str> {
        self.edges.iter().map(|e| e.relationship_kind.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_definition_builder() {
        let property = PropertyDefinition::new("severity", PropertyType::Enum)
            .nullable()
            .with_enum_values(vec!["low".into(), "medium".into(), "high".into()]);

        assert_eq!(property.name, "severity");
        assert_eq!(property.property_type, PropertyType::Enum);
        assert!(property.nullable);
        assert_eq!(property.enum_values.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn node_definition_builder() {
        let node = NodeDefinition::new("Vulnerability")
            .with_property(PropertyDefinition::new("score", PropertyType::Float))
            .with_property(PropertyDefinition::new("cve_id", PropertyType::String).nullable());

        assert_eq!(node.name, "Vulnerability");
        assert_eq!(node.properties.len(), 2);
    }

    #[test]
    fn plugin_schema_serde_roundtrip() {
        let schema = PluginSchema::new()
            .with_node(
                NodeDefinition::new("security_scanner_Vulnerability")
                    .with_property(PropertyDefinition::new("severity", PropertyType::Enum))
                    .with_property(PropertyDefinition::new("score", PropertyType::Float)),
            )
            .with_edge(
                EdgeDefinition::new("security_scanner_AFFECTS")
                    .from_kinds(vec!["security_scanner_Vulnerability".into()])
                    .to_kinds(vec!["File".into(), "Project".into()]),
            );

        let json = serde_json::to_string_pretty(&schema).unwrap();
        let parsed: PluginSchema = serde_json::from_str(&json).unwrap();

        assert_eq!(schema, parsed);
    }
}
