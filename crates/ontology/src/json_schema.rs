//! JSON Schema generation from ontology definitions.
//!
//! Populates a base query DSL schema with ontology-specific values:
//! entity types, relationship types, per-node column validation, etc.

use serde_json::{Map, Value};

use crate::constants::NODE_RESERVED_COLUMNS;
use crate::{Ontology, OntologyError};

impl Ontology {
    /// Generate a JSON Schema with ontology values populated.
    ///
    /// Given a base schema template, this populates:
    /// - `$defs.EntityType.enum` with valid entity types
    /// - `$defs.RelationshipTypeName.enum` with valid relationship types (including wildcard `*`)
    /// - `$defs.NodeProperties` with property definitions per node type
    /// - `$defs.NodeSelector.allOf` with per-entity column and filter validation
    ///
    /// # Errors
    ///
    /// Returns an error if the base schema is invalid JSON or missing required sections.
    pub fn derive_json_schema(&self, base_schema_json: &str) -> Result<Value, OntologyError> {
        let mut schema: Value = serde_json::from_str(base_schema_json)
            .map_err(|e| OntologyError::Validation(format!("failed to parse base schema: {e}")))?;

        let defs = schema
            .get_mut("$defs")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| OntologyError::Validation("schema missing $defs".into()))?;

        if let Some(entity_type) = defs.get_mut("EntityType").and_then(Value::as_object_mut) {
            let types: Vec<Value> = self
                .node_names()
                .map(|s| Value::String(s.to_string()))
                .collect();
            entity_type.insert("enum".to_string(), Value::Array(types));
        }

        if let Some(rel_type) = defs
            .get_mut("RelationshipTypeName")
            .and_then(Value::as_object_mut)
        {
            let types: Vec<Value> = self
                .edge_names()
                .map(|s| Value::String(s.to_string()))
                .chain(std::iter::once(Value::String("*".to_string())))
                .collect();
            rel_type.insert("enum".to_string(), Value::Array(types));
        }

        let node_props = self.build_node_properties_schema();
        defs.insert("NodeProperties".to_string(), node_props);

        let entity_conditions = self.build_node_selector_validation();
        if let Some(node_selector) = defs.get_mut("NodeSelector").and_then(Value::as_object_mut) {
            node_selector.insert("allOf".to_string(), Value::Array(entity_conditions));
        }

        // Inject compiler performance options into QueryOptions. These are
        // kept out of the base schema to reduce token usage for LLM consumers
        // that only need the structural query surface.
        if let Some(query_options) = defs.get_mut("QueryOptions").and_then(Value::as_object_mut) {
            if let Some(props) = query_options
                .get_mut("properties")
                .and_then(Value::as_object_mut)
            {
                props.insert(
                    "skip_dedup".to_string(),
                    serde_json::json!({
                        "type": "boolean",
                        "description": "Skip ReplacingMergeTree deduplication. Not allowed for aggregation queries.",
                        "default": false
                    }),
                );
                props.insert(
                    "materialize_ctes".to_string(),
                    serde_json::json!({
                        "type": "boolean",
                        "description": "Materialize multi-referenced CTEs for reduced redundant scans.",
                        "default": false
                    }),
                );
                props.insert(
                    "use_semi_join".to_string(),
                    serde_json::json!({
                        "type": "boolean",
                        "description": "Rewrite IN-subquery SIP patterns to LEFT SEMI JOIN.",
                        "default": false
                    }),
                );
            }
            query_options.insert("additionalProperties".to_string(), Value::Bool(false));
        }

        Ok(schema)
    }

    fn build_node_properties_schema(&self) -> Value {
        let mut node_props = Map::new();

        for node in self.nodes() {
            let mut prop_map = Map::new();

            for field in &node.fields {
                let mut prop_schema = Map::new();
                prop_schema.insert(
                    "type".to_string(),
                    Value::String(field.data_type.to_json_schema_type().to_string()),
                );

                if let Some(enum_values) = &field.enum_values {
                    let values: Vec<Value> = enum_values
                        .values()
                        .map(|v| Value::String(v.clone()))
                        .collect();
                    prop_schema.insert("enum".to_string(), Value::Array(values));
                }

                prop_map.insert(field.name.clone(), Value::Object(prop_schema));
            }

            node_props.insert(node.name.clone(), Value::Object(prop_map));
        }

        Value::Object(node_props)
    }

    fn build_node_selector_validation(&self) -> Vec<Value> {
        self.nodes()
            .map(|node| {
                // All fields are valid for column selection.
                let valid_columns: Vec<Value> = NODE_RESERVED_COLUMNS
                    .iter()
                    .map(|s| Value::String((*s).to_string()))
                    .chain(node.fields.iter().map(|f| Value::String(f.name.clone())))
                    .collect();

                // Only filterable fields are valid filter targets.
                let filterable_fields: Vec<Value> = NODE_RESERVED_COLUMNS
                    .iter()
                    .map(|s| Value::String((*s).to_string()))
                    .chain(
                        node.fields
                            .iter()
                            .filter(|f| f.filterable)
                            .map(|f| Value::String(f.name.clone())),
                    )
                    .collect();

                serde_json::json!({
                    "if": { "properties": { "entity": { "const": node.name } } },
                    "then": {
                        "properties": {
                            "columns": {
                                "oneOf": [
                                    { "const": "*" },
                                    { "type": "array", "items": { "enum": valid_columns }, "minItems": 1 }
                                ]
                            },
                            "filters": {
                                "propertyNames": { "enum": filterable_fields }
                            }
                        }
                    }
                })
            })
            .collect()
    }
}
