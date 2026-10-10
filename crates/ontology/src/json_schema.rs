use serde_json::{Map, Value};

use crate::constants::{NODE_RESERVED_COLUMNS, TRAVERSAL_PATH_COLUMN};
use crate::{Field, Ontology, OntologyError};

impl Ontology {
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
                let valid_columns = field_name_allowlist(node.fields.iter());

                let filterable_fields = field_name_allowlist(
                    node.fields
                        .iter()
                        .filter(|f| f.filterable || f.name == TRAVERSAL_PATH_COLUMN),
                );

                serde_json::json!({
                    "if": { "properties": { "entity": { "const": node.name } } },
                    "then": {
                        "properties": {
                            "columns": {
                                "oneOf": [
                                    { "const": "*" },
                                    { "type": "array", "items": valid_columns, "minItems": 1 }
                                ]
                            },
                            "filters": {
                                "propertyNames": filterable_fields
                            }
                        }
                    }
                })
            })
            .collect()
    }
}

fn field_name_allowlist<'a>(fields: impl Iterator<Item = &'a Field>) -> Value {
    let (hidden, listed): (Vec<&Field>, Vec<&Field>) = fields.partition(|f| f.hidden);
    let names = |fields: Vec<&'a Field>| fields.into_iter().map(|f| f.name.as_str());
    let listed: Vec<&str> = NODE_RESERVED_COLUMNS
        .iter()
        .copied()
        .chain(names(listed))
        .collect();
    let hidden: Vec<&str> = names(hidden).collect();
    serde_json::json!({ "anyOf": [{ "enum": listed }, { "enum": hidden }] })
}
