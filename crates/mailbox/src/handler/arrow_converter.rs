//! Dynamic Arrow batch building from JSON payloads.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use chrono::{NaiveDate, Utc};
use serde_json::Value;

use crate::error::MailboxError;
use crate::handler::generate_node_id;
use crate::schema_generator::build_arrow_schema;
use crate::types::{NodeDefinition, NodePayload, NodeReference, Plugin, PropertyType};

pub struct ArrowConverter;

impl ArrowConverter {
    pub fn build_node_batch(
        plugin: &Plugin,
        node_definition: &NodeDefinition,
        nodes: &[NodePayload],
        traversal_path: &str,
    ) -> Result<RecordBatch, MailboxError> {
        let schema = Arc::new(build_arrow_schema(node_definition));

        if nodes.is_empty() {
            let mut empty_columns: Vec<ArrayRef> = Vec::new();

            empty_columns.push(Arc::new(Int64Builder::new().finish()));
            empty_columns.push(Arc::new(StringBuilder::new().finish()));

            for property in &node_definition.properties {
                let empty_col = Self::build_empty_column(property.property_type);
                empty_columns.push(empty_col);
            }

            empty_columns.push(Arc::new(
                TimestampMicrosecondBuilder::new()
                    .with_timezone("UTC")
                    .finish(),
            ));
            empty_columns.push(Arc::new(BooleanBuilder::new().finish()));

            return RecordBatch::try_new(schema, empty_columns).map_err(|e| {
                MailboxError::processing(format!("failed to create empty batch: {}", e))
            });
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        let mut id_builder = Int64Builder::with_capacity(nodes.len());
        for node in nodes {
            let id = generate_node_id(
                &plugin.plugin_id,
                plugin.namespace_id,
                &node.node_kind,
                &node.external_id,
            );
            id_builder.append_value(id);
        }
        columns.push(Arc::new(id_builder.finish()));

        let mut path_builder =
            StringBuilder::with_capacity(nodes.len(), traversal_path.len() * nodes.len());
        for _ in nodes {
            path_builder.append_value(traversal_path);
        }
        columns.push(Arc::new(path_builder.finish()));

        for property in &node_definition.properties {
            let column = Self::build_property_column(property, nodes)?;
            columns.push(column);
        }

        let now_micros = Utc::now().timestamp_micros();
        let mut version_builder =
            TimestampMicrosecondBuilder::with_capacity(nodes.len()).with_timezone("UTC");
        for _ in nodes {
            version_builder.append_value(now_micros);
        }
        columns.push(Arc::new(version_builder.finish()));

        let mut deleted_builder = BooleanBuilder::with_capacity(nodes.len());
        for _ in nodes {
            deleted_builder.append_value(false);
        }
        columns.push(Arc::new(deleted_builder.finish()));

        RecordBatch::try_new(schema, columns)
            .map_err(|e| MailboxError::processing(format!("failed to create record batch: {}", e)))
    }

    pub fn build_node_deletion_batch(
        plugin: &Plugin,
        node_definition: &NodeDefinition,
        delete_refs: &[NodeReference],
        traversal_path: &str,
    ) -> Result<RecordBatch, MailboxError> {
        let schema = Arc::new(build_arrow_schema(node_definition));

        if delete_refs.is_empty() {
            let mut empty_columns: Vec<ArrayRef> = Vec::new();
            empty_columns.push(Arc::new(Int64Builder::new().finish()));
            empty_columns.push(Arc::new(StringBuilder::new().finish()));

            for property in &node_definition.properties {
                let empty_col = Self::build_empty_column(property.property_type);
                empty_columns.push(empty_col);
            }

            empty_columns.push(Arc::new(
                TimestampMicrosecondBuilder::new()
                    .with_timezone("UTC")
                    .finish(),
            ));
            empty_columns.push(Arc::new(BooleanBuilder::new().finish()));

            return RecordBatch::try_new(schema, empty_columns).map_err(|e| {
                MailboxError::processing(format!("failed to create empty batch: {}", e))
            });
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        let mut id_builder = Int64Builder::with_capacity(delete_refs.len());
        for node_ref in delete_refs {
            let id = generate_node_id(
                &plugin.plugin_id,
                plugin.namespace_id,
                &node_ref.node_kind,
                &node_ref.external_id,
            );
            id_builder.append_value(id);
        }
        columns.push(Arc::new(id_builder.finish()));

        let mut path_builder = StringBuilder::with_capacity(
            delete_refs.len(),
            traversal_path.len() * delete_refs.len(),
        );
        for _ in delete_refs {
            path_builder.append_value(traversal_path);
        }
        columns.push(Arc::new(path_builder.finish()));

        for property in &node_definition.properties {
            let column = Self::build_null_column(property.property_type, delete_refs.len());
            columns.push(column);
        }

        let now_micros = Utc::now().timestamp_micros();
        let mut version_builder =
            TimestampMicrosecondBuilder::with_capacity(delete_refs.len()).with_timezone("UTC");
        for _ in delete_refs {
            version_builder.append_value(now_micros);
        }
        columns.push(Arc::new(version_builder.finish()));

        let mut deleted_builder = BooleanBuilder::with_capacity(delete_refs.len());
        for _ in delete_refs {
            deleted_builder.append_value(true);
        }
        columns.push(Arc::new(deleted_builder.finish()));

        RecordBatch::try_new(schema, columns).map_err(|e| {
            MailboxError::processing(format!("failed to create deletion batch: {}", e))
        })
    }

    fn build_property_column(
        property: &crate::types::PropertyDefinition,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        match property.property_type {
            PropertyType::String | PropertyType::Enum => {
                Self::build_string_column(&property.name, property.nullable, nodes)
            }
            PropertyType::Int64 => {
                Self::build_int64_column(&property.name, property.nullable, nodes)
            }
            PropertyType::Float => {
                Self::build_float64_column(&property.name, property.nullable, nodes)
            }
            PropertyType::Boolean => {
                Self::build_boolean_column(&property.name, property.nullable, nodes)
            }
            PropertyType::Date => Self::build_date_column(&property.name, property.nullable, nodes),
            PropertyType::Timestamp => {
                Self::build_timestamp_column(&property.name, property.nullable, nodes)
            }
        }
    }

    fn build_empty_column(property_type: PropertyType) -> ArrayRef {
        match property_type {
            PropertyType::String | PropertyType::Enum => Arc::new(StringBuilder::new().finish()),
            PropertyType::Int64 => Arc::new(Int64Builder::new().finish()),
            PropertyType::Float => Arc::new(Float64Builder::new().finish()),
            PropertyType::Boolean => Arc::new(BooleanBuilder::new().finish()),
            PropertyType::Date => Arc::new(Date32Builder::new().finish()),
            PropertyType::Timestamp => Arc::new(
                TimestampMicrosecondBuilder::new()
                    .with_timezone("UTC")
                    .finish(),
            ),
        }
    }

    fn build_null_column(property_type: PropertyType, count: usize) -> ArrayRef {
        match property_type {
            PropertyType::String | PropertyType::Enum => {
                let mut builder = StringBuilder::with_capacity(count, 0);
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            PropertyType::Int64 => {
                let mut builder = Int64Builder::with_capacity(count);
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            PropertyType::Float => {
                let mut builder = Float64Builder::with_capacity(count);
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            PropertyType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(count);
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            PropertyType::Date => {
                let mut builder = Date32Builder::with_capacity(count);
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            PropertyType::Timestamp => {
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(count).with_timezone("UTC");
                for _ in 0..count {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
        }
    }

    fn build_string_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder = StringBuilder::with_capacity(nodes.len(), nodes.len() * 32);

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::String(s)) => builder.append_value(s),
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected string, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn build_int64_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder = Int64Builder::with_capacity(nodes.len());

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::Number(n)) if n.is_i64() => {
                    builder.append_value(n.as_i64().unwrap());
                }
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected int64, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn build_float64_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder = Float64Builder::with_capacity(nodes.len());

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::Number(n)) => {
                    let f = n.as_f64().unwrap_or(n.as_i64().unwrap_or(0) as f64);
                    builder.append_value(f);
                }
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected float, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn build_boolean_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder = BooleanBuilder::with_capacity(nodes.len());

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::Bool(b)) => builder.append_value(*b),
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected boolean, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn build_date_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder = Date32Builder::with_capacity(nodes.len());
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::String(s)) => {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                        MailboxError::processing(format!(
                            "property '{}' on node '{}' has invalid date format",
                            name, node.external_id
                        ))
                    })?;
                    let days = date.signed_duration_since(epoch).num_days() as i32;
                    builder.append_value(days);
                }
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected date string, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn build_timestamp_column(
        name: &str,
        nullable: bool,
        nodes: &[NodePayload],
    ) -> Result<ArrayRef, MailboxError> {
        let mut builder =
            TimestampMicrosecondBuilder::with_capacity(nodes.len()).with_timezone("UTC");

        for node in nodes {
            let value = node.properties.get(name);
            match value {
                Some(Value::String(s)) => {
                    let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|_| {
                        MailboxError::processing(format!(
                            "property '{}' on node '{}' has invalid timestamp format",
                            name, node.external_id
                        ))
                    })?;
                    builder.append_value(dt.timestamp_micros());
                }
                Some(Value::Null) | None if nullable => builder.append_null(),
                Some(Value::Null) | None => {
                    return Err(MailboxError::processing(format!(
                        "missing required property '{}' on node '{}'",
                        name, node.external_id
                    )));
                }
                Some(v) => {
                    return Err(MailboxError::processing(format!(
                        "property '{}' on node '{}' expected timestamp string, got {:?}",
                        name, node.external_id, v
                    )));
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PluginSchema, PropertyDefinition};
    use serde_json::json;

    fn test_plugin() -> Plugin {
        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("test_Node")
                .with_property(PropertyDefinition::new("name", PropertyType::String))
                .with_property(PropertyDefinition::new("count", PropertyType::Int64).nullable())
                .with_property(PropertyDefinition::new("score", PropertyType::Float)),
        );

        Plugin::new("test", 42, "hash", schema)
    }

    #[test]
    fn builds_batch_with_correct_schema() {
        let plugin = test_plugin();
        let node_def = plugin.schema.get_node("test_Node").unwrap();

        let nodes = vec![
            NodePayload::new("n1", "test_Node")
                .with_properties(json!({"name": "first", "count": 10, "score": 1.5})),
            NodePayload::new("n2", "test_Node")
                .with_properties(json!({"name": "second", "count": null, "score": 2.5})),
        ];

        let batch = ArrowConverter::build_node_batch(&plugin, node_def, &nodes, "1/42").unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "traversal_path");
        assert_eq!(schema.field(2).name(), "name");
        assert_eq!(schema.field(3).name(), "count");
        assert_eq!(schema.field(4).name(), "score");
        assert_eq!(schema.field(5).name(), "_version");
        assert_eq!(schema.field(6).name(), "_deleted");
    }

    #[test]
    fn generates_deterministic_ids() {
        let plugin = test_plugin();
        let node_def = plugin.schema.get_node("test_Node").unwrap();

        let nodes = vec![
            NodePayload::new("n1", "test_Node")
                .with_properties(json!({"name": "test", "score": 1.0})),
        ];

        let batch1 = ArrowConverter::build_node_batch(&plugin, node_def, &nodes, "1/42").unwrap();
        let batch2 = ArrowConverter::build_node_batch(&plugin, node_def, &nodes, "1/42").unwrap();

        use arrow::array::Int64Array;
        let ids1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids1.value(0), ids2.value(0));
    }

    #[test]
    fn returns_empty_batch_for_no_nodes() {
        let plugin = test_plugin();
        let node_def = plugin.schema.get_node("test_Node").unwrap();

        let batch = ArrowConverter::build_node_batch(&plugin, node_def, &[], "1/42").unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
