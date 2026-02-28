//! Dynamic Arrow schema generation from ontology entities.
//!
//! Provides the `ToArrowSchema` extension trait used throughout the simulator,
//! delegating to [`synthetic_graph::batch::arrow_schema`] for the actual conversion.

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use ontology::{DataType, Field, NodeEntity};

/// Extension trait to convert ontology types to Arrow types.
pub trait ToArrowSchema {
    fn to_arrow_schema(&self) -> Schema;
}

impl ToArrowSchema for NodeEntity {
    fn to_arrow_schema(&self) -> Schema {
        synthetic_graph::batch::arrow_schema::node_to_arrow_schema(self)
    }
}

/// Extension trait to convert ontology Field to Arrow Field.
pub trait ToArrowField {
    fn to_arrow_field(&self) -> ArrowField;
}

impl ToArrowField for Field {
    fn to_arrow_field(&self) -> ArrowField {
        let arrow_type = self.data_type.to_arrow_type();
        ArrowField::new(&self.name, arrow_type, self.nullable)
    }
}

/// Extension trait to convert ontology DataType to Arrow DataType.
pub trait ToArrowType {
    fn to_arrow_type(&self) -> ArrowDataType;
}

impl ToArrowType for DataType {
    fn to_arrow_type(&self) -> ArrowDataType {
        synthetic_graph::batch::arrow_schema::ontology_to_arrow_type(self)
    }
}

/// Create the Arrow schema for the unified edges table.
pub fn edge_schema() -> Schema {
    synthetic_graph::batch::arrow_schema::edge_schema()
}

/// Convert Arrow schema to ClickHouse CREATE TABLE statement.
pub fn to_clickhouse_ddl(table_name: &str, schema: &Schema, order_by: &[&str]) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let ch_type = arrow_to_clickhouse_type(field.data_type(), field.is_nullable());
            format!("    {} {}", field.name(), ch_type)
        })
        .collect();

    let order_by_clause = order_by.join(", ");

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE = ReplacingMergeTree()\nORDER BY ({});",
        table_name,
        columns.join(",\n"),
        order_by_clause
    )
}

/// Convert Arrow DataType to ClickHouse type string.
fn arrow_to_clickhouse_type(arrow_type: &ArrowDataType, nullable: bool) -> String {
    let base_type = match arrow_type {
        ArrowDataType::Boolean => "Bool",
        ArrowDataType::Int8 => "Int8",
        ArrowDataType::Int16 => "Int16",
        ArrowDataType::Int32 => "Int32",
        ArrowDataType::Int64 => "Int64",
        ArrowDataType::UInt8 => "UInt8",
        ArrowDataType::UInt16 => "UInt16",
        ArrowDataType::UInt32 => "UInt32",
        ArrowDataType::UInt64 => "UInt64",
        ArrowDataType::Float32 => "Float32",
        ArrowDataType::Float64 => "Float64",
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "String",
        ArrowDataType::Date32 => "Date",
        ArrowDataType::Date64 => "DateTime64(6, 'UTC')",
        ArrowDataType::Timestamp(_, _) => "DateTime64(6, 'UTC')",
        _ => "String",
    };

    if nullable {
        format!("Nullable({})", base_type)
    } else {
        base_type.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_to_arrow_schema() {
        let node = NodeEntity {
            name: "User".to_string(),
            domain: "core".to_string(),
            description: "Test user entity".to_string(),
            label: "username".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "username".to_string(),
                    source: "username".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "email".to_string(),
                    source: "email".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_user".to_string(),
            style: Default::default(),
            etl: None,
            redaction: None,
        };

        let schema = node.to_arrow_schema();
        assert_eq!(schema.fields().len(), 4); // traversal_path + 3 fields

        assert_eq!(schema.field(0).name(), "traversal_path");
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Utf8);

        assert_eq!(schema.field(1).name(), "id");
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Int64);

        assert_eq!(schema.field(2).name(), "username");
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Utf8);
    }

    #[test]
    fn test_edge_schema() {
        let schema = edge_schema();
        assert_eq!(schema.fields().len(), 6);

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "traversal_path",
                "relationship_kind",
                "source_id",
                "source_kind",
                "target_id",
                "target_kind"
            ]
        );
    }

    #[test]
    fn test_to_clickhouse_ddl() {
        let schema = edge_schema();
        let ddl = to_clickhouse_ddl(
            "gl_edge",
            &schema,
            &["relationship_kind", "source_kind", "source_id"],
        );

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS gl_edge"));
        assert!(ddl.contains("relationship_kind String"));
        assert!(ddl.contains("ORDER BY (relationship_kind, source_kind, source_id)"));
    }
}
