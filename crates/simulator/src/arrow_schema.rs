//! Dynamic Arrow schema generation from ontology entities.

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use ontology::{DataType, Field, NodeEntity};

/// Extension trait to convert ontology types to Arrow types.
pub trait ToArrowSchema {
    /// Convert to an Arrow schema, prepending organization_id and traversal_id.
    fn to_arrow_schema(&self) -> Schema;
}

impl ToArrowSchema for NodeEntity {
    fn to_arrow_schema(&self) -> Schema {
        let mut fields = vec![
            ArrowField::new("organization_id", ArrowDataType::UInt32, false),
            ArrowField::new("traversal_id", ArrowDataType::Utf8, false),
        ];

        for field in &self.fields {
            fields.push(field.to_arrow_field());
        }

        Schema::new(fields)
    }
}

/// Extension trait to convert ontology Field to Arrow Field.
pub trait ToArrowField {
    /// Convert to an Arrow field.
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
    /// Convert to Arrow DataType.
    fn to_arrow_type(&self) -> ArrowDataType;
}

impl ToArrowType for DataType {
    fn to_arrow_type(&self) -> ArrowDataType {
        match self {
            DataType::String | DataType::Enum => ArrowDataType::Utf8,
            DataType::Int => ArrowDataType::Int64,
            DataType::Float => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Date => ArrowDataType::Date32,
            DataType::DateTime => ArrowDataType::Int64, // Unix timestamp in millis
        }
    }
}

/// Create the Arrow schema for the unified edges table.
///
/// Matches `EdgeEntity` from ontology:
/// - relationship_kind: Utf8 (e.g., "AUTHORED", "CONTAINS")
/// - source: Int64 (source node ID)
/// - source_kind: Utf8 (e.g., "User", "Group")
/// - target: Int64 (target node ID)
/// - target_kind: Utf8 (e.g., "MergeRequest", "Project")
pub fn edge_schema() -> Schema {
    Schema::new(vec![
        ArrowField::new("relationship_kind", ArrowDataType::Utf8, false),
        ArrowField::new("source", ArrowDataType::Int64, false),
        ArrowField::new("source_kind", ArrowDataType::Utf8, false),
        ArrowField::new("target", ArrowDataType::Int64, false),
        ArrowField::new("target_kind", ArrowDataType::Utf8, false),
    ])
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
        _ => "String", // Fallback
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
            fields: vec![
                Field {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "username".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "email".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_users".to_string(),
            etl: None,
        };

        let schema = node.to_arrow_schema();
        assert_eq!(schema.fields().len(), 5); // organization_id + traversal_id + 3 fields

        assert_eq!(schema.field(0).name(), "organization_id");
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::UInt32);

        assert_eq!(schema.field(1).name(), "traversal_id");
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Utf8);

        assert_eq!(schema.field(2).name(), "id");
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Int64);

        assert_eq!(schema.field(3).name(), "username");
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Utf8);
    }

    #[test]
    fn test_edge_schema() {
        let schema = edge_schema();
        assert_eq!(schema.fields().len(), 5);

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "relationship_kind",
                "source",
                "source_kind",
                "target",
                "target_kind"
            ]
        );
    }

    #[test]
    fn test_to_clickhouse_ddl() {
        let schema = edge_schema();
        let ddl = to_clickhouse_ddl(
            "kg_edges",
            &schema,
            &["relationship_kind", "source_kind", "source"],
        );

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS kg_edges"));
        assert!(ddl.contains("relationship_kind String"));
        assert!(ddl.contains("ORDER BY (relationship_kind, source_kind, source)"));
    }
}
