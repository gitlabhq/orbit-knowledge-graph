//! Dynamic Arrow schema generation from ontology entities.

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use ontology::{DataType, Field, NodeEntity};

/// Extension trait to convert ontology types to Arrow types.
pub trait ToArrowSchema {
    /// Convert to an Arrow schema, prepending tenant_id.
    fn to_arrow_schema(&self) -> Schema;
}

impl ToArrowSchema for NodeEntity {
    fn to_arrow_schema(&self) -> Schema {
        let mut fields = vec![ArrowField::new("tenant_id", ArrowDataType::UInt32, false)];

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
            DataType::String => ArrowDataType::Utf8,
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
/// Matches `EdgeEntity` from ontology + tenant_id:
/// - tenant_id: UInt32
/// - relationship_kind: Utf8 (e.g., "AUTHORED", "CONTAINS")
/// - source: Int64 (source node ID)
/// - source_kind: Utf8 (e.g., "User", "Group")
/// - target: Int64 (target node ID)
/// - target_kind: Utf8 (e.g., "MergeRequest", "Project")
pub fn edge_schema() -> Schema {
    Schema::new(vec![
        ArrowField::new("tenant_id", ArrowDataType::UInt32, false),
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
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE = MergeTree()\nORDER BY ({});",
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
        ArrowDataType::Date64 => "DateTime64(3)",
        ArrowDataType::Timestamp(_, _) => "DateTime64(3)",
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
                },
                Field {
                    name: "username".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                Field {
                    name: "email".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            primary_keys: vec!["id".to_string()],
        };

        let schema = node.to_arrow_schema();
        assert_eq!(schema.fields().len(), 4); // tenant_id + 3 fields

        assert_eq!(schema.field(0).name(), "tenant_id");
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::UInt32);

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
                "tenant_id",
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
        let ddl = to_clickhouse_ddl("kg_edges", &schema, &["tenant_id", "relationship_kind"]);

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS kg_edges"));
        assert!(ddl.contains("tenant_id UInt32"));
        assert!(ddl.contains("relationship_kind String"));
        assert!(ddl.contains("ORDER BY (tenant_id, relationship_kind)"));
    }
}
