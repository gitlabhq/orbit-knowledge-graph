//! Dynamic Arrow schema generation from ontology entities.

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use ontology::constants::TRAVERSAL_PATH_COLUMN;
use ontology::{DataType, Field, NodeEntity};

/// Extension trait to convert ontology types to Arrow types.
pub trait ToArrowSchema {
    /// Convert to an Arrow schema, prepending traversal_path.
    fn to_arrow_schema(&self) -> Schema;
}

impl ToArrowSchema for NodeEntity {
    fn to_arrow_schema(&self) -> Schema {
        let mut fields = vec![ArrowField::new(
            TRAVERSAL_PATH_COLUMN,
            ArrowDataType::Utf8,
            false,
        )];

        for field in &self.fields {
            // Skip traversal_path if defined in ontology - it's a system column
            if field.name == TRAVERSAL_PATH_COLUMN {
                continue;
            }
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
            DataType::String | DataType::Enum | DataType::Uuid => ArrowDataType::Utf8,
            DataType::Int => ArrowDataType::Int64,
            DataType::Float => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Date => ArrowDataType::Date32,
            DataType::DateTime => ArrowDataType::Int64, // Unix timestamp in millis
        }
    }
}

/// Create the Arrow schema for the unified edges table.
pub fn edge_schema(ontology: &ontology::Ontology) -> Schema {
    Schema::new(
        ontology
            .edge_columns()
            .iter()
            .map(|col| {
                let arrow_type = col.data_type.to_arrow_type();
                ArrowField::new(&col.name, arrow_type, false)
            })
            .collect::<Vec<_>>(),
    )
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
///
/// This is the single source of truth for Arrow→ClickHouse type mapping.
/// Used by both `to_clickhouse_ddl()` and `clickhouse::schema::SchemaGenerator`.
pub(crate) fn arrow_to_clickhouse_type(arrow_type: &ArrowDataType, nullable: bool) -> String {
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
        other => panic!("unsupported Arrow type: {other:?}"),
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
    use ontology::FieldSource;

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
                    source: FieldSource::DatabaseColumn("id".to_string()),
                    data_type: DataType::Int,
                    ..Default::default()
                },
                Field {
                    name: "username".to_string(),
                    source: FieldSource::DatabaseColumn("username".to_string()),
                    data_type: DataType::String,
                    ..Default::default()
                },
                Field {
                    name: "email".to_string(),
                    source: FieldSource::DatabaseColumn("email".to_string()),
                    data_type: DataType::String,
                    nullable: true,
                    ..Default::default()
                },
            ],
            destination_table: "gl_user".to_string(),
            ..Default::default()
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
        let ontology = ontology::Ontology::load_embedded().expect("must load");
        let schema = edge_schema(&ontology);
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
    fn test_edge_schema_matches_ontology_constants() {
        let ontology = ontology::Ontology::load_embedded().expect("must load");
        let schema = edge_schema(&ontology);
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert_eq!(
            field_names,
            ontology::constants::EDGE_RESERVED_COLUMNS,
            "edge_schema() columns must match ontology::constants::EDGE_RESERVED_COLUMNS"
        );
    }

    #[test]
    fn test_arrow_to_clickhouse_type_nullable() {
        let ty = arrow_to_clickhouse_type(&ArrowDataType::Int64, true);
        assert_eq!(ty, "Nullable(Int64)");

        let ty = arrow_to_clickhouse_type(&ArrowDataType::Int64, false);
        assert_eq!(ty, "Int64");
    }

    #[test]
    fn test_arrow_to_clickhouse_type_datetime() {
        let ty = arrow_to_clickhouse_type(&ArrowDataType::Date64, false);
        assert_eq!(ty, "DateTime64(3)");

        let ty = arrow_to_clickhouse_type(
            &ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        );
        assert_eq!(ty, "DateTime64(3)");
    }

    #[test]
    fn test_to_clickhouse_ddl() {
        let ontology = ontology::Ontology::load_embedded().expect("must load");
        let schema = edge_schema(&ontology);
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
