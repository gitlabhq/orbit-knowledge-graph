//! Guards that every ontology-derived column type has a `logical_byte_size` counting rule.

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use query_engine::compiler::ddl::ColumnType;
    use query_engine::compiler::generate_graph_tables;
    use std::sync::Arc;

    fn ddl_column_type_to_arrow(column_type: &ColumnType) -> DataType {
        match column_type {
            ColumnType::String => DataType::Utf8,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::UInt64 => DataType::UInt64,
            ColumnType::UInt32 => DataType::UInt32,
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Date32 => DataType::Date32,
            ColumnType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnType::Timestamp { .. } => DataType::Timestamp(TimeUnit::Microsecond, None),
            // Enums are written as their variant name, not their integer discriminant.
            ColumnType::Enum8(_) => DataType::Utf8,
            ColumnType::Nullable(inner) | ColumnType::LowCardinality(inner) => {
                ddl_column_type_to_arrow(inner)
            }
            ColumnType::Array(inner) => DataType::List(Arc::new(Field::new(
                "item",
                ddl_column_type_to_arrow(inner),
                true,
            ))),
        }
    }

    #[test]
    fn every_ontology_column_type_has_byte_counting_coverage() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        for table in generate_graph_tables(&ontology) {
            for column in &table.columns {
                let arrow_type = ddl_column_type_to_arrow(&column.data_type);
                assert!(
                    gkg_utils::arrow::has_logical_byte_size(&arrow_type),
                    "table '{}' column '{}' has DDL type {:?} (arrow {arrow_type:?}) with no \
                     logical-byte-size rule; extend the counting rules in \
                     crates/utils/src/arrow_logical_bytes.rs and bump \
                     LOGICAL_BYTE_SIZE_VERSION",
                    table.name,
                    column.name,
                    column.data_type,
                );
            }
        }
    }
}
