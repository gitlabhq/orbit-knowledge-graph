#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    fn clickhouse_type_string_to_arrow(ch_type: &str) -> DataType {
        let trimmed = ch_type.trim();
        if let Some(inner) = strip_wrapper(trimmed, "Nullable") {
            return clickhouse_type_string_to_arrow(inner);
        }
        if let Some(inner) = strip_wrapper(trimmed, "LowCardinality") {
            return clickhouse_type_string_to_arrow(inner);
        }
        if let Some(inner) = strip_wrapper(trimmed, "Array") {
            return DataType::List(Arc::new(Field::new(
                "item",
                clickhouse_type_string_to_arrow(inner),
                true,
            )));
        }
        match trimmed {
            "String" => DataType::Utf8,
            "Int64" => DataType::Int64,
            "UInt64" => DataType::UInt64,
            "UInt32" => DataType::UInt32,
            "Bool" => DataType::Boolean,
            "Date32" => DataType::Date32,
            "DateTime" => DataType::Timestamp(TimeUnit::Microsecond, None),
            t if t.starts_with("DateTime64") => DataType::Timestamp(TimeUnit::Microsecond, None),
            t if t.starts_with("Enum8") => DataType::Utf8,
            _ => DataType::Utf8,
        }
    }

    fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
        if s.starts_with(prefix) && s.ends_with(')') {
            Some(&s[prefix.len() + 1..s.len() - 1])
        } else {
            None
        }
    }

    #[test]
    fn every_ontology_column_type_has_byte_counting_coverage() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let schema = orbit_migrations::schema::GraphSchema::from_ontology(&ontology);
        for table in &schema.tables {
            for column in &table.columns {
                let arrow_type = clickhouse_type_string_to_arrow(&column.column_type);
                assert!(
                    orbit_utils::arrow::has_logical_byte_size(&arrow_type),
                    "table '{}' column '{}' has type '{}' (arrow {arrow_type:?}) with no \
                     logical-byte-size rule; extend the counting rules in \
                     crates/utils/src/arrow_logical_bytes.rs",
                    table.name,
                    column.name,
                    column.column_type,
                );
            }
        }
    }
}
