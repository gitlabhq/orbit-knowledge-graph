//! Arrow result decoding shared by the observability lookups.

use std::collections::HashMap;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;

/// Reads a two-column `(key, value)` result into a map. Both columns are
/// projected as `String` by the lookup queries so one decoder serves all of
/// them; typed conversion happens per lookup.
pub fn two_string_columns(batches: &[RecordBatch]) -> HashMap<String, String> {
    let mut rows = HashMap::new();

    for batch in batches {
        if batch.num_columns() < 2 {
            continue;
        }

        let (Some(keys), Some(values)) = (
            batch.column(0).as_any().downcast_ref::<StringArray>(),
            batch.column(1).as_any().downcast_ref::<StringArray>(),
        ) else {
            continue;
        };

        for index in 0..batch.num_rows() {
            if keys.is_null(index) || values.is_null(index) {
                continue;
            }
            rows.insert(
                keys.value(index).to_string(),
                values.value(index).to_string(),
            );
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(keys: Vec<Option<&str>>, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn pairs_are_read_into_a_map() {
        let rows = two_string_columns(&[batch(
            vec![Some("1"), Some("2")],
            vec![Some("abc"), Some("def")],
        )]);

        assert_eq!(rows.get("1"), Some(&"abc".to_string()));
        assert_eq!(rows.get("2"), Some(&"def".to_string()));
    }

    #[test]
    fn rows_from_several_batches_are_merged() {
        let rows = two_string_columns(&[
            batch(vec![Some("1")], vec![Some("abc")]),
            batch(vec![Some("2")], vec![Some("def")]),
        ]);

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn null_keys_and_values_are_skipped() {
        let rows = two_string_columns(&[batch(
            vec![Some("1"), None, Some("3")],
            vec![Some("abc"), Some("def"), None],
        )]);

        assert_eq!(rows.len(), 1);
        assert!(rows.contains_key("1"));
    }

    #[test]
    fn no_batches_yield_no_rows() {
        assert!(two_string_columns(&[]).is_empty());
    }
}
