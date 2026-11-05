use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray, LargeStringArray, StringArray};
use arrow::record_batch::RecordBatch;

/// Reads a two-column `(key, value)` result into a map.
///
/// ClickHouse does not render every text column as Arrow `Utf8`: a
/// `FixedString(32)` such as `trace_id` arrives as `FixedSizeBinary`, which a
/// plain `StringArray` downcast silently misses and turns into an empty result.
pub fn two_string_columns(batches: &[RecordBatch]) -> HashMap<String, String> {
    let mut rows = HashMap::new();

    for batch in batches {
        if batch.num_columns() < 2 {
            continue;
        }

        for index in 0..batch.num_rows() {
            let (Some(key), Some(value)) = (text_at(batch, 0, index), text_at(batch, 1, index))
            else {
                continue;
            };
            rows.insert(key, value);
        }
    }

    rows
}

fn text_at(batch: &RecordBatch, column: usize, index: usize) -> Option<String> {
    let column = batch.column(column);
    if column.is_null(index) {
        return None;
    }

    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(index).to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(index).to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return String::from_utf8(values.value(index).to_vec()).ok();
    }

    None
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

    #[test]
    fn a_fixed_size_binary_value_is_read_as_text() {
        let schema = Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, true),
            Field::new("value", DataType::FixedSizeBinary(4), true),
        ]);
        let trace_ids =
            FixedSizeBinaryArray::try_from_iter(vec![b"abcd".to_vec()].into_iter()).expect("array");
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1")])),
                Arc::new(trace_ids),
            ],
        )
        .expect("batch");

        let rows = two_string_columns(&[batch]);

        assert_eq!(rows.get("1"), Some(&"abcd".to_string()));
    }
}
