//! Arrow array → ColumnValue conversion utilities.

use std::collections::HashMap;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, PartialEq, enum_as_inner::EnumAsInner)]
pub enum ColumnValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Null,
}

/// Stateless helper for extracting typed values from Arrow [`RecordBatch`]es.
///
/// All methods are associated functions — no instance required.
pub struct ArrowUtils;

impl ArrowUtils {
    /// Extract every column value from a single row of a [`RecordBatch`],
    /// keyed by the field name as it appears in the schema.
    pub fn extract_row(batch: &RecordBatch, row_idx: usize) -> HashMap<String, ColumnValue> {
        let schema = batch.schema();
        let mut map = HashMap::with_capacity(schema.fields().len());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            map.insert(
                field.name().clone(),
                Self::extract_value(batch.column(col_idx).as_ref(), row_idx),
            );
        }
        map
    }

    /// Look up a column by name and return its `i64` value at the given row,
    /// or `None` if the column is missing, not an `Int64Array`, or null.
    pub fn get_column_i64(batch: &RecordBatch, col_name: &str, row: usize) -> Option<i64> {
        let idx = batch.schema().index_of(col_name).ok()?;
        let arr = batch.column(idx).as_any().downcast_ref::<Int64Array>()?;
        if arr.is_null(row) {
            return None;
        }
        Some(arr.value(row))
    }

    /// Look up a column by name and return its `String` value at the given row,
    /// or `None` if the column is missing, not a `StringArray`, or null.
    pub fn get_column_string(batch: &RecordBatch, col_name: &str, row: usize) -> Option<String> {
        let idx = batch.schema().index_of(col_name).ok()?;
        let arr = batch.column(idx).as_any().downcast_ref::<StringArray>()?;
        if arr.is_null(row) {
            return None;
        }
        Some(arr.value(row).to_string())
    }

    /// Look up a `List<String>` column by name and collect its non-null elements
    /// at the given row. Returns an empty vec if the column is missing, not a
    /// `ListArray`, null at this row, or contains a non-`StringArray` inner type.
    pub fn get_string_list(batch: &RecordBatch, col_name: &str, row: usize) -> Vec<String> {
        let Some(idx) = batch.schema().index_of(col_name).ok() else {
            return Vec::new();
        };
        let Some(list) = batch.column(idx).as_any().downcast_ref::<ListArray>() else {
            return Vec::new();
        };
        if list.is_null(row) {
            return Vec::new();
        }
        let values = list.value(row);
        let Some(arr) = values.as_any().downcast_ref::<StringArray>() else {
            return Vec::new();
        };
        (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i).to_string())
            .collect()
    }

    /// Look up a `List<Struct<Int64, String>>` column by name and collect its
    /// non-null `(i64, String)` pairs at the given row. Returns an empty vec if the
    /// column is missing, the list is null, or the inner struct doesn't have the
    /// expected layout (at least two columns: `Int64Array` then `StringArray`).
    pub fn get_i64_string_pairs(
        batch: &RecordBatch,
        col_name: &str,
        row: usize,
    ) -> Vec<(i64, String)> {
        let Some(idx) = batch.schema().index_of(col_name).ok() else {
            return Vec::new();
        };
        let Some(list) = batch.column(idx).as_any().downcast_ref::<ListArray>() else {
            return Vec::new();
        };
        if list.is_null(row) {
            return Vec::new();
        }
        let values = list.value(row);
        let Some(structs) = values.as_any().downcast_ref::<StructArray>() else {
            return Vec::new();
        };
        if structs.num_columns() < 2 {
            return Vec::new();
        }
        let Some(ids) = structs.column(0).as_any().downcast_ref::<Int64Array>() else {
            return Vec::new();
        };
        let Some(types) = structs.column(1).as_any().downcast_ref::<StringArray>() else {
            return Vec::new();
        };
        (0..ids.len())
            .filter(|&i| !ids.is_null(i) && !types.is_null(i))
            .map(|i| (ids.value(i), types.value(i).to_string()))
            .collect()
    }

    /// Extract a typed `ColumnValue` from an Arrow array at the given row index.
    pub fn extract_value(array: &dyn Array, idx: usize) -> ColumnValue {
        if array.is_null(idx) {
            return ColumnValue::Null;
        }

        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            return ColumnValue::Int64(arr.value(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
            let val = arr.value(idx);
            return ColumnValue::Int64(i64::try_from(val).unwrap_or(i64::MAX));
        }

        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            return ColumnValue::String(arr.value(idx).to_string());
        }

        if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            return ColumnValue::Float64(arr.value(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }

        if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            return ColumnValue::String(arr.value(idx).to_string());
        }

        ColumnValue::Null
    }
}

fn timestamp_to_string(dt: Option<chrono::NaiveDateTime>) -> ColumnValue {
    dt.map(|d| ColumnValue::String(d.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
        .unwrap_or(ColumnValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Builder, ListBuilder, StringBuilder, StructBuilder, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    /// Build a `List<Struct<Int64, Utf8>>` column with the given rows.
    /// Each row is a slice of `(i64, &str)` pairs; `None` produces a null list entry.
    fn make_i64_string_list(rows: &[Option<&[(i64, &str)]>]) -> ListArray {
        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let mut builder = ListBuilder::new(StructBuilder::new(
            fields,
            vec![
                Box::new(Int64Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        ));
        for row in rows {
            match row {
                Some(pairs) => {
                    for &(id, s) in *pairs {
                        builder
                            .values()
                            .field_builder::<Int64Builder>(0)
                            .unwrap()
                            .append_value(id);
                        builder
                            .values()
                            .field_builder::<StringBuilder>(1)
                            .unwrap()
                            .append_value(s);
                        builder.values().append(true);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    fn assert_ts(arr: Arc<dyn Array>) {
        let batch = make_batch(vec![("ts", arr)]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("ts"),
            Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string())),
        );
    }

    // -- ColumnValue enum --

    #[test]
    fn column_value_accessors() {
        let i = ColumnValue::Int64(42);
        assert_eq!(i.as_int64().copied(), Some(42));
        assert!(i.as_string().is_none());

        let s = ColumnValue::String("hello".into());
        assert_eq!(s.as_string().map(|s| s.as_str()), Some("hello"));
        assert!(s.as_int64().is_none());

        let n = ColumnValue::Null;
        assert!(n.as_int64().is_none());
        assert!(n.as_string().is_none());
    }

    #[test]
    fn column_value_equality() {
        assert_eq!(ColumnValue::Int64(1), ColumnValue::Int64(1));
        assert_ne!(ColumnValue::Int64(1), ColumnValue::Int64(2));
        assert_eq!(
            ColumnValue::String("a".into()),
            ColumnValue::String("a".into())
        );
        assert_ne!(ColumnValue::Null, ColumnValue::Int64(0));
    }

    // -- extract_value / extract_row --

    #[test]
    fn extract_row_returns_all_columns() {
        let batch = make_batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("name", Arc::new(StringArray::from(vec!["alice"]))),
        ]);
        let row = ArrowUtils::extract_row(&batch, 0);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("id"), Some(&ColumnValue::Int64(1)));
        assert_eq!(row.get("name"), Some(&ColumnValue::String("alice".into())));
    }

    #[test]
    fn extract_uint64_as_int64() {
        let batch = make_batch(vec![(
            "n",
            Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
        )]);
        for (i, expected) in [100, 200, 300].iter().enumerate() {
            assert_eq!(
                ArrowUtils::extract_row(&batch, i).get("n"),
                Some(&ColumnValue::Int64(*expected)),
            );
        }
    }

    #[test]
    fn extract_uint64_overflow_clamps_to_max() {
        let batch = make_batch(vec![("big", Arc::new(UInt64Array::from(vec![u64::MAX])))]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("big"),
            Some(&ColumnValue::Int64(i64::MAX)),
        );
    }

    #[test]
    fn extract_all_timestamp_precisions() {
        // 2024-01-01T00:00:00Z at each resolution
        assert_ts(Arc::new(TimestampSecondArray::new(
            vec![1_704_067_200].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampMillisecondArray::new(
            vec![1_704_067_200_000].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampMicrosecondArray::new(
            vec![1_704_067_200_000_000].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampNanosecondArray::new(
            vec![1_704_067_200_000_000_000].into(),
            None,
        )));
    }

    #[test]
    fn extract_null_timestamp_returns_null() {
        let arr: TimestampSecondArray = vec![Some(1_704_067_200i64), None].into_iter().collect();
        let batch = make_batch(vec![("ts", Arc::new(arr))]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("ts"),
            Some(&ColumnValue::String("2024-01-01T00:00:00Z".into())),
        );
        assert_eq!(
            ArrowUtils::extract_row(&batch, 1).get("ts"),
            Some(&ColumnValue::Null)
        );
    }

    // -- typed column getters --

    #[test]
    fn get_column_i64_and_string() {
        let batch = make_batch(vec![
            ("id", Arc::new(Int64Array::from(vec![42]))),
            ("name", Arc::new(StringArray::from(vec!["bob"]))),
        ]);
        assert_eq!(ArrowUtils::get_column_i64(&batch, "id", 0), Some(42));
        assert_eq!(
            ArrowUtils::get_column_string(&batch, "name", 0),
            Some("bob".into())
        );
        assert_eq!(ArrowUtils::get_column_i64(&batch, "missing", 0), None);
        assert_eq!(ArrowUtils::get_column_string(&batch, "missing", 0), None);
    }

    #[test]
    fn get_column_null_returns_none() {
        let batch = make_batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![Option::<i64>::None])),
        )]);
        assert_eq!(ArrowUtils::get_column_i64(&batch, "id", 0), None);
    }

    // -- list column getters --

    #[test]
    fn get_string_list_returns_values() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);

        let batch = make_batch(vec![("tags", Arc::new(builder.finish()))]);
        assert_eq!(
            ArrowUtils::get_string_list(&batch, "tags", 0),
            vec!["a", "b"]
        );
        assert!(ArrowUtils::get_string_list(&batch, "missing", 0).is_empty());
    }

    #[test]
    fn get_i64_string_pairs_returns_pairs() {
        let list = make_i64_string_list(&[Some(&[(10, "User"), (20, "Project")]), None]);
        let batch = make_batch(vec![("path", Arc::new(list))]);

        assert_eq!(
            ArrowUtils::get_i64_string_pairs(&batch, "path", 0),
            vec![(10, "User".into()), (20, "Project".into())],
        );
        assert!(ArrowUtils::get_i64_string_pairs(&batch, "path", 1).is_empty());
        assert!(ArrowUtils::get_i64_string_pairs(&batch, "missing", 0).is_empty());
    }
}
