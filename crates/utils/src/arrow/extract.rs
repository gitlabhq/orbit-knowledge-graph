use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeStringArray, ListArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, thiserror::Error)]
pub enum CellError {
    #[error("expected {expected}, got {actual}")]
    TypeMismatch {
        expected: &'static str,
        actual: String,
    },

    #[error("{0}")]
    InvalidValue(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ArrowExtractError {
    #[error("column '{column}' is missing")]
    MissingColumn { column: String },

    #[error("column '{column}' row {row}: value is null")]
    UnexpectedNull { column: String, row: usize },

    #[error("column '{column}' row {row}: {source}")]
    Cell {
        column: String,
        row: usize,
        #[source]
        source: CellError,
    },
}

pub trait FromArrowArray: Sized {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError>;
}

pub trait FromRecordBatch: Sized {
    fn from_batches(batches: &[RecordBatch]) -> Result<Vec<Self>, ArrowExtractError>;
}

pub use gkg_utils_derive::FromRecordBatch;

impl FromArrowArray for String {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        Err(CellError::TypeMismatch {
            expected: "StringArray or LargeStringArray",
            actual: format!("{:?}", array.data_type()),
        })
    }
}

impl FromArrowArray for i64 {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        let arr =
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CellError::TypeMismatch {
                    expected: "Int64Array",
                    actual: format!("{:?}", array.data_type()),
                })?;
        Ok(Some(arr.value(row)))
    }
}

impl FromArrowArray for bool {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        let arr = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| CellError::TypeMismatch {
                expected: "BooleanArray",
                actual: format!("{:?}", array.data_type()),
            })?;
        Ok(Some(arr.value(row)))
    }
}

impl FromArrowArray for DateTime<Utc> {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            return Utc
                .timestamp_micros(arr.value(row))
                .single()
                .map(Some)
                .ok_or_else(|| {
                    CellError::InvalidValue(format!(
                        "invalid microsecond timestamp: {}",
                        arr.value(row)
                    ))
                });
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
            return Utc
                .timestamp_opt(arr.value(row), 0)
                .single()
                .map(Some)
                .ok_or_else(|| {
                    CellError::InvalidValue(format!("invalid second timestamp: {}", arr.value(row)))
                });
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
            return Utc
                .timestamp_millis_opt(arr.value(row))
                .single()
                .map(Some)
                .ok_or_else(|| {
                    CellError::InvalidValue(format!(
                        "invalid millisecond timestamp: {}",
                        arr.value(row)
                    ))
                });
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            let nanos = arr.value(row);
            let secs = nanos / 1_000_000_000;
            let nsecs = (nanos % 1_000_000_000) as u32;
            return Utc
                .timestamp_opt(secs, nsecs)
                .single()
                .map(Some)
                .ok_or_else(|| {
                    CellError::InvalidValue(format!("invalid nanosecond timestamp: {nanos}"))
                });
        }
        Err(CellError::TypeMismatch {
            expected: "TimestampArray (any precision)",
            actual: format!("{:?}", array.data_type()),
        })
    }
}

impl FromArrowArray for Vec<String> {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CellError::TypeMismatch {
                    expected: "ListArray",
                    actual: format!("{:?}", array.data_type()),
                })?;
        let values = list.value(row);
        let arr = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CellError::TypeMismatch {
                expected: "ListArray<StringArray>",
                actual: format!("{:?}", array.data_type()),
            })?;
        Ok(Some(
            (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i).to_string())
                .collect(),
        ))
    }
}

impl FromArrowArray for Vec<(i64, String)> {
    fn from_arrow_array(array: &ArrayRef, row: usize) -> Result<Option<Self>, CellError> {
        if array.is_null(row) {
            return Ok(None);
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CellError::TypeMismatch {
                    expected: "ListArray",
                    actual: format!("{:?}", array.data_type()),
                })?;
        let values = list.value(row);
        let structs = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| CellError::TypeMismatch {
                expected: "ListArray<StructArray<Int64, String>>",
                actual: format!("{:?}", array.data_type()),
            })?;
        if structs.num_columns() < 2 {
            return Err(CellError::TypeMismatch {
                expected: "StructArray with at least 2 columns",
                actual: format!("StructArray with {} columns", structs.num_columns()),
            });
        }
        let ids = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| CellError::TypeMismatch {
                expected: "Int64Array as first struct field",
                actual: format!("{:?}", structs.column(0).data_type()),
            })?;
        let strings = structs
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CellError::TypeMismatch {
                expected: "StringArray as second struct field",
                actual: format!("{:?}", structs.column(1).data_type()),
            })?;
        Ok(Some(
            (0..ids.len())
                .filter(|&i| !ids.is_null(i) && !strings.is_null(i))
                .map(|i| (ids.value(i), strings.value(i).to_string()))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Builder, ListBuilder, StringBuilder, StructBuilder, TimestampMicrosecondArray,
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

    fn make_tags_column(rows: &[Option<&[&str]>]) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for v in *values {
                        builder.values().append_value(v);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    fn make_pairs_column(rows: &[Option<&[(i64, &str)]>]) -> ListArray {
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

    #[test]
    fn large_string_array_fallback() {
        let batch = make_batch(vec![("s", Arc::new(LargeStringArray::from(vec!["large"])))]);
        let col = batch.column_by_name("s").unwrap();
        assert_eq!(
            String::from_arrow_array(col, 0).unwrap(),
            Some("large".to_string())
        );
    }

    #[test]
    fn type_mismatch_returns_error() {
        let batch = make_batch(vec![("s", Arc::new(Int64Array::from(vec![42])))]);
        let col = batch.column_by_name("s").unwrap();
        assert!(String::from_arrow_array(col, 0).is_err());
    }

    #[derive(FromRecordBatch, Debug, PartialEq)]
    struct FullRow {
        name: String,
        count: i64,
        active: bool,
        created_at: DateTime<Utc>,
        tags: Vec<String>,
        pairs: Vec<(i64, String)>,
        #[arrow(column = "display_name")]
        label: Option<String>,
    }

    #[test]
    fn derive_all_types() {
        let micros = 1_704_067_200_000_000i64; // 2024-01-01T00:00:00Z
        let batch = make_batch(vec![
            ("name", Arc::new(StringArray::from(vec!["alice", "bob"]))),
            ("count", Arc::new(Int64Array::from(vec![10, 20]))),
            ("active", Arc::new(BooleanArray::from(vec![true, false]))),
            (
                "created_at",
                Arc::new(TimestampMicrosecondArray::new(
                    vec![micros, micros].into(),
                    None,
                )),
            ),
            (
                "tags",
                Arc::new(make_tags_column(&[Some(&["a", "b"]), Some(&[])])),
            ),
            (
                "pairs",
                Arc::new(make_pairs_column(&[Some(&[(1, "User")]), Some(&[])])),
            ),
            (
                "display_name",
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
            ),
        ]);
        let rows = FullRow::from_batches(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "alice");
        assert_eq!(rows[0].count, 10);
        assert!(rows[0].active);
        assert_eq!(rows[0].created_at.to_rfc3339(), "2024-01-01T00:00:00+00:00");
        assert_eq!(rows[0].tags, vec!["a", "b"]);
        assert_eq!(rows[0].pairs, vec![(1, "User".to_string())]);
        assert_eq!(rows[0].label, Some("Alice".to_string()));
        assert!(!rows[1].active);
        assert!(rows[1].tags.is_empty());
        assert!(rows[1].pairs.is_empty());
        assert_eq!(rows[1].label, None);
    }

    #[derive(FromRecordBatch, Debug, PartialEq)]
    struct SimpleRow {
        name: String,
        count: i64,
        label: Option<String>,
    }

    #[test]
    fn optional_column_missing_yields_none() {
        let batch = make_batch(vec![
            ("name", Arc::new(StringArray::from(vec!["alice"]))),
            ("count", Arc::new(Int64Array::from(vec![10]))),
        ]);
        let rows = SimpleRow::from_batches(&[batch]).unwrap();
        assert_eq!(rows[0].label, None);
    }

    #[test]
    fn required_column_missing_errors() {
        let batch = make_batch(vec![("count", Arc::new(Int64Array::from(vec![10])))]);
        let err = SimpleRow::from_batches(&[batch]).unwrap_err();
        assert!(matches!(err, ArrowExtractError::MissingColumn { .. }));
    }

    #[test]
    fn null_on_required_field_errors() {
        let batch = make_batch(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ),
            ("count", Arc::new(Int64Array::from(vec![10]))),
        ]);
        let err = SimpleRow::from_batches(&[batch]).unwrap_err();
        assert!(matches!(err, ArrowExtractError::UnexpectedNull { .. }));
    }
}
