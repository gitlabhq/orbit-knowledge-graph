//! Typed keys read from a stage's Arrow rows, and the row filter remote checks apply.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use compiler::KeyKind;
use compiler::orbit_authorization::Key;
use query_data_model::catalog::Row;
use serde_json::Value;

use super::executor::ExecutorError;

fn missing(column: &str) -> ExecutorError {
    ExecutorError(format!("the stage returned no column {column}"))
}

fn column<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b ArrayRef, ExecutorError> {
    batch.column_by_name(name).ok_or_else(|| missing(name))
}

/// One row's value of a key column, typed as the column holds it; a null holds no key.
pub(super) fn scalar(
    array: &ArrayRef,
    row: usize,
    kind: KeyKind,
) -> Result<Option<Key>, ExecutorError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let unsupported = || ExecutorError(format!("a {} column holds no key", array.data_type()));
    Ok(Some(match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = cast(&array.slice(row, 1), &DataType::Utf8).map_err(|_| unsupported())?;
            let text = text
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(unsupported)?;
            match kind {
                KeyKind::Uuid => Key::Uuid(text.value(0).to_string()),
                _ => Key::Text(text.value(0).to_string()),
            }
        }
        DataType::FixedSizeBinary(16) if kind == KeyKind::Uuid => {
            let bytes = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .ok_or_else(unsupported)?;
            let uuid = uuid::Uuid::from_slice(bytes.value(row)).map_err(|_| unsupported())?;
            Key::Uuid(uuid.hyphenated().to_string())
        }
        data_type if data_type.is_integer() => {
            let value = cast(&array.slice(row, 1), &DataType::Int64).map_err(|_| unsupported())?;
            let value = value
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(unsupported)?;
            Key::Int(value.value(0))
        }
        _ => return Err(unsupported()),
    }))
}

/// The key of each row across `columns`, each read as its declared type: one column's value, or
/// a composite of several.
fn row_keys(
    batch: &RecordBatch,
    columns: &[String],
    types: &[query_data_model::DataType],
) -> Result<Vec<Option<Key>>, ExecutorError> {
    let arrays = columns
        .iter()
        .map(|name| column(batch, name))
        .collect::<Result<Vec<_>, _>>()?;
    (0..batch.num_rows())
        .map(|row| {
            let parts = arrays
                .iter()
                .zip(types)
                .map(|(array, data_type)| scalar(array, row, KeyKind::of(*data_type)))
                .collect::<Result<Option<Vec<Key>>, _>>()?;
            Ok(parts.map(|mut parts| match parts.len() {
                1 => parts.remove(0),
                _ => Key::Composite(parts),
            }))
        })
        .collect()
}

/// The distinct keys a stage's rows hold in `columns`, in key order.
pub fn keys(
    batches: &[RecordBatch],
    columns: &[String],
    types: &[query_data_model::DataType],
) -> Result<Vec<Key>, ExecutorError> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        keys.extend(row_keys(batch, columns, types)?.into_iter().flatten());
    }
    Ok(keys.into_iter().collect())
}

/// The distinct keys of one column, typed as a row check reads them.
pub fn checked_keys(
    batches: &[RecordBatch],
    name: &str,
    kind: KeyKind,
) -> Result<BTreeSet<Key>, ExecutorError> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        let array = column(batch, name)?;
        for row in 0..batch.num_rows() {
            keys.extend(scalar(array, row, kind)?);
        }
    }
    Ok(keys)
}

/// Keeps the rows whose key in `name` the check allows; a row without one fails closed.
pub fn retain(
    batches: Vec<RecordBatch>,
    name: &str,
    kind: KeyKind,
    allowed: &BTreeSet<Key>,
) -> Result<Vec<RecordBatch>, ExecutorError> {
    batches
        .into_iter()
        .map(|batch| {
            let array = column(&batch, name)?;
            let mask = (0..batch.num_rows())
                .map(|row| {
                    Ok(Some(
                        scalar(array, row, kind)?.is_some_and(|key| allowed.contains(&key)),
                    ))
                })
                .collect::<Result<BooleanArray, ExecutorError>>()?;
            filter_record_batch(&batch, &mask).map_err(|error| ExecutorError(error.to_string()))
        })
        .collect()
}

/// One batch of every row, under the schema of the first.
pub fn concat(batches: &[RecordBatch]) -> Result<Option<RecordBatch>, ExecutorError> {
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    concat_batches(&first.schema(), batches)
        .map(Some)
        .map_err(|error| ExecutorError(error.to_string()))
}

/// JSON rows a service returned as one batch of `fields`: integers as Int64, anything else as
/// text.
pub fn from_rows(rows: &[Row], fields: &[String]) -> Result<RecordBatch, ExecutorError> {
    let mut schema = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for field in fields {
        let values: Vec<&Value> = rows
            .iter()
            .map(|row| row.get(field).unwrap_or(&Value::Null))
            .collect();
        let integers = values.iter().all(|value| value.is_null() || value.is_i64());
        let array: ArrayRef = match integers && values.iter().any(|value| value.is_i64()) {
            true => Arc::new(
                values
                    .iter()
                    .map(|value| value.as_i64())
                    .collect::<Int64Array>(),
            ),
            false => Arc::new(
                values
                    .iter()
                    .map(|value| match value {
                        Value::Null => None,
                        Value::String(text) => Some(text.clone()),
                        other => Some(other.to_string()),
                    })
                    .collect::<StringArray>(),
            ),
        };
        schema.push(Field::new(field, array.data_type().clone(), true));
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(schema)), arrays)
        .map_err(|error| ExecutorError(error.to_string()))
}

/// Each row's value of one column as a key; a null holds none.
pub fn column_keys(batch: &RecordBatch, name: &str) -> Result<Vec<Option<Key>>, ExecutorError> {
    let array = column(batch, name)?;
    (0..batch.num_rows())
        .map(|row| scalar(array, row, KeyKind::Text))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
    use compiler::KeyKind;
    use compiler::orbit_authorization::Key;

    use super::scalar;

    const UUID: &str = "61f0c404-5cb3-11e7-907b-a6006ad3dba0";

    /// ClickHouse returns a UUID column as 16 bytes and a service returns it as text; both read
    /// as the same typed key, so the Combine stage joins them.
    #[test]
    fn a_uuid_key_reads_the_same_from_bytes_and_text() {
        let bytes = uuid::Uuid::parse_str(UUID).unwrap().into_bytes();
        let binary: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap());
        let text: ArrayRef = Arc::new(StringArray::from(vec![UUID]));
        let expected = Some(Key::Uuid(UUID.to_string()));
        assert_eq!(scalar(&binary, 0, KeyKind::Uuid).unwrap(), expected);
        assert_eq!(scalar(&text, 0, KeyKind::Uuid).unwrap(), expected);
        assert!(scalar(&binary, 0, KeyKind::Text).is_err());
    }
}
