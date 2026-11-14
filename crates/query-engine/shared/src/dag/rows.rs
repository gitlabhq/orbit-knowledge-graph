//! Stage rows in their contract: wire rows decoded into contract order and types, the typed keys
//! read at contract positions, and the row filter remote checks apply.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray,
};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use compiler::KeyKind;
use compiler::dag::{ColumnType, Contract};
use compiler::orbit_authorization::Key;
use query_data_model::DataType;
use query_data_model::catalog::Row;
use serde_json::Value;

use super::executor::ExecutorError;

fn failed(error: impl std::fmt::Display) -> ExecutorError {
    ExecutorError(error.to_string())
}

fn column(batch: &RecordBatch, position: usize) -> Result<&ArrayRef, ExecutorError> {
    batch.columns().get(position).ok_or_else(|| {
        ExecutorError(format!(
            "the stage returned {} columns, none at position {position}",
            batch.num_columns()
        ))
    })
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
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => {
            let text = cast(&array.slice(row, 1), &ArrowType::Utf8).map_err(|_| unsupported())?;
            let text = text
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(unsupported)?;
            match kind {
                KeyKind::Uuid => Key::Uuid(text.value(0).to_string()),
                _ => Key::Text(text.value(0).to_string()),
            }
        }
        ArrowType::FixedSizeBinary(16) if kind == KeyKind::Uuid => {
            let bytes = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(unsupported)?;
            let uuid = uuid::Uuid::from_slice(bytes.value(row)).map_err(|_| unsupported())?;
            Key::Uuid(uuid.hyphenated().to_string())
        }
        data_type if data_type.is_integer() => {
            let value = cast(&array.slice(row, 1), &ArrowType::Int64).map_err(|_| unsupported())?;
            let value = value
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(unsupported)?;
            Key::Int(value.value(0))
        }
        _ => return Err(unsupported()),
    }))
}

/// The key of each row across the columns at `positions`, each read as its declared type: one
/// column's value, or a composite of several.
fn row_keys(
    batch: &RecordBatch,
    positions: &[usize],
    types: &[DataType],
) -> Result<Vec<Option<Key>>, ExecutorError> {
    let arrays = positions
        .iter()
        .map(|position| column(batch, *position))
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

/// The distinct keys a stage's rows hold at `positions`, in key order.
pub fn keys(
    batches: &[RecordBatch],
    positions: &[usize],
    types: &[DataType],
) -> Result<Vec<Key>, ExecutorError> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        keys.extend(row_keys(batch, positions, types)?.into_iter().flatten());
    }
    Ok(keys.into_iter().collect())
}

/// The distinct keys of one column, typed as a row check reads them.
pub fn checked_keys(
    batches: &[RecordBatch],
    position: usize,
    kind: KeyKind,
) -> Result<BTreeSet<Key>, ExecutorError> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        let array = column(batch, position)?;
        for row in 0..batch.num_rows() {
            keys.extend(scalar(array, row, kind)?);
        }
    }
    Ok(keys)
}

/// Keeps the rows whose key at `position` the check allows; a row without one fails closed.
pub fn retain(
    batches: Vec<RecordBatch>,
    position: usize,
    kind: KeyKind,
    allowed: &BTreeSet<Key>,
) -> Result<Vec<RecordBatch>, ExecutorError> {
    batches
        .into_iter()
        .map(|batch| {
            let array = column(&batch, position)?;
            let mask = (0..batch.num_rows())
                .map(|row| {
                    Ok(Some(
                        scalar(array, row, kind)?.is_some_and(|key| allowed.contains(&key)),
                    ))
                })
                .collect::<Result<BooleanArray, ExecutorError>>()?;
            filter_record_batch(&batch, &mask).map_err(failed)
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
        .map_err(failed)
}

/// Each row's key at `position`, read as text or integer; a null holds none.
pub fn column_keys(
    batch: &RecordBatch,
    position: usize,
) -> Result<Vec<Option<Key>>, ExecutorError> {
    let array = column(batch, position)?;
    (0..batch.num_rows())
        .map(|row| scalar(array, row, KeyKind::Text))
        .collect()
}

/// The Arrow type a contract column decodes into; a path keeps the type its backend returns.
fn arrow_type(data_type: ColumnType) -> Option<ArrowType> {
    let ColumnType::Scalar(data_type) = data_type else {
        return None;
    };
    Some(match data_type {
        DataType::Int => ArrowType::Int64,
        DataType::Float => ArrowType::Float64,
        DataType::Bool => ArrowType::Boolean,
        DataType::String | DataType::Enum | DataType::Uuid => ArrowType::Utf8,
        DataType::Date => ArrowType::Date32,
        DataType::DateTime => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    })
}

/// Columns named by their contract position, which nothing reads them by.
fn schema(arrays: &[ArrayRef]) -> SchemaRef {
    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(position, array)| Field::new(position.to_string(), array.data_type().clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// One array cast to the type its contract column declares.
fn decoded(array: &ArrayRef, data_type: ColumnType) -> Result<ArrayRef, ExecutorError> {
    let Some(target) = arrow_type(data_type) else {
        return Ok(Arc::clone(array));
    };
    if array.data_type() == &target {
        return Ok(Arc::clone(array));
    }
    if let (ArrowType::FixedSizeBinary(16), ColumnType::Scalar(DataType::Uuid)) =
        (array.data_type(), data_type)
    {
        let bytes = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| ExecutorError("a uuid column holds 16 bytes per row".into()))?;
        let text = (0..bytes.len())
            .map(|row| {
                (!bytes.is_null(row))
                    .then(|| uuid::Uuid::from_slice(bytes.value(row)).map(|id| id.to_string()))
                    .transpose()
            })
            .collect::<Result<StringArray, _>>()
            .map_err(failed)?;
        return Ok(Arc::new(text));
    }
    cast(array, &target).map_err(failed)
}

/// Arrow rows a relational backend returned, as batches in contract order with contract types.
pub fn decode(
    batches: Vec<RecordBatch>,
    contract: &Contract,
) -> Result<Vec<RecordBatch>, ExecutorError> {
    batches
        .into_iter()
        .map(|batch| {
            if batch.num_columns() != contract.columns.len() {
                return Err(ExecutorError(format!(
                    "the stage returned {} columns where its contract lists {}",
                    batch.num_columns(),
                    contract.columns.len()
                )));
            }
            let arrays = batch
                .columns()
                .iter()
                .zip(&contract.columns)
                .map(|(array, column)| decoded(array, column.data_type))
                .collect::<Result<Vec<_>, _>>()?;
            RecordBatch::try_new(schema(&arrays), arrays).map_err(failed)
        })
        .collect()
}

/// JSON rows a service returned, read field by field in contract order, as one batch with
/// contract types.
pub fn from_rows(
    rows: &[Row],
    fields: &[String],
    contract: &Contract,
) -> Result<RecordBatch, ExecutorError> {
    if fields.len() != contract.columns.len() {
        return Err(ExecutorError(format!(
            "the request names {} fields where its contract lists {}",
            fields.len(),
            contract.columns.len()
        )));
    }
    let arrays = fields
        .iter()
        .zip(&contract.columns)
        .map(|(field, column)| {
            let values: Vec<&Value> = rows
                .iter()
                .map(|row| row.get(field).unwrap_or(&Value::Null))
                .collect();
            json_array(field, &values, column.data_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema(&arrays), arrays).map_err(failed)
}

fn json_array(
    field: &str,
    values: &[&Value],
    data_type: ColumnType,
) -> Result<ArrayRef, ExecutorError> {
    let ColumnType::Scalar(data_type) = data_type else {
        return Err(ExecutorError(format!(
            "a service returns no path in {field}"
        )));
    };
    let mistyped = || ExecutorError(format!("{field} holds no {data_type} value"));
    let text = || -> ArrayRef {
        Arc::new(
            values
                .iter()
                .map(|value| match value {
                    Value::Null => None,
                    Value::String(text) => Some(text.clone()),
                    other => Some(other.to_string()),
                })
                .collect::<StringArray>(),
        )
    };
    Ok(match data_type {
        DataType::Int => Arc::new(
            values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    value => value.as_i64().map(Some).ok_or_else(mistyped),
                })
                .collect::<Result<Int64Array, _>>()?,
        ),
        DataType::Float => Arc::new(
            values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    value => value.as_f64().map(Some).ok_or_else(mistyped),
                })
                .collect::<Result<Float64Array, _>>()?,
        ),
        DataType::Bool => Arc::new(
            values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    value => value.as_bool().map(Some).ok_or_else(mistyped),
                })
                .collect::<Result<BooleanArray, _>>()?,
        ),
        DataType::String | DataType::Enum | DataType::Uuid => text(),
        DataType::Date | DataType::DateTime => decoded(&text(), ColumnType::Scalar(data_type))?,
    })
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
