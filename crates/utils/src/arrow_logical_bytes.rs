use arrow::array::{
    Array, ByteView, GenericListArray, GenericStringArray, OffsetSizeTrait, StringArray,
    StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;

/// Version of the [`logical_byte_size`] counting rules; bump on any rule change.
pub const LOGICAL_SIZE_FORMULA_VERSION: u32 = 1;

/// A column's Arrow type has no [`logical_byte_size`] counting rule.
#[derive(Debug, thiserror::Error)]
#[error("column {column} has Arrow type {data_type} with no byte-counting rule")]
pub struct UncountedType {
    pub column: String,
    pub data_type: DataType,
}

/// Deterministic count of the logical bytes in a [`RecordBatch`]: a pure function of the row
/// values, invariant under batch splitting, buffer capacity, and dictionary vs. plain encoding.
/// Counts only customer data, excluding serialization overhead (offset arrays, null maps) that
/// ClickHouse's own `byteSize()` includes. An unknown Arrow type returns [`UncountedType`]
/// rather than counting as 0, so a new column type can't ship unmetered.
///
/// | Arrow logical type                                 | bytes per non-null value    |
/// |-----------------------------------------------------|------------------------------|
/// | `Int64`, `UInt64`, `Timestamp` (any unit/tz)         | 8                            |
/// | `Int32`, `UInt32`, `Date32`                          | 4                            |
/// | `Boolean`                                            | 1                            |
/// | `Utf8`, `LargeUtf8`, `Utf8View`                      | UTF-8 byte length            |
/// | `Dictionary(<int key>, Utf8)`                        | UTF-8 length of decoded value|
/// | `List`/`LargeList` of a counted type                 | sum of element counts        |
/// | NULL (any type, incl. an all-null `Null` column)     | 0                            |
/// | anything else                                        | [`UncountedType`] error      |
pub fn logical_byte_size(batch: &RecordBatch) -> Result<u64, UncountedType> {
    let schema = batch.schema();
    let mut total = 0u64;
    for (i, field) in schema.fields().iter().enumerate() {
        total += value_byte_count(field.name(), field.data_type(), batch.column(i).as_ref())?;
    }
    Ok(total)
}

pub fn is_counted(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Int64
        | DataType::UInt64
        | DataType::Timestamp(_, _)
        | DataType::Int32
        | DataType::UInt32
        | DataType::Date32
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::Dictionary(_, value_type) => value_type.as_ref() == &DataType::Utf8,
        DataType::List(field) | DataType::LargeList(field) => is_counted(field.data_type()),
        _ => false,
    }
}

fn value_byte_count(
    column: &str,
    data_type: &DataType,
    array: &dyn Array,
) -> Result<u64, UncountedType> {
    match data_type {
        DataType::Null => Ok(0),
        DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _) => {
            Ok(non_null_count(array) * 8)
        }
        DataType::Int32 | DataType::UInt32 | DataType::Date32 => Ok(non_null_count(array) * 4),
        DataType::Boolean => Ok(non_null_count(array)),
        DataType::Utf8 => Ok(utf8_payload_bytes::<i32>(array)),
        DataType::LargeUtf8 => Ok(utf8_payload_bytes::<i64>(array)),
        DataType::Utf8View => Ok(utf8_view_payload_bytes(array)),
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            dictionary_payload_bytes(column, array)
        }
        DataType::List(inner) => list_payload_bytes::<i32>(column, inner.data_type(), array),
        DataType::LargeList(inner) => list_payload_bytes::<i64>(column, inner.data_type(), array),
        _ => Err(UncountedType {
            column: column.to_string(),
            data_type: data_type.clone(),
        }),
    }
}

fn non_null_count(array: &dyn Array) -> u64 {
    (array.len() - array.null_count()) as u64
}

fn utf8_payload_bytes<O: OffsetSizeTrait>(array: &dyn Array) -> u64 {
    let arr = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .expect("DataType::Utf8/LargeUtf8 guarantees a GenericStringArray");
    let offsets = arr.offsets();
    let start = offsets.first().expect("OffsetBuffer is never empty");
    let end = offsets.last().expect("OffsetBuffer is never empty");
    (end.as_usize() - start.as_usize()) as u64
}

fn utf8_view_payload_bytes(array: &dyn Array) -> u64 {
    let arr = array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("DataType::Utf8View guarantees a StringViewArray");
    let views = arr.views();
    (0..arr.len())
        .filter(|&i| !arr.is_null(i))
        .map(|i| ByteView::from(views[i]).length as u64)
        .sum()
}

fn dictionary_payload_bytes(column: &str, array: &dyn Array) -> Result<u64, UncountedType> {
    downcast_dictionary_array!(
        array => {
            let Some(values) = array.values().as_any().downcast_ref::<StringArray>() else {
                return Err(UncountedType {
                    column: column.to_string(),
                    data_type: array.data_type().clone(),
                });
            };
            Ok(array
                .keys_iter()
                .flatten()
                .map(|key| values.value_length(key) as u64)
                .sum())
        },
        other => Err(UncountedType {
            column: column.to_string(),
            data_type: other.clone(),
        }),
    )
}

fn list_payload_bytes<O: OffsetSizeTrait>(
    column: &str,
    element_type: &DataType,
    array: &dyn Array,
) -> Result<u64, UncountedType> {
    let list = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .expect("DataType::List/LargeList guarantees a GenericListArray");
    let offsets = list.offsets();
    let start = offsets
        .first()
        .expect("OffsetBuffer is never empty")
        .as_usize();
    let end = offsets
        .last()
        .expect("OffsetBuffer is never empty")
        .as_usize();
    let values = list.values().slice(start, end - start);
    value_byte_count(column, element_type, values.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{BatchBuilder, ColumnSpec, ColumnType};
    use arrow::array::{
        ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, ListBuilder,
        NullArray, StringBuilder, StringViewArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn spec(name: &str, col_type: ColumnType, nullable: bool) -> ColumnSpec {
        ColumnSpec {
            name: name.to_string(),
            col_type,
            nullable,
        }
    }

    fn single_col_batch(name: &str, data_type: DataType, array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, data_type, true)]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    struct Row {
        int: i64,
        ts: i64,
        flag: bool,
        text: Option<&'static str>,
        code: &'static str,
        tags: &'static [&'static str],
    }

    /// Every counted type that [`BatchBuilder`] can express, with a null string and an
    /// empty list so a slice can land on a zero-byte row.
    fn builder_fixture() -> RecordBatch {
        let specs = [
            spec("int", ColumnType::Int, false),
            spec("ts", ColumnType::TimestampMicros, false),
            spec("flag", ColumnType::Bool, false),
            spec("text", ColumnType::Str, true),
            spec("code", ColumnType::DictStr, false),
            spec("tags", ColumnType::StrList, false),
        ];
        let rows = [
            Row {
                int: 1,
                ts: 100,
                flag: true,
                text: Some("a"),
                code: "x",
                tags: &["p", "q"],
            },
            Row {
                int: 2,
                ts: 200,
                flag: false,
                text: None,
                code: "y",
                tags: &[],
            },
            Row {
                int: 3,
                ts: 300,
                flag: true,
                text: Some("ccc"),
                code: "x",
                tags: &["r"],
            },
            Row {
                int: 4,
                ts: 400,
                flag: false,
                text: Some(""),
                code: "z",
                tags: &["s", "t"],
            },
        ];
        BatchBuilder::new(&specs, rows.len())
            .unwrap()
            .build(&rows, |r, b| {
                b.col("int")?.push_int(r.int)?;
                b.col("ts")?.push_timestamp_micros(r.ts)?;
                b.col("flag")?.push_bool(r.flag)?;
                b.col("text")?.push_opt_str(r.text)?;
                b.col("code")?.push_str(r.code)?;
                b.col("tags")?.push_str_list(r.tags)?;
                Ok(())
            })
            .unwrap()
    }

    /// Counted types [`BatchBuilder`] has no [`ColumnSpec`] for; built directly to keep the
    /// width-4, i64-offset, and view code paths under the split-invariance check.
    fn unbuildable_types_fixture() -> RecordBatch {
        let fields = vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("date32", DataType::Date32, false),
            Field::new("large_str", DataType::LargeUtf8, false),
            Field::new("str_view", DataType::Utf8View, false),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Date32Array::from(vec![1, 2, 3, 4])),
            Arc::new(LargeStringArray::from(vec!["e", "ff", "", "hhhh"])),
            Arc::new(StringViewArray::from(vec!["j", "kk", "", "mmmm"])),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn split_invariant_across_counted_types() {
        for batch in [builder_fixture(), unbuildable_types_fixture()] {
            let total = logical_byte_size(&batch).unwrap();
            for split in 1..batch.num_rows() {
                let left = logical_byte_size(&batch.slice(0, split)).unwrap();
                let right =
                    logical_byte_size(&batch.slice(split, batch.num_rows() - split)).unwrap();
                assert_eq!(
                    left + right,
                    total,
                    "split at row {split} changed the count"
                );
            }
        }
    }

    #[test]
    fn dictionary_counts_same_as_plain_utf8() {
        let values: &[&str] = &["alpha", "beta", "alpha", "gamma"];
        let build = |col_type| {
            BatchBuilder::new(&[spec("s", col_type, false)], values.len())
                .unwrap()
                .build(values, |v, b| b.col("s")?.push_str(v))
                .unwrap()
        };
        assert_eq!(
            logical_byte_size(&build(ColumnType::Str)).unwrap(),
            logical_byte_size(&build(ColumnType::DictStr)).unwrap()
        );
    }

    #[test]
    fn slice_counts_only_its_own_rows() {
        let batch = builder_fixture().project(&[3]).unwrap();
        let sliced = batch.slice(2, 2);
        assert_eq!(
            logical_byte_size(&sliced).unwrap(),
            "ccc".len() as u64 + "".len() as u64
        );
    }

    #[test]
    fn nulls_charge_zero() {
        let null_string = builder_fixture().project(&[3]).unwrap().slice(1, 1);
        assert_eq!(logical_byte_size(&null_string).unwrap(), 0);

        let null_int = single_col_batch(
            "n",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![None::<i64>])),
        );
        assert_eq!(logical_byte_size(&null_int).unwrap(), 0);

        let mut list_builder = ListBuilder::new(StringBuilder::new());
        list_builder.append(false);
        let null_list = single_col_batch(
            "l",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            Arc::new(list_builder.finish()),
        );
        assert_eq!(logical_byte_size(&null_list).unwrap(), 0);

        let all_null = single_col_batch("u", DataType::Null, Arc::new(NullArray::new(3)));
        assert_eq!(logical_byte_size(&all_null).unwrap(), 0);
        assert!(is_counted(&DataType::Null));
    }

    #[test]
    fn uncounted_type_names_column_and_type() {
        let batch = single_col_batch(
            "score",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        );
        let err = logical_byte_size(&batch).unwrap_err();
        assert_eq!(err.column, "score");
        assert_eq!(err.data_type, DataType::Float64);
        assert!(!is_counted(&DataType::Float64));
    }

    #[test]
    fn formula_version_is_one() {
        assert_eq!(LOGICAL_SIZE_FORMULA_VERSION, 1);
    }
}
