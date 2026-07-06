use arrow::array::types::{
    BinaryType, BinaryViewType, ByteArrayType, ByteViewType, LargeBinaryType, LargeUtf8Type,
    StringViewType, Utf8Type,
};
use arrow::array::{
    Array, ByteView, GenericByteArray, GenericByteViewArray, GenericListArray, OffsetSizeTrait,
    StringArray, new_empty_array,
};
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;

/// Version of the [`logical_byte_size`] counting rules; bump on any rule change.
pub const LOGICAL_BYTE_SIZE_VERSION: u32 = 1;

/// A column's Arrow type has no [`logical_byte_size`] counting rule.
#[derive(Debug, thiserror::Error)]
#[error("column {column} has Arrow type {data_type} with no logical-byte-size rule")]
pub struct UnsupportedTypeError {
    pub column: String,
    pub data_type: DataType,
}

/// Deterministic, split-invariant count of the customer-data bytes in a [`RecordBatch`], excluding serialization overhead; unsupported Arrow types return [`UnsupportedTypeError`] rather than counting as 0.
///
/// | Arrow logical type                                  | bytes per non-null value     |
/// |-----------------------------------------------------|------------------------------|
/// | fixed-width primitive (ints, floats, dates, times,  | its width in bytes           |
/// | timestamps, durations, intervals, decimals)         |                              |
/// | `Boolean`                                            | 1                            |
/// | `Utf8`/`LargeUtf8`/`Utf8View`, `Binary` family       | byte length                  |
/// | `Dictionary(<int key>, Utf8)`                        | byte length of decoded value |
/// | `List`/`LargeList` of a supported type               | sum of element counts        |
/// | NULL (any type, incl. an all-null `Null` column)     | 0                            |
/// | anything else (Struct, Map, Union, …)                | [`UnsupportedTypeError`]     |
pub fn logical_byte_size(batch: &RecordBatch) -> Result<u64, UnsupportedTypeError> {
    let schema = batch.schema();
    let mut total = 0u64;
    for (i, field) in schema.fields().iter().enumerate() {
        total +=
            array_logical_byte_size(field.name(), field.data_type(), batch.column(i).as_ref())?;
    }
    Ok(total)
}

/// Whether [`logical_byte_size`] has a counting rule for this Arrow type.
///
/// Probes the sizing dispatch with an empty array of the type, so this predicate
/// cannot drift from what [`logical_byte_size`] actually accepts.
pub fn has_logical_byte_size(data_type: &DataType) -> bool {
    array_logical_byte_size("", data_type, new_empty_array(data_type).as_ref()).is_ok()
}

fn array_logical_byte_size(
    column: &str,
    data_type: &DataType,
    array: &dyn Array,
) -> Result<u64, UnsupportedTypeError> {
    if let Some(width) = data_type.primitive_width() {
        return Ok(non_null_count(array) * width as u64);
    }
    match data_type {
        DataType::Null => Ok(0),
        DataType::Boolean => Ok(non_null_count(array)),
        DataType::Utf8 => Ok(byte_array_logical_bytes::<Utf8Type>(array)),
        DataType::LargeUtf8 => Ok(byte_array_logical_bytes::<LargeUtf8Type>(array)),
        DataType::Binary => Ok(byte_array_logical_bytes::<BinaryType>(array)),
        DataType::LargeBinary => Ok(byte_array_logical_bytes::<LargeBinaryType>(array)),
        DataType::Utf8View => Ok(byte_view_logical_bytes::<StringViewType>(array)),
        DataType::BinaryView => Ok(byte_view_logical_bytes::<BinaryViewType>(array)),
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            dictionary_logical_bytes(column, array)
        }
        DataType::List(inner) => list_logical_bytes::<i32>(column, inner.data_type(), array),
        DataType::LargeList(inner) => list_logical_bytes::<i64>(column, inner.data_type(), array),
        _ => Err(UnsupportedTypeError {
            column: column.to_string(),
            data_type: data_type.clone(),
        }),
    }
}

fn non_null_count(array: &dyn Array) -> u64 {
    (array.len() - array.null_count()) as u64
}

fn byte_array_logical_bytes<T: ByteArrayType>(array: &dyn Array) -> u64 {
    let arr = array
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .expect("a string/binary DataType guarantees a GenericByteArray");
    let offsets = arr.offsets();
    let start = offsets.first().expect("OffsetBuffer is never empty");
    let end = offsets.last().expect("OffsetBuffer is never empty");
    (end.as_usize() - start.as_usize()) as u64
}

fn byte_view_logical_bytes<T: ByteViewType>(array: &dyn Array) -> u64 {
    let arr = array
        .as_any()
        .downcast_ref::<GenericByteViewArray<T>>()
        .expect("a view DataType guarantees a GenericByteViewArray");
    let views = arr.views();
    (0..arr.len())
        .filter(|&i| !arr.is_null(i))
        .map(|i| ByteView::from(views[i]).length as u64)
        .sum()
}

fn dictionary_logical_bytes(column: &str, array: &dyn Array) -> Result<u64, UnsupportedTypeError> {
    downcast_dictionary_array!(
        array => {
            let Some(values) = array.values().as_any().downcast_ref::<StringArray>() else {
                return Err(UnsupportedTypeError {
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
        other => Err(UnsupportedTypeError {
            column: column.to_string(),
            data_type: other.clone(),
        }),
    )
}

fn list_logical_bytes<O: OffsetSizeTrait>(
    column: &str,
    element_type: &DataType,
    array: &dyn Array,
) -> Result<u64, UnsupportedTypeError> {
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
    array_logical_byte_size(column, element_type, values.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{BatchBuilder, ColumnSpec, ColumnType};
    use arrow::array::{
        ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, LargeStringArray, ListBuilder, NullArray, StringBuilder, StringViewArray,
        StructArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};
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
    /// primitive-width, i64-offset, binary, and view code paths under the split-invariance check.
    fn unbuildable_types_fixture() -> RecordBatch {
        let fields = vec![
            Field::new("int16", DataType::Int16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("float64", DataType::Float64, false),
            Field::new("date32", DataType::Date32, false),
            Field::new("large_str", DataType::LargeUtf8, false),
            Field::new("str_view", DataType::Utf8View, false),
            Field::new("binary", DataType::Binary, false),
            Field::new("binary_view", DataType::BinaryView, false),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            Arc::new(Date32Array::from(vec![1, 2, 3, 4])),
            Arc::new(LargeStringArray::from(vec!["e", "ff", "", "hhhh"])),
            Arc::new(StringViewArray::from(vec!["j", "kk", "", "mmmm"])),
            Arc::new(BinaryArray::from_iter_values([
                b"e".as_ref(),
                b"ff",
                b"",
                b"hhhh",
            ])),
            Arc::new(BinaryViewArray::from_iter_values([
                b"j".as_ref(),
                b"kk",
                b"",
                b"mmmm",
            ])),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn concatenated_slices_count_the_same_as_the_whole_batch() {
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
    fn dictionary_encoding_counts_identical_to_plain_utf8() {
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
    fn sliced_batch_counts_only_rows_inside_the_slice() {
        let batch = builder_fixture().project(&[3]).unwrap();
        let sliced = batch.slice(2, 2);
        assert_eq!(
            logical_byte_size(&sliced).unwrap(),
            "ccc".len() as u64 + "".len() as u64
        );
    }

    #[test]
    fn null_values_count_zero_for_string_int_and_list_columns() {
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
        assert!(has_logical_byte_size(&DataType::Null));
    }

    #[test]
    fn fixed_width_primitives_count_their_declared_width() {
        let floats = single_col_batch(
            "f",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
        );
        assert_eq!(logical_byte_size(&floats).unwrap(), 2 * 8);
        assert!(has_logical_byte_size(&DataType::Float64));

        let shorts = single_col_batch(
            "i",
            DataType::Int16,
            Arc::new(Int16Array::from(vec![1i16, 2, 3])),
        );
        assert_eq!(logical_byte_size(&shorts).unwrap(), 3 * 2);
        assert!(has_logical_byte_size(&DataType::Int16));
    }

    #[test]
    fn binary_columns_count_byte_length() {
        let batch = single_col_batch(
            "b",
            DataType::Binary,
            Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"ab"),
                None,
                Some(b"cccc"),
            ])),
        );
        assert_eq!(logical_byte_size(&batch).unwrap(), 2 + 4);
    }

    #[test]
    fn unknown_arrow_type_errors_with_column_name_and_type() {
        let struct_fields = Fields::from(vec![Field::new("x", DataType::Int64, false)]);
        let array = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
            None,
        );
        let data_type = DataType::Struct(struct_fields);
        let batch = single_col_batch("nested", data_type.clone(), Arc::new(array));
        let err = logical_byte_size(&batch).unwrap_err();
        assert_eq!(err.column, "nested");
        assert_eq!(err.data_type, data_type);
        assert!(!has_logical_byte_size(&data_type));
    }

    #[test]
    fn has_logical_byte_size_recurses_into_composite_types() {
        let utf8_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(has_logical_byte_size(&utf8_list));

        let struct_list = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::empty()),
            true,
        )));
        assert!(!has_logical_byte_size(&struct_list));

        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(has_logical_byte_size(&utf8_dict));

        let int_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64));
        assert!(!has_logical_byte_size(&int_dict));
    }

    #[test]
    fn logical_byte_size_version_is_pinned_at_one() {
        assert_eq!(LOGICAL_BYTE_SIZE_VERSION, 1);
    }
}
