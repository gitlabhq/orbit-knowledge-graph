use arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;

pub fn get_string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} should be StringArray"))
}

pub fn get_uint64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a UInt64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("{name} should be UInt64Array"))
}

pub fn get_int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} should be Int64Array"))
}

pub fn get_boolean_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("{name} should be BooleanArray"))
}
