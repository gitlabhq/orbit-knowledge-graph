use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;

use super::DispatcherError;

pub trait FromArrowColumn: Sized {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, DispatcherError>;
}

impl FromArrowColumn for i64 {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, DispatcherError> {
        let mut values = Vec::new();

        for batch in batches {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(DispatcherError::InvalidColumnType {
                    expected: "Int64Array",
                })?;

            for i in 0..column.len() {
                if !column.is_null(i) {
                    values.push(column.value(i));
                }
            }
        }

        Ok(values)
    }
}
