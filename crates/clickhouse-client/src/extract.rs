use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, thiserror::Error)]
#[error("invalid column type: expected {expected}")]
pub struct ExtractError {
    pub expected: &'static str,
}

pub trait FromArrowColumn: Sized {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, ExtractError>;
}

impl FromArrowColumn for bool {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, ExtractError> {
        let mut values = Vec::new();

        for batch in batches {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or(ExtractError {
                    expected: "BooleanArray",
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

impl FromArrowColumn for String {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, ExtractError> {
        let mut values = Vec::new();

        for batch in batches {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(ExtractError {
                    expected: "StringArray",
                })?;

            for i in 0..column.len() {
                if !column.is_null(i) {
                    values.push(column.value(i).to_string());
                }
            }
        }

        Ok(values)
    }
}

impl FromArrowColumn for i64 {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, ExtractError> {
        let mut values = Vec::new();

        for batch in batches {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExtractError {
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

impl FromArrowColumn for DateTime<Utc> {
    fn extract_column(
        batches: &[RecordBatch],
        column_index: usize,
    ) -> Result<Vec<Self>, ExtractError> {
        let mut values = Vec::new();

        for batch in batches {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or(ExtractError {
                    expected: "TimestampMicrosecondArray",
                })?;

            for i in 0..column.len() {
                if column.is_null(i) {
                    continue;
                }
                let micros = column.value(i);
                let timestamp = Utc.timestamp_micros(micros).single().ok_or(ExtractError {
                    expected: "valid microsecond timestamp",
                })?;
                values.push(timestamp);
            }
        }

        Ok(values)
    }
}
