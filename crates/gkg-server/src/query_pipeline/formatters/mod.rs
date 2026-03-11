mod raw_row;

use serde_json::{Value, json};

use crate::redaction::QueryResult;
use gkg_utils::arrow::ColumnValue;
use query_engine::ResultContext;

use super::types::QueryPipelineContext;

pub use raw_row::{RawRowFormatter, row_to_json};

pub trait ResultFormatter: Send + Sync {
    fn format(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        ctx: &QueryPipelineContext,
    ) -> Value;
}

pub(crate) fn column_value_to_json(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::Int64(v) => json!(v),
        ColumnValue::Float64(v) if v.is_finite() => json!(v),
        ColumnValue::Float64(_) => Value::Null,
        ColumnValue::String(v) => json!(v),
        ColumnValue::Null => Value::Null,
    }
}
