mod goon;
mod graph;
mod raw_row;

use serde_json::{Value, json};

use gkg_utils::arrow::ColumnValue;
use query_engine::ResultContext;
use querying_types::QueryResult;

pub use goon::GoonFormatter;
pub use graph::{GraphEdge, GraphFormatter, GraphNode, GraphResponse};
pub use raw_row::row_to_json;

pub trait ResultFormatter: Send + Sync {
    fn format(&self, result: &QueryResult, result_context: &ResultContext) -> Value;
}

pub fn column_value_to_json(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::Int64(v) => json!(v),
        ColumnValue::Float64(v) if v.is_finite() => json!(v),
        ColumnValue::Float64(_) => Value::Null,
        ColumnValue::String(v) => json!(v),
        ColumnValue::Null => Value::Null,
    }
}
