mod goon;
mod graph;
mod raw_row;

use std::sync::LazyLock;

use semver::Version;
use serde_json::{Value, json};

use gkg_utils::arrow::ColumnValue;
use shared::PipelineOutput;

pub use goon::GoonFormatter;
pub use graph::{
    ColumnDescriptor, GraphEdge, GraphFormatter, GraphNode, GraphResponse, PaginationResponse,
};
pub use raw_row::row_to_json;

pub static RAW_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    include_str!(concat!(env!("CONFIG_DIR"), "/RAW_OUTPUT_FORMAT_VERSION"))
        .trim()
        .parse()
        .expect("RAW_OUTPUT_FORMAT_VERSION must be valid semver")
});

/// Concrete encoding of a formatter's output. Mirrors the proto `FormatName`
/// enum but lives here so the formatters crate stays proto-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatName {
    Raw,
    Goon,
}

impl FormatName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Goon => "goon",
        }
    }
}

pub trait ResultFormatter: Send + Sync {
    fn format_name(&self) -> FormatName;
    /// `None` for stubs that have not yet defined their own version
    /// (e.g. `GoonFormatter` before ADR 009 ships).
    fn format_version(&self) -> Option<&Version>;
    fn format(&self, output: &PipelineOutput) -> Value;
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
