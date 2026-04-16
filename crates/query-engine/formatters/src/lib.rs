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

    /// Format the output and return it alongside the stamped version string
    /// and format name. Callers use this to build transport metadata without
    /// re-querying the trait for each field separately.
    fn format_stamped(&self, output: &PipelineOutput) -> (Value, String, FormatName) {
        let formatted = self.format(output);
        let version = self
            .format_version()
            .map(|v| v.to_string())
            .unwrap_or_default();
        (formatted, version, self.format_name())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_response_schema_id_major_matches_raw_output_format_version() {
        // The `$id` in crates/gkg-server/schemas/query_response.json ends with
        // `/vN` where N is the major component of RAW_OUTPUT_FORMAT_VERSION.
        // Guards against the two drifting silently when the semver major bumps.
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("GKG_SERVER_SCHEMAS_DIR"),
            "/query_response.json"
        )))
        .expect("query_response.json must be valid JSON");

        let id = schema
            .get("$id")
            .and_then(Value::as_str)
            .expect("query_response.json must declare $id");

        let id_major: u64 = id
            .rsplit('/')
            .next()
            .and_then(|seg| seg.strip_prefix('v'))
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("$id '{id}' must end with /vN"));

        assert_eq!(
            id_major, RAW_OUTPUT_FORMAT_VERSION.major,
            "query_response.json $id '{id}' does not match RAW_OUTPUT_FORMAT_VERSION major ({})",
            RAW_OUTPUT_FORMAT_VERSION.major,
        );
    }
}
