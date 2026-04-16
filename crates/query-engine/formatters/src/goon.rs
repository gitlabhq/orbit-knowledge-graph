use semver::Version;
use serde_json::Value;
use shared::PipelineOutput;

use super::ResultFormatter;
use super::graph::GraphFormatter;

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format_name(&self) -> &'static str {
        "goon"
    }

    fn format_version(&self) -> &Version {
        // Stub: reports RAW version while delegating to GraphFormatter.
        // A follow-up MR will add config/GOON_OUTPUT_FORMAT_VERSION and
        // the actual GOON encoding (ADR 009).
        &super::RAW_OUTPUT_FORMAT_VERSION
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        GraphFormatter.format(output)
    }
}
