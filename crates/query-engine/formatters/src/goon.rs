use semver::Version;
use serde_json::Value;
use shared::PipelineOutput;

use super::graph::GraphFormatter;
use super::{FormatName, ResultFormatter};

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format_name(&self) -> FormatName {
        FormatName::Goon
    }

    fn format_version(&self) -> Option<&Version> {
        // Stub: no GOON encoding yet. Returns None until ADR 009 ships
        // along with config/GOON_OUTPUT_FORMAT_VERSION. The output still
        // carries the RAW format_version inside the JSON body because
        // we delegate to GraphFormatter.
        None
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        GraphFormatter.format(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_name_is_goon() {
        assert_eq!(GoonFormatter.format_name(), FormatName::Goon);
    }

    #[test]
    fn format_name_is_not_raw() {
        // Guards against a copy-paste regression where GoonFormatter
        // accidentally reports itself as raw.
        assert_ne!(GoonFormatter.format_name(), FormatName::Raw);
    }

    #[test]
    fn format_version_is_none_while_stub() {
        // When ADR 009 lands and GOON gets its own config file, this
        // test flips to assert Some(version). Keeping it explicit so the
        // transition is observable.
        assert!(GoonFormatter.format_version().is_none());
    }
}
