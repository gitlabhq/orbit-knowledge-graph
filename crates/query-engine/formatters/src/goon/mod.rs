use std::sync::LazyLock;

use semver::Version;
use serde_json::Value;
use shared::PipelineOutput;

use super::graph::GraphFormatter;
use super::{FormatName, ResultFormatter};

mod encode;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;

pub use encode::encode;

#[cfg(test)]
mod trait_tests {
    use super::*;

    #[test]
    fn format_name_is_goon() {
        assert_eq!(GoonFormatter.format_name(), FormatName::Goon);
    }

    #[test]
    fn format_version_is_some_and_parseable() {
        let v = GoonFormatter
            .format_version()
            .expect("GoonFormatter must have a version");
        assert_eq!(v, &*GOON_OUTPUT_FORMAT_VERSION);
    }
}

pub static GOON_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    include_str!(concat!(env!("CONFIG_DIR"), "/GOON_OUTPUT_FORMAT_VERSION"))
        .trim()
        .parse()
        .expect("GOON_OUTPUT_FORMAT_VERSION must be valid semver")
});

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format_name(&self) -> FormatName {
        FormatName::Goon
    }

    fn format_version(&self) -> Option<&Version> {
        Some(&GOON_OUTPUT_FORMAT_VERSION)
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        let response = GraphFormatter.build_response(output);
        Value::String(encode::encode(&response, &GOON_OUTPUT_FORMAT_VERSION))
    }
}
