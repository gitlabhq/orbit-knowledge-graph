use std::sync::LazyLock;

use semver::Version;
use serde_json::Value;
use shared::PipelineOutput;

use super::graph::GraphFormatter;
use super::{FormatName, ResultFormatter};

mod encode;

pub use encode::encode;

pub static GQL_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    orbit_versions::VERSIONS
        .gql_output_format
        .parse()
        .expect("GQL_OUTPUT_FORMAT_VERSION must be valid semver")
});

#[derive(Clone, Copy)]
pub struct GqlFormatter;

impl ResultFormatter for GqlFormatter {
    fn format_name(&self) -> FormatName {
        FormatName::Gql
    }

    fn format_version(&self) -> Option<&Version> {
        Some(&GQL_OUTPUT_FORMAT_VERSION)
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        let response = GraphFormatter.build_response(output);
        Value::String(encode::encode(&response, &GQL_OUTPUT_FORMAT_VERSION))
    }
}
