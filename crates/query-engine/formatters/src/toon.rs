use std::sync::LazyLock;

use orbit_utils::toon::encode;
use semver::Version;
use serde_json::Value;
use shared::PipelineOutput;

use super::{FormatName, GraphFormatter, ResultFormatter};

pub static TOON_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    orbit_versions::VERSIONS
        .toon_output_format
        .parse()
        .expect("TOON_OUTPUT_FORMAT_VERSION must be valid semver")
});

#[derive(Clone, Copy)]
pub struct ToonFormatter;

impl ResultFormatter for ToonFormatter {
    fn format_name(&self) -> FormatName {
        FormatName::Toon
    }

    fn format_version(&self) -> Option<&Version> {
        Some(&TOON_OUTPUT_FORMAT_VERSION)
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        let mut response = GraphFormatter.build_response(output);
        for properties in response
            .nodes
            .iter_mut()
            .map(|node| &mut node.properties)
            .chain(response.rows.iter_mut().flatten())
        {
            properties.sort_keys();
            properties.values_mut().for_each(Value::sort_all_objects);
        }
        Value::String(
            encode(&response).expect("graph response contains only TOON-encodable values"),
        )
    }
}
