use query_engine::formatters::{
    FormatName, ResultFormatter, TOON_OUTPUT_FORMAT_VERSION, ToonFormatter,
};
use query_engine::shared::PipelineOutput;
use serde_json::Value;

use super::Encoding;

pub(super) struct Toon;

impl Encoding for Toon {
    fn formatter(&self) -> &'static dyn ResultFormatter {
        &ToonFormatter
    }

    fn name(&self) -> FormatName {
        FormatName::Toon
    }

    fn version(&self) -> String {
        TOON_OUTPUT_FORMAT_VERSION.to_string()
    }

    fn decode(&self, text: &str, _: &PipelineOutput, graph: &Value, label: &str) -> Value {
        let value: Value = toon_format::decode_strict(text)
            .unwrap_or_else(|error| panic!("{label}: invalid TOON ({error}): {text}"));
        assert_eq!(
            text,
            toon_format::encode_default(&value).unwrap() + "\n",
            "{label}: TOON output is not the canonical encoding"
        );
        if value["query_type"] != "aggregation" {
            let toon_nodes: usize = value["nodes"].as_object().map_or(0, |tables| {
                tables
                    .values()
                    .filter_map(Value::as_array)
                    .map(Vec::len)
                    .sum()
            });
            let count = |key: &str, value: &Value| value[key].as_array().map_or(0, Vec::len);
            assert_eq!(
                (toon_nodes, count("edges", &value)),
                (count("nodes", graph), count("edges", graph)),
                "{label}: TOON and graph responses disagree on node and edge counts"
            );
        }
        value
    }
}
