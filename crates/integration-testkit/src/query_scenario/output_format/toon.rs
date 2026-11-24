use query_engine::formatters::ToonFormatter;
use query_engine::shared::PipelineOutput;
use serde_json::Value;

use super::Encoding;

pub(super) struct Toon;

impl Encoding for Toon {
    type Formatter = ToonFormatter;
    const FORMATTER: ToonFormatter = ToonFormatter;

    fn decode(text: &str, _: &PipelineOutput) -> Value {
        let value: Value = toon_format::decode_strict(text)
            .unwrap_or_else(|error| panic!("invalid TOON ({error}): {text}"));
        assert_eq!(
            text,
            toon_format::encode_default(&value).unwrap() + "\n",
            "TOON output is not the canonical encoding"
        );
        value
    }

    fn check(value: &Value, graph: &Value, label: &str) {
        if value["query_type"] == "aggregation" {
            return;
        }
        let toon_nodes: usize = value["nodes"].as_object().map_or(0, |tables| {
            tables
                .values()
                .filter_map(Value::as_array)
                .map(Vec::len)
                .sum()
        });
        let count = |key: &str, value: &Value| value[key].as_array().map_or(0, Vec::len);
        assert_eq!(
            (toon_nodes, count("edges", value)),
            (count("nodes", graph), count("edges", graph)),
            "{label}: TOON and graph responses disagree on node and edge counts"
        );
    }
}
