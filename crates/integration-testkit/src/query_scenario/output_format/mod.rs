mod gql;
mod toon;

use query_engine::formatters::{FormatName, ResultFormatter};
use query_engine::shared::PipelineOutput;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, strum::IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum OutputFormat {
    Toon,
    Gql,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputExpect {
    #[serde(default)]
    pub contains: Vec<String>,
    pub ends_with: Option<String>,
    #[serde(default)]
    pub absent: Vec<String>,
    pub value: Option<Value>,
}

trait Encoding {
    fn formatter(&self) -> &'static dyn ResultFormatter;
    fn name(&self) -> FormatName;
    fn version(&self) -> String;
    fn decode(&self, text: &str, output: &PipelineOutput, graph: &Value, label: &str) -> Value;
}

impl OutputFormat {
    fn encoding(self) -> &'static dyn Encoding {
        match self {
            Self::Toon => &toon::Toon,
            Self::Gql => &gql::Gql,
        }
    }
}

pub(super) fn assert_format(
    output: &PipelineOutput,
    graph: &Value,
    format: OutputFormat,
    expect: &OutputExpect,
    label: &str,
) {
    let encoding = format.encoding();
    let format: &str = format.into();
    let (formatted, version, name) = encoding.formatter().format_stamped(output);
    assert_eq!(name, encoding.name(), "{label}: {format} format name");
    assert_eq!(
        version,
        encoding.version(),
        "{label}: {format} format version"
    );
    let Value::String(text) = formatted else {
        panic!("{label}: {format} output must be a string, got {formatted}");
    };
    let value = encoding.decode(&text, output, graph, label);
    for fragment in &expect.contains {
        assert!(
            text.contains(fragment),
            "{label}: {format} output lacks {fragment:?}:\n{text}"
        );
    }
    if let Some(suffix) = &expect.ends_with {
        assert!(
            text.ends_with(suffix),
            "{label}: {format} output does not end with {suffix:?}:\n{text}"
        );
    }
    for key in &expect.absent {
        assert!(
            value.get(key).is_none(),
            "{label}: {format} output has {key}: {value}"
        );
    }
    if let Some(expected) = &expect.value
        && let Some(mismatch) = subset_mismatch(format, expected, &value)
    {
        panic!("{label}: {mismatch}\n{value}");
    }
}

fn subset_mismatch(path: &str, expected: &Value, actual: &Value) -> Option<String> {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            expected
                .iter()
                .find_map(|(key, value)| match actual.get(key) {
                    Some(found) => subset_mismatch(&format!("{path}.{key}"), value, found),
                    None => Some(format!("{path}.{key} is missing")),
                })
        }
        (Value::Array(expected), Value::Array(actual)) if expected.len() == actual.len() => {
            expected
                .iter()
                .zip(actual)
                .enumerate()
                .find_map(|(index, (value, found))| {
                    subset_mismatch(&format!("{path}[{index}]"), value, found)
                })
        }
        (Value::Array(expected), Value::Array(actual)) => Some(format!(
            "{path} has {} items, expected {}",
            actual.len(),
            expected.len()
        )),
        _ if expected == actual => None,
        _ => Some(format!("{path} is {actual}, expected {expected}")),
    }
}
