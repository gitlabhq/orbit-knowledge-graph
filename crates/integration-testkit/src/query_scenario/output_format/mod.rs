mod gql;
mod toon;

use query_engine::formatters::ResultFormatter;
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
    type Formatter: ResultFormatter;
    const FORMATTER: Self::Formatter;
    fn decode(text: &str, output: &PipelineOutput) -> Value;
    fn check(_value: &Value, _graph: &Value, _label: &str) {}
}

pub(super) fn assert_format(
    output: &PipelineOutput,
    graph: &Value,
    format: OutputFormat,
    expect: &OutputExpect,
    label: &str,
) {
    let name = format.into();
    match format {
        OutputFormat::Toon => assert_encoded::<toon::Toon>(output, graph, name, expect, label),
        OutputFormat::Gql => assert_encoded::<gql::Gql>(output, graph, name, expect, label),
    }
}

fn assert_encoded<E: Encoding>(
    output: &PipelineOutput,
    graph: &Value,
    format: &str,
    expect: &OutputExpect,
    label: &str,
) {
    let (formatted, version, name) = E::FORMATTER.format_stamped(output);
    assert_eq!(name.as_ref(), format, "{label}: {format} format name");
    assert!(
        !version.is_empty(),
        "{label}: {format} output has no format version"
    );
    let Value::String(text) = formatted else {
        panic!("{label}: {format} output must be a string, got {formatted}");
    };
    let value = E::decode(&text, output);
    E::check(&value, graph, label);
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
        && let Some(mismatch) = crate::scenario::subset_mismatch(format, expected, &value)
    {
        panic!("{label}: {mismatch}\n{value}");
    }
}
