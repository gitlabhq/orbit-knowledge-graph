use query_engine::formatters::{
    FormatName, GQL_OUTPUT_FORMAT_VERSION, GqlFormatter, ResultFormatter,
};
use query_engine::shared::PipelineOutput;
use serde_json::{Map, Value, json};

use super::Encoding;

pub(super) struct Gql;

impl Encoding for Gql {
    fn formatter(&self) -> &'static dyn ResultFormatter {
        &GqlFormatter
    }

    fn name(&self) -> FormatName {
        FormatName::Gql
    }

    fn version(&self) -> String {
        GQL_OUTPUT_FORMAT_VERSION.to_string()
    }

    fn decode(&self, _: &str, output: &PipelineOutput, _: &Value, _: &str) -> Value {
        let (header, rows) = GqlFormatter.table(output);
        let columns: Map<String, Value> = header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let cells = rows.iter().map(|row| Value::from(row[index].as_str()));
                (name.clone(), cells.collect())
            })
            .collect();
        json!({"header": header, "rows": rows, "columns": columns})
    }
}
