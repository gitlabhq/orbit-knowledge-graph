use query_engine::formatters::GqlFormatter;
use query_engine::shared::PipelineOutput;
use serde_json::{Map, Value, json};

use super::Encoding;

pub(super) struct Gql;

impl Encoding for Gql {
    type Formatter = GqlFormatter;
    const FORMATTER: GqlFormatter = GqlFormatter;

    fn decode(_: &str, output: &PipelineOutput) -> Value {
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
