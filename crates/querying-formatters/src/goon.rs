use query_engine::ResultContext;
use serde_json::Value;

use querying_types::QueryResult;

use super::ResultFormatter;
use super::graph::GraphFormatter;

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format(&self, result: &QueryResult, result_context: &ResultContext) -> Value {
        let graph = GraphFormatter.build_response(result, result_context);
        serde_json::to_value(graph).unwrap_or(Value::Null)
    }
}
