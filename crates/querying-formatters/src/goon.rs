use querying_shared_stages::PipelineOutput;
use serde_json::Value;

use super::ResultFormatter;
use super::graph::GraphFormatter;

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format(&self, output: &PipelineOutput) -> Value {
        let graph = GraphFormatter.build_response(output);
        serde_json::to_value(graph).unwrap_or(Value::Null)
    }
}
