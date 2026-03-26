use serde_json::Value;
use shared::PipelineOutput;

use super::graph::GraphFormatter;
use super::ResultFormatter;

#[derive(Clone, Copy)]
pub struct GoonFormatter;

impl ResultFormatter for GoonFormatter {
    fn format(&self, output: &PipelineOutput) -> Value {
        // Use GraphFormatter.format() which handles aggregates injection.
        GraphFormatter.format(output)
    }
}
