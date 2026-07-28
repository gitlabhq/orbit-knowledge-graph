mod prompts {
    include!(concat!(env!("OUT_DIR"), "/tool_prompts.rs"));
}
mod registry;
mod schema;
mod service;
mod v2_registry;

pub use registry::{CommandRegistry, ToolDefinition, ToolRegistry};
pub use service::{ExecutorError, OutputFormat, ToolPlan, ToolService};
pub use v2_registry::{V2CommandRegistry, V2ToolRegistry};
