use std::sync::LazyLock;

static PROMPTS: LazyLock<orbit_prompts::Prompts> = LazyLock::new(|| {
    orbit_prompts::Prompts::load_embedded("remote").expect("prompts are validated by build.rs")
});

fn prompt(key: &str) -> &'static orbit_prompts::Prompt {
    PROMPTS
        .get(key)
        .unwrap_or_else(|| panic!("prompt `{key}` missing from config/prompts/remote"))
}

mod registry;
mod schema;
mod service;
mod v2_registry;

pub use registry::{CommandRegistry, ToolDefinition, ToolRegistry};
pub use service::{ExecutorError, OutputFormat, ToolPlan, ToolService};
pub use v2_registry::{V2CommandRegistry, V2ToolRegistry};
