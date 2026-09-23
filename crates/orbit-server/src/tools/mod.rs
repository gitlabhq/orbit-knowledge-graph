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

pub use registry::{CommandRegistry, ToolDefinition, ToolRegistry};
pub(crate) use service::AgentCommand;
pub use service::{ExecutorError, OutputFormat, ToolService};
