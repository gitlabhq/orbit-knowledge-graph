mod health;
mod tools;

use std::sync::Arc;

use crate::webserver::tools::ToolRegistry;

pub use health::{HealthResponse, health};
pub use tools::{CallToolRequest, ToolsResponse, call_tool, list_tools};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ToolRegistry>,
}
