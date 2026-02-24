//! Configuration for the code indexing module.

use std::time::Duration;

pub const LOCK_TTL: Duration = Duration::from_secs(60);

pub mod siphon_actions {
    pub const PUSHED: i32 = 2;
}

pub mod siphon_ref_types {
    pub const BRANCH: i32 = 0;
}

pub mod subjects {
    pub const PUSH_EVENT_PAYLOADS: &str = "push_event_payloads";
}

// TODO: Should be derived from the ontology
pub mod tables {
    pub const GL_DIRECTORY: &str = "gl_directory";
    pub const GL_FILE: &str = "gl_file";
    pub const GL_DEFINITION: &str = "gl_definition";
    pub const GL_IMPORTED_SYMBOL: &str = "gl_imported_symbol";

    pub fn all() -> Vec<&'static str> {
        vec![GL_DIRECTORY, GL_FILE, GL_DEFINITION, GL_IMPORTED_SYMBOL]
    }
}

use crate::configuration::ModuleConfiguration;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CodeIndexingConfig {
    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,

    #[serde(flatten)]
    pub engine: ModuleConfiguration,
}

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

impl Default for CodeIndexingConfig {
    fn default() -> Self {
        Self {
            events_stream_name: "siphon_stream_main_db".to_string(),
            engine: ModuleConfiguration::default(),
        }
    }
}
