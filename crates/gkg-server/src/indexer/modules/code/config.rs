//! Configuration for the code indexing module.

use std::env;
use std::time::Duration;

/// NATS KV bucket names used by the code indexing module.
pub mod buckets {
    pub const INDEXING_LOCKS: &str = "kg_code_index_locks";
    pub const EVENTS_CACHE: &str = "kg_code_index_events_cache";
}

pub const LOCK_TTL: Duration = Duration::from_secs(60);

pub const EVENTS_CACHE_TTL: Duration = Duration::from_secs(300);

pub mod siphon_actions {
    pub const PUSHED: i32 = 2;
    pub const PUSH_EVENT: i32 = 5;
}

pub mod siphon_ref_types {
    pub const BRANCH: i32 = 0;
}

pub mod subjects {
    pub const PUSH_EVENT_PAYLOADS: &str = "push_event_payloads";
    pub const EVENTS: &str = "events";
}

// TODO: Should be derived from the ontology
pub mod tables {
    pub const GL_DIRECTORY: &str = "gl_directory";
    pub const GL_FILE: &str = "gl_file";
    pub const GL_DEFINITION: &str = "gl_definition";
    pub const GL_IMPORTED_SYMBOL: &str = "gl_imported_symbol";
}

#[derive(Debug, Clone)]
pub struct CodeIndexingConfig {
    pub events_stream_name: String,
}

impl Default for CodeIndexingConfig {
    fn default() -> Self {
        Self {
            events_stream_name: "siphon_stream_main_db".to_string(),
        }
    }
}

impl CodeIndexingConfig {
    pub fn from_env() -> Self {
        Self {
            events_stream_name: env::var("CODE_INDEX_EVENTS_STREAM")
                .unwrap_or_else(|_| "siphon_stream_main_db".to_string()),
        }
    }
}
