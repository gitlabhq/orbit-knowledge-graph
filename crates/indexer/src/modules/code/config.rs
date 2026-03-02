//! Constants for the code indexing module.

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
