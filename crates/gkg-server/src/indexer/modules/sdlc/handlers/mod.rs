//! Generic handlers for ontology-driven ETL processing.
//!
//! This module provides generic handler implementations that use ontology
//! definitions to process entities dynamically.

mod generic_namespaced;
pub mod global_entity;
mod global_handler;

pub use generic_namespaced::GenericNamespacedHandler;
pub use global_entity::GlobalEntityHandler;
pub use global_handler::GlobalHandler;

pub use global_entity::GenericGlobalEntityHandler;

/// Error returned when creating a generic handler fails.
#[derive(Debug, Clone)]
pub struct HandlerCreationError {
    pub node_name: String,
    pub reason: String,
}

impl std::fmt::Display for HandlerCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to create handler for '{}': {}",
            self.node_name, self.reason
        )
    }
}

impl std::error::Error for HandlerCreationError {}
