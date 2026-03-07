pub mod security;

// Re-export pipeline pass traits for convenience.
pub use crate::pipeline::{EmitPass, FrontendPass, IrPass};
