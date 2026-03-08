pub mod check;
pub mod security;
pub mod verification;

// Re-export pipeline pass traits for convenience.
pub use crate::pipeline::{EmitPass, FrontendPass, IrPass};
