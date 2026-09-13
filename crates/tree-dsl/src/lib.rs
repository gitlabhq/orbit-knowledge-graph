pub mod canonical;
pub mod dsl;
pub mod file_tree;
pub mod grammar;
pub mod lang;
pub mod linker;
pub mod pipeline;
pub mod resolver;
pub mod ssa;
pub mod tree;

// Re-export pattern/rules at old paths for existing consumers
pub mod pattern {
    pub use crate::dsl::pattern::*;
}
pub mod rules {
    pub use crate::dsl::rules::*;
}

pub use pipeline::{IndexResult, Pipeline, index, parse};
