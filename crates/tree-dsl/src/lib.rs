pub mod canonical;
pub mod dsl;
pub mod file_tree;
pub mod intern;
pub mod linker;
pub mod pipeline;
pub mod resolver;
pub mod snapshot;
pub mod ssa;
pub mod tree;
pub mod treesitter;

pub mod pattern {
    pub use crate::dsl::pattern::*;
}
pub mod rules {
    pub use crate::dsl::rules::*;
}

pub use pipeline::{IndexResult, Pipeline, index, parse};
