pub mod canonical;
pub mod constants;
pub mod dsl;
pub mod file_tree;
pub mod intern;
pub mod linker;
pub mod pipeline;
pub mod resolver;
pub mod ssa;
pub mod tags;
pub mod tree;
pub mod treesitter;

pub mod pattern {
    pub use crate::dsl::rewrite::*;
    pub use crate::dsl::types::*;
}
pub mod rules {
    pub use crate::dsl::rules::*;
}

pub use pipeline::phases;
pub use pipeline::types::{Env, State};
pub use pipeline::{index, parse_single, reindex};
