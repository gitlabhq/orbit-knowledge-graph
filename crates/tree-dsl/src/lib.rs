pub mod constants;
pub mod dsl;
pub mod file_tree;
pub mod intern;
pub mod linker;
pub mod pipeline;
pub mod resolver;
pub mod shared;
pub mod ssa;
pub mod tree;
pub mod treesitter;

pub use shared::error::{Error, LoadError};
pub use shared::{canonical, error, sentinel, tags};

pub mod pattern {
    pub use crate::dsl::rewrite::*;
    pub use crate::dsl::types::*;
}
pub mod rules {
    pub use crate::dsl::rules::*;
}

pub use pipeline::phases;
pub use pipeline::types::{Env, State};
pub use pipeline::{Envelope, Indexed, export, index, index_with, parse_single, reindex};
