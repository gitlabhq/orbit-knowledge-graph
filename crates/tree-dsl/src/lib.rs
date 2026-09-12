pub mod canonical;
pub mod file_tree;
pub mod grammar;
pub mod lang;
pub mod linker;
pub mod pattern;
pub mod pipeline;
pub mod resolver;
pub mod rules;
pub mod ssa;
pub mod tree;

pub use pipeline::{IndexResult, Pipeline, index, parse};
