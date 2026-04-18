//! Minimal tree-sitter wrapper for AST parsing and traversal.
//!
//! This crate provides a lightweight wrapper around tree-sitter for parsing source code
//! and traversing the resulting AST.

pub mod language;
pub mod languages;
mod node;
mod source;
pub mod tree_sitter;

// Re-export core types
pub use language::Language;
pub use languages::SupportLang;
pub use node::{Axis, KindId, Match, Node, Position, Root};
pub use source::{Content, Doc, SgNode};
pub use tree_sitter::{LanguageExt, StrDoc, TSLanguage, TSParseError, TSPoint, TSRange, TsPre};
