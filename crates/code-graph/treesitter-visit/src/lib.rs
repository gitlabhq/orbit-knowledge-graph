//! Tree-sitter AST parsing, traversal, and extraction toolkit.
//!
//! This crate provides:
//! - A lightweight wrapper around tree-sitter for parsing source code
//! - Composable traversal via [`Axis`] + [`Match`] on [`Node`]
//! - Extraction pipelines ([`extract::Extract`]) for navigating CST paths
//! - Boolean predicates ([`predicate::Pred`]) for node matching

pub mod extract;
pub mod language;
pub mod languages;
mod node;
pub mod predicate;
mod source;
pub mod tree_sitter;

// Re-export core types
pub use language::Language;
pub use languages::SupportLang;
pub use node::{Axis, KindId, Match, Node, Position, Root};
pub use source::{Content, Doc, SgNode};
pub use tree_sitter::{LanguageExt, StrDoc, TSLanguage, TSParseError, TSPoint, TSRange, TsPre};
