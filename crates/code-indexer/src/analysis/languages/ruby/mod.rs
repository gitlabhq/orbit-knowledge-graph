//! Ruby-specific analysis components for the Knowledge Graph indexer.
//!
//! This module implements the semantic analysis phase of Ruby code,
//! building upon the structural parsing provided by `gitlab-code-parser`. It follows
//! the **Expression-Oriented Type Inference** strategy inspired by LSPs like
//! `ruby-lsp` and implementations in other languages.
//!
//! ## Architecture Overview
//!
//! The Ruby analyzer operates in two phases:
//!
//! ### Phase 1: Global Definition Index Construction
//! The [`RubyAnalyzer`] first processes all parsed Ruby files to build a complete
//! project-wide index of definitions (classes, modules, methods) and their relationships
//! (inheritance, module inclusions). This creates the foundation for type resolution.
//!
//! ### Phase 2: Expression Resolution and Type Inference
//! Using the [`ExpressionResolver`], the system processes Ruby expressions extracted
//! by the parser, performing sequential symbol-by-symbol resolution that mimics Ruby's
//! method lookup rules. This creates "calls" relationships in the Knowledge Graph.
//!
//! ## Components
//!
//! - [`RubyAnalyzer`]: Main orchestrator for Ruby-specific analysis operations
//! - [`ExpressionResolver`]: Handles reference resolution with type inference
//! - [`ScopeResolver`]: Implements Ruby's scope resolution and method lookup rules  
//! - [`TypeMap`]: Tracks inferred variable types across scopes for accurate resolution
//!
//! ## Supported Analysis Patterns
//!
//! The analyzer handles common Ruby idioms with high accuracy:
//!
//! - **Direct method calls**: `User.find_by_email("test@example.com")`
//! - **Instance method chains**: `user.profile.update(name: "new")`
//! - **Variable assignments**: `user = User.new; user.save`
//! - **Cross-file references**: Method calls spanning multiple Ruby files
//! - **Inheritance resolution**: Finding methods in superclasses and included modules
//!
//! ## Limitations
//!
//! As a static analysis system for a dynamic language, certain patterns cannot be
//! reliably resolved:
//!
//! - Dynamic method dispatch (`send`, `method_missing`)
//! - Runtime metaprogramming (`define_method`)
//! - Complex polymorphism without type annotations
//! - Methods defined via `eval` or other string execution
//!
//! ## Integration with Parser
//!
//! This analyzer consumes [`RubyExpressionMetadata`](parser_core::ruby::references::types::RubyExpressionMetadata)
//! from the parser, which provides structured symbol chains representing Ruby expressions.
//! The parser performs purely structural analysis, while this indexer adds semantic meaning.
//!
//! ## Performance Characteristics
//!
//! The implementation is built with performance in mind for large Ruby codebases:
//!
//! - Pre-allocated hash maps with estimated capacities based on project size
//! - Cached method lookups to avoid repeated inheritance traversal
//! - Memory-efficient string interning for frequently used identifiers
//! - Parallel processing support for independent file analysis
//!
//! For detailed implementation information, see the individual module documentation.

pub mod analyzer;
pub mod expression_resolver;
pub mod scope_resolver;
pub mod type_map;

#[cfg(test)]
mod tests;

pub use analyzer::{AnalyzerStats, RubyAnalyzer, RubyReference};
pub use expression_resolver::ExpressionResolver;
pub use scope_resolver::ScopeResolver;
pub use type_map::TypeMap;
