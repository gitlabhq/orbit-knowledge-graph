use code_graph_types::CanonicalResult;

use super::edges::Edge;

/// Trait for resolving references into edges.
///
/// Receives all `CanonicalResult`s for a language after parsing.
/// Responsible for *all* reference resolution — both intra-file
/// (scope-based) and cross-file (import/symbol lookup).
///
/// The parser produces raw, unresolved references. The resolver
/// is the only place resolution happens.
///
/// Each language implements its own resolver. Generic resolvers
/// (`NoResolver`, `GlobalBacktracker`) are available for languages
/// that don't need custom logic.
pub trait ReferenceResolver {
    fn resolve(results: &[CanonicalResult], root_path: &str) -> Vec<Edge>;
}

/// No reference resolution. Only structural edges (containment,
/// file→def) are produced by the GraphBuilder.
pub struct NoResolver;

impl ReferenceResolver for NoResolver {
    fn resolve(_results: &[CanonicalResult], _root_path: &str) -> Vec<Edge> {
        vec![]
    }
}

/// Name-based resolution with local-first preference.
///
/// Simpler than a full per-language resolver — matches references to
/// definitions by name, preferring same-file definitions, with an
/// ambiguity cap. Used for DSL-engine languages that don't have
/// full expression resolvers.
pub struct GlobalBacktracker;

impl ReferenceResolver for GlobalBacktracker {
    fn resolve(_results: &[CanonicalResult], _root_path: &str) -> Vec<Edge> {
        // TODO: implement name-based backtracking resolution
        vec![]
    }
}
