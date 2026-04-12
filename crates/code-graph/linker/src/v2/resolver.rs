use super::context::ResolutionContext;
use super::edges::Edge;

/// Trait for resolving references into edges.
///
/// Receives a `ResolutionContext` containing all parsed results plus
/// pre-built indexes (definitions by FQN/name/file, imports by path,
/// per-file scope indices).
///
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
    fn resolve(ctx: &ResolutionContext) -> Vec<Edge>;
}

/// No reference resolution. Only structural edges (containment,
/// file→def) are produced by the GraphBuilder.
pub struct NoResolver;

impl ReferenceResolver for NoResolver {
    fn resolve(_ctx: &ResolutionContext) -> Vec<Edge> {
        vec![]
    }
}

/// Name-based resolution with local-first preference.
///
/// Matches references to definitions by name, preferring same-file
/// definitions, with an ambiguity cap. Used for DSL-engine languages
/// that don't have full expression resolvers.
pub struct GlobalBacktracker;

impl ReferenceResolver for GlobalBacktracker {
    fn resolve(_ctx: &ResolutionContext) -> Vec<Edge> {
        // TODO: implement name-based backtracking resolution
        // Uses ctx.definitions.lookup_name() with local-first preference
        // and ctx.scopes.enclosing_definition() for caller context
        vec![]
    }
}
