use super::context::ResolutionContext;
use super::edges::Edge;

/// Trait for resolving references into edges.
///
/// Receives a `ResolutionContext` containing all parsed results plus
/// pre-built indexes. The generic `A` matches the AST type — resolvers
/// that need expression-level access constrain `A` to their concrete
/// AST type.
///
/// Each language implements its own resolver. Generic resolvers
/// (`NoResolver`, `GlobalBacktracker`) work with any AST type.
pub trait ReferenceResolver<A = ()> {
    fn resolve(ctx: &ResolutionContext<A>) -> Vec<Edge>;
}

/// No reference resolution. Only structural edges (containment,
/// file→def) are produced by the GraphBuilder.
pub struct NoResolver;

impl<A> ReferenceResolver<A> for NoResolver {
    fn resolve(_ctx: &ResolutionContext<A>) -> Vec<Edge> {
        vec![]
    }
}

/// Name-based resolution with local-first preference.
///
/// Matches references to definitions by name, preferring same-file
/// definitions, with an ambiguity cap. Used for DSL-engine languages
/// that don't have full expression resolvers.
pub struct GlobalBacktracker;

impl<A> ReferenceResolver<A> for GlobalBacktracker {
    fn resolve(_ctx: &ResolutionContext<A>) -> Vec<Edge> {
        // TODO: implement name-based backtracking resolution
        vec![]
    }
}
