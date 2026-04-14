use super::context::ResolutionContext;
use super::edges::ResolvedEdge;

/// Trait for resolving references into edges.
///
/// Receives a `ResolutionContext` containing all parsed results plus
/// pre-built indexes. Returns edges referencing definitions by index.
pub trait ReferenceResolver {
    fn resolve(ctx: &ResolutionContext) -> Vec<ResolvedEdge>;
}

/// No reference resolution. Only structural edges (containment,
/// file→def) are produced by the GraphBuilder.
pub struct NoResolver;

impl ReferenceResolver for NoResolver {
    fn resolve(_ctx: &ResolutionContext) -> Vec<ResolvedEdge> {
        vec![]
    }
}

/// Name-based resolution with local-first preference.
pub struct GlobalBacktracker;

impl ReferenceResolver for GlobalBacktracker {
    fn resolve(_ctx: &ResolutionContext) -> Vec<ResolvedEdge> {
        // TODO: implement name-based backtracking resolution
        vec![]
    }
}
