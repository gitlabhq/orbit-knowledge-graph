pub mod graph;
pub mod resolve;
pub mod rules;
pub mod ssa;
pub mod walker;

pub use graph::{CodeGraph, GraphBuilder, GraphEdge, GraphNode};
pub use resolve::{
    BuildEdgesResult, DefRef, EdgeSource, HasRules, MemberIndex, ResolutionContext,
    ResolveSettings, ResolveStats, ResolvedEdge, build_edges,
};
pub use rules::ResolutionRules;
pub use ssa::{BlockId, ReachingDefs, SsaResolver, SsaStats, Value};
