mod builder;
mod context;
mod edge_builder;
mod edges;
pub mod graph;
mod imports;
mod resolver;
pub mod rules;
pub mod ssa;
pub mod walker;

pub use builder::GraphBuilder;
pub use context::{DefRef, MemberIndex, ResolutionContext};
pub use edge_builder::{BuildEdgesResult, HasRules, ResolveStats, build_edges};
pub use edges::{EdgeSource, ResolvedEdge};
pub use graph::{CodeGraph, GraphEdge, GraphNode};
pub use resolver::{GlobalBacktracker, NoResolver, ReferenceResolver};
pub use rules::ResolutionRules;
pub use ssa::{BlockId, ReachingDefs, SsaResolver, SsaStats, Value};
