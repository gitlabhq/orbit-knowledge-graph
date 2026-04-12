mod builder;
mod context;
mod edges;
pub mod reaching;
mod resolver;
pub mod rules;
pub mod ssa;
pub mod walker;

pub use builder::{GraphBuilder, GraphData};
pub use context::{DefRef, ImportRef, ResolutionContext, ScopedDef};
pub use edges::Edge;
pub use reaching::{HasRules, RulesResolver};
pub use resolver::{GlobalBacktracker, NoResolver, ReferenceResolver};
pub use rules::ResolutionRules;
pub use ssa::{BlockId, ReachingDefs, SsaResolver, Value};
