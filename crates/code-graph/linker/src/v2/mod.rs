mod builder;
mod context;
mod edges;
mod resolver;

pub use builder::{GraphBuilder, GraphData};
pub use context::{DefRef, ImportRef, ResolutionContext, ScopedDef};
pub use edges::Edge;
pub use resolver::{GlobalBacktracker, NoResolver, ReferenceResolver};
