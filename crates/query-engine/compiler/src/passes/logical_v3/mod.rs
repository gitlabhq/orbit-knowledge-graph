mod explain;
mod algebra;
mod expr;
mod op;
mod plan;
mod tree;

pub use expr::*;
pub use algebra::*;
pub use op::*;
pub use plan::plan;
pub use tree::Plan;

pub type Rel = Plan<LogicalOp>;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub root: Rel,
    pub relations: std::collections::BTreeMap<RelationId, LogicalRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalRelation {
    pub alias: String,
    pub source: LogicalRelationSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalRelationSource {
    Node(usize),
    Edge(Option<usize>),
}
