use super::Backend;
use crate::passes::logical_v3::{Expr, NamedExpr, RelationId, SortKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    Default,
    Hash,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalOp<B: Backend> {
    Scan {
        relation: RelationId,
        alias: String,
        access: B::Access,
    },
    Filter(Expr),
    Project(Vec<NamedExpr>),
    Join {
        conditions: Vec<Expr>,
        strategy: JoinStrategy,
    },
    SemiJoin(Expr),
    Aggregate {
        groups: Vec<NamedExpr>,
        metrics: Vec<NamedExpr>,
    },
    Union,
    Alias {
        relation: RelationId,
        alias: String,
    },
    Sort(Vec<SortKey>),
    Limit(u32),
    Deduplicate {
        keys: Vec<Expr>,
        strategy: B::Dedup,
    },
    FusedNeighbors {
        outgoing_predicate: Expr,
        incoming_predicate: Expr,
        columns: Vec<NamedExpr>,
    },
    ScopeGuard(crate::ScopePrefix),
    ReadColumns(Vec<String>),
}
