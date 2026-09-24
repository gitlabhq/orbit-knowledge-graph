use super::{Expr, NamedExpr, RelationId, SortKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalSource {
    Node {
        relation: RelationId,
        entity: String,
        alias: String,
    },
    Edge {
        relation: RelationId,
        relationships: Vec<String>,
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOp {
    Scan(LogicalSource),
    Filter(Expr),
    Project(Vec<NamedExpr>),
    Join(Vec<Expr>),
    SemiJoin(Expr),
    Aggregate {
        groups: Vec<NamedExpr>,
        metrics: Vec<NamedExpr>,
    },
    Alias {
        relation: RelationId,
        alias: String,
    },
    Union,
    Sort(Vec<SortKey>),
    Limit(u32),
    LatestBy(Vec<Expr>),
}
