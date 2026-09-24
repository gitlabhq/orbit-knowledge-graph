use super::{Expr, LogicalOp, LogicalSource, NamedExpr, Rel, RelationId, SortKey};

pub fn node(relation: RelationId, entity: impl Into<String>, alias: impl Into<String>) -> Rel {
    Rel::leaf(LogicalOp::Scan(LogicalSource::Node {
        relation,
        entity: entity.into(),
        alias: alias.into(),
    }))
}

pub fn edge(relation: RelationId, relationships: Vec<String>, alias: impl Into<String>) -> Rel {
    Rel::leaf(LogicalOp::Scan(LogicalSource::Edge {
        relation,
        relationships,
        alias: alias.into(),
    }))
}

pub trait RelExt: Sized {
    fn filter(self, predicates: impl IntoIterator<Item = Expr>) -> Self;
    fn project(self, columns: Vec<NamedExpr>) -> Self;
    fn aggregate(self, groups: Vec<NamedExpr>, metrics: Vec<NamedExpr>) -> Self;
    fn sort(self, keys: Vec<SortKey>) -> Self;
    fn limit(self, count: u32) -> Self;
    fn latest_by(self, keys: Vec<Expr>) -> Self;
    fn alias(self, relation: RelationId, alias: impl Into<String>) -> Self;
}

impl RelExt for Rel {
    fn filter(self, predicates: impl IntoIterator<Item = Expr>) -> Self {
        Expr::and(predicates).map_or(self.clone(), |predicate| {
            Rel::unary(LogicalOp::Filter(predicate), self)
        })
    }

    fn project(self, columns: Vec<NamedExpr>) -> Self {
        Rel::unary(LogicalOp::Project(columns), self)
    }

    fn aggregate(self, groups: Vec<NamedExpr>, metrics: Vec<NamedExpr>) -> Self {
        Rel::unary(LogicalOp::Aggregate { groups, metrics }, self)
    }

    fn sort(self, keys: Vec<SortKey>) -> Self {
        if keys.is_empty() {
            self
        } else {
            Rel::unary(LogicalOp::Sort(keys), self)
        }
    }

    fn limit(self, count: u32) -> Self {
        Rel::unary(LogicalOp::Limit(count), self)
    }

    fn latest_by(self, keys: Vec<Expr>) -> Self {
        Rel::unary(LogicalOp::LatestBy(keys), self)
    }

    fn alias(self, relation: RelationId, alias: impl Into<String>) -> Self {
        Rel::unary(
            LogicalOp::Alias {
                relation,
                alias: alias.into(),
            },
            self,
        )
    }
}

pub fn join(
    inputs: impl IntoIterator<Item = Rel>,
    conditions: impl IntoIterator<Item = Expr>,
) -> Rel {
    let mut inputs: Vec<Rel> = inputs.into_iter().collect();
    if inputs.len() == 1 {
        inputs.pop().unwrap()
    } else {
        Rel::nary(LogicalOp::Join(conditions.into_iter().collect()), inputs)
    }
}

pub fn union(inputs: impl IntoIterator<Item = Rel>) -> Rel {
    let mut inputs: Vec<Rel> = inputs.into_iter().collect();
    if inputs.len() == 1 {
        inputs.pop().unwrap()
    } else {
        Rel::nary(LogicalOp::Union, inputs)
    }
}
