use super::Expr;
use crate::plan::tracking::{BodyId, Ref};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Table {
    pub(super) store: usize,
    pub(super) index: usize,
}

impl Table {
    pub fn store(self) -> usize {
        self.store
    }

    pub fn index(self) -> usize {
        self.index
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Current,
    Raw,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinKind {
    Inner(Expr),
    Left(Expr),
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    ArgMax,
    ArgMaxOrNull,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub function: AggregateFunction,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub condition: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub value: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    All,
    Columns(Vec<Ref>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Scan {
        table: Table,
        read: ReadMode,
        alias: String,
    },
    Read {
        body: BodyId,
        alias: Option<String>,
    },
    Join(JoinKind),
    Filter(Expr),
    Aggregate {
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    },
    Compute(Vec<Expr>),
    Order(Vec<Order>),
    Latest {
        key: Vec<Ref>,
        version: Ref,
    },
    LimitBy {
        count: u32,
        by: Vec<Expr>,
    },
    Limit {
        count: Option<u32>,
        offset: Option<u32>,
    },
    Select(Selection),
    Union,
}

impl Operation {
    fn expressions(&self) -> Vec<&Expr> {
        match self {
            Self::Join(JoinKind::Inner(on) | JoinKind::Left(on)) | Self::Filter(on) => vec![on],
            Self::Aggregate { groups, measures } => groups
                .iter()
                .chain(
                    measures
                        .iter()
                        .flat_map(|measure| measure.args.iter().chain(&measure.condition)),
                )
                .collect(),
            Self::Compute(values) | Self::LimitBy { by: values, .. } => values.iter().collect(),
            Self::Order(orders) => orders.iter().map(|order| &order.value).collect(),
            _ => Vec::new(),
        }
    }

    /// The bodies this operation's expressions test membership in.
    pub fn memberships(&self) -> Vec<BodyId> {
        let mut bodies = Vec::new();
        for expr in self.expressions() {
            expr.memberships(&mut bodies);
        }
        bodies
    }

    /// Every body this operation's expressions read, as a membership or a scalar.
    pub(super) fn subqueries(&self) -> Vec<BodyId> {
        let mut bodies = Vec::new();
        for expr in self.expressions() {
            expr.clone().bodies(&mut |body| bodies.push(*body));
        }
        bodies
    }

    pub(super) fn visit(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        let exprs: Vec<&mut Expr> = match self {
            Self::Join(JoinKind::Inner(on) | JoinKind::Left(on)) | Self::Filter(on) => vec![on],
            Self::Aggregate { groups, measures } => {
                groups
                    .iter_mut()
                    .chain(measures.iter_mut().flat_map(|measure| {
                        measure.args.iter_mut().chain(measure.condition.as_mut())
                    }))
                    .collect()
            }
            Self::Compute(values) | Self::LimitBy { by: values, .. } => values.iter_mut().collect(),
            Self::Order(orders) => orders.iter_mut().map(|order| &mut order.value).collect(),
            Self::Latest { key, version } => {
                key.iter_mut().for_each(&mut *visit);
                visit(version);
                Vec::new()
            }
            Self::Select(Selection::Columns(columns)) => {
                columns.iter_mut().for_each(&mut *visit);
                Vec::new()
            }
            Self::Scan { .. }
            | Self::Read { .. }
            | Self::Join(JoinKind::Cross)
            | Self::Limit { .. }
            | Self::Select(Selection::All)
            | Self::Union => Vec::new(),
        };
        for expr in exprs {
            expr.visit(visit);
        }
    }
}
