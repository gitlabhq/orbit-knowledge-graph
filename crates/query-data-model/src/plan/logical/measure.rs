use super::Expr;
use crate::PropertyId;
use crate::plan::tracking::{Named, Ref};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    Count(Option<Expr>),
    CountDistinct(Expr),
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
}

pub fn count() -> Measure {
    Measure::Count(None)
}

impl Measure {
    pub(super) fn input(&mut self) -> Option<&mut Expr> {
        match self {
            Self::Count(input) => input.as_mut(),
            Self::CountDistinct(expr)
            | Self::Sum(expr)
            | Self::Avg(expr)
            | Self::Min(expr)
            | Self::Max(expr) => Some(expr),
        }
    }

    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Columns {
    Default,
    All,
    Listed(Vec<PropertyId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    Graph { binding: Ref, columns: Columns },
    Value(Expr),
}

impl Projection {
    pub(super) fn references(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Graph { binding, .. } => visit(binding),
            Self::Value(expr) => expr.references(visit),
        }
    }

    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }
}

impl From<Expr> for Projection {
    fn from(expr: Expr) -> Self {
        Self::Value(expr)
    }
}
