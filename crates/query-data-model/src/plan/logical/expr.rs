use crate::PropertyId;
use crate::plan::tracking::Ref;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expr {
    Property { binding: Ref, property: PropertyId },
    Output(Ref),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<bool> for Literal {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Compare {
        left: Expr,
        comparison: Comparison,
        right: Literal,
    },
    Contains {
        left: Expr,
        needle: String,
    },
    And(Vec<Predicate>),
}

impl Predicate {
    pub(super) fn references(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Compare { left, .. } | Self::Contains { left, .. } => left.references(visit),
            Self::And(all) => all
                .iter_mut()
                .for_each(|predicate| predicate.references(visit)),
        }
    }

    #[must_use]
    pub fn and(self, other: Predicate) -> Predicate {
        match self {
            Self::And(mut all) => {
                all.push(other);
                Self::And(all)
            }
            first => Self::And(vec![first, other]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    Count,
    Sum(Expr),
    Min(Expr),
    Max(Expr),
}

pub fn count() -> Measure {
    Measure::Count
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Named<T> {
    pub item: T,
    pub name: String,
}

impl Measure {
    pub(super) fn input(&mut self) -> Option<&mut Expr> {
        match self {
            Self::Count => None,
            Self::Sum(expr) | Self::Min(expr) | Self::Max(expr) => Some(expr),
        }
    }

    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }
}

impl Expr {
    pub(super) fn references(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Property { binding, .. } => visit(binding),
            Self::Output(output) => visit(output),
        }
    }

    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }

    pub fn eq(self, right: impl Into<Literal>) -> Predicate {
        self.compare(Comparison::Eq, right)
    }

    pub fn ne(self, right: impl Into<Literal>) -> Predicate {
        self.compare(Comparison::Ne, right)
    }

    pub fn gt(self, right: impl Into<Literal>) -> Predicate {
        self.compare(Comparison::Gt, right)
    }

    pub fn lt(self, right: impl Into<Literal>) -> Predicate {
        self.compare(Comparison::Lt, right)
    }

    pub fn compare(self, comparison: Comparison, right: impl Into<Literal>) -> Predicate {
        Predicate::Compare {
            left: self,
            comparison,
            right: right.into(),
        }
    }

    pub fn contains(self, needle: impl Into<String>) -> Predicate {
        Predicate::Contains {
            left: self,
            needle: needle.into(),
        }
    }
}
