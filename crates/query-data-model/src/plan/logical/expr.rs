use super::{Comparison, Matching, Predicate};
use crate::PropertyId;
use crate::plan::tracking::{Named, Ref};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expr {
    Property {
        binding: Ref,
        property: PropertyId,
    },
    Truncate {
        binding: Ref,
        property: PropertyId,
        unit: TimeUnit,
    },
    Node(Ref),
    Edge {
        binding: Ref,
        field: EdgeField,
    },
    Output(Ref),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum TimeUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames)]
pub enum EdgeField {
    #[strum(serialize = "relationship_kind")]
    Kind,
    #[strum(serialize = "source_id")]
    Source,
    #[strum(serialize = "source_kind")]
    SourceKind,
    #[strum(serialize = "target_id")]
    Target,
    #[strum(serialize = "target_kind")]
    TargetKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Temporal(String),
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

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Literal(Literal),
    Expr(Expr),
}

impl<T: Into<Literal>> From<T> for Operand {
    fn from(value: T) -> Self {
        Self::Literal(value.into())
    }
}

impl From<Expr> for Operand {
    fn from(value: Expr) -> Self {
        Self::Expr(value)
    }
}

impl Expr {
    pub(super) fn references(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Property { binding, .. }
            | Self::Truncate { binding, .. }
            | Self::Edge { binding, .. }
            | Self::Node(binding)
            | Self::Output(binding) => visit(binding),
        }
    }

    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }

    pub fn eq(self, right: impl Into<Operand>) -> Predicate {
        self.compare(Comparison::Eq, right)
    }

    pub fn ne(self, right: impl Into<Operand>) -> Predicate {
        self.compare(Comparison::Ne, right)
    }

    pub fn gt(self, right: impl Into<Operand>) -> Predicate {
        self.compare(Comparison::Gt, right)
    }

    pub fn lt(self, right: impl Into<Operand>) -> Predicate {
        self.compare(Comparison::Lt, right)
    }

    pub fn compare(self, comparison: Comparison, right: impl Into<Operand>) -> Predicate {
        Predicate::Compare {
            left: self,
            comparison,
            right: right.into(),
        }
    }

    pub fn one_of(self, values: impl IntoIterator<Item = Literal>) -> Predicate {
        Predicate::In {
            left: self,
            values: values.into_iter().collect(),
        }
    }

    pub fn contains(self, needle: impl Into<String>) -> Predicate {
        self.matches(Matching::Contains, needle)
    }

    pub fn matches(self, matching: Matching, needle: impl Into<String>) -> Predicate {
        Predicate::Text {
            left: self,
            matching,
            needle: needle.into(),
        }
    }

    pub fn is_null(self, null: bool) -> Predicate {
        Predicate::Null { left: self, null }
    }
}
