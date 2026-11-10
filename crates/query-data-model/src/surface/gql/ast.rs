use pest::Span;

pub(super) use crate::Direction;
pub(super) use crate::plan::logical::{Literal, Operator, TimeUnit as TruncateUnit};

pub(super) enum BooleanExpression<T> {
    Leaf(T),
    And(Vec<Self>),
    Not(Box<Self>),
}

impl<T> BooleanExpression<T> {
    pub(super) fn depth(&self) -> usize {
        match self {
            Self::Leaf(_) => 0,
            Self::And(children) => children.iter().map(Self::depth).max().unwrap_or(0) + 1,
            Self::Not(child) => child.depth() + 1,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OrderDirection {
    Asc,
    Desc,
}

pub(super) enum Statement<'i> {
    Query(Box<Query<'i>>),
    SchemaCall(Option<String>),
}

pub(super) struct Query<'i> {
    pub pattern: Pattern<'i>,
    pub predicates: Vec<BooleanExpression<Predicate<'i>>>,
    pub projections: Projections<'i>,
    pub order: Option<Sort<'i>>,
    pub skip: Option<u32>,
    pub limit: Option<u32>,
    pub debug: bool,
}

pub(super) struct Name<'i> {
    pub span: Span<'i>,
    pub value: String,
}

pub(super) struct Property<'i> {
    pub span: Span<'i>,
    pub node: Name<'i>,
    pub property: Name<'i>,
}

pub(super) enum Pattern<'i> {
    Elements(Vec<PatternElement<'i>>),
    Shortest {
        variable: Name<'i>,
        element: Box<PatternElement<'i>>,
    },
}

pub(super) struct PatternElement<'i> {
    pub head: NodePattern<'i>,
    pub chain: Vec<(Relationship<'i>, NodePattern<'i>)>,
}

pub(super) struct NodePattern<'i> {
    pub span: Span<'i>,
    pub variable: Name<'i>,
    pub label: Option<Name<'i>>,
    pub properties: Vec<MapEntry<'i>>,
}

pub(super) struct MapEntry<'i> {
    pub key: Name<'i>,
    pub value: Value,
}

/// A literal, or a list of literals.
pub(super) enum Value {
    Scalar(Literal),
    List(Vec<Literal>),
}

pub(super) struct Relationship<'i> {
    pub direction: Direction,
    pub variable: Option<Name<'i>>,
    pub types: Vec<Name<'i>>,
    pub range: Option<Range<'i>>,
    pub properties: Vec<MapEntry<'i>>,
}

pub(super) struct Range<'i> {
    pub span: Span<'i>,
    pub start: Option<u32>,
    pub dots: bool,
    pub end: Option<u32>,
}

pub(super) enum Subject<'i> {
    Property(Property<'i>),
    Type(Name<'i>),
}

pub(super) enum Operand<'i> {
    Value(Value),
    Property(Property<'i>),
}

pub(super) enum Predicate<'i> {
    Comparison {
        span: Span<'i>,
        subject: Subject<'i>,
        op: Operator,
        operand: Option<Operand<'i>>,
    },
    Structural {
        span: Span<'i>,
        property: Property<'i>,
        pattern: String,
        language: String,
    },
}

pub(super) enum Projections<'i> {
    Star(Span<'i>),
    Items(Vec<ProjectionItem<'i>>),
}

pub(super) struct ProjectionItem<'i> {
    pub expression: Expression<'i>,
    pub alias: Option<Name<'i>>,
}

pub(super) enum Expression<'i> {
    Aggregate {
        function: AggregateFunction,
        distinct: bool,
        target: Target<'i>,
    },
    DateTrunc {
        span: Span<'i>,
        unit: TruncateUnit,
        property: Property<'i>,
    },
    AllProperties(Name<'i>),
    RelationshipType {
        span: Span<'i>,
        variable: Name<'i>,
    },
    Node {
        span: Span<'i>,
        variable: Name<'i>,
        properties: Vec<Name<'i>>,
    },
    Property(Property<'i>),
    Variable(Name<'i>),
    Match(Name<'i>),
}

pub(super) enum Target<'i> {
    Property(Property<'i>),
    Variable(Name<'i>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

pub(super) struct Sort<'i> {
    pub span: Span<'i>,
    pub key: SortKey<'i>,
    pub direction: OrderDirection,
}

pub(super) enum SortKey<'i> {
    Property(Property<'i>),
    Variable(Name<'i>),
    RelationshipType(Name<'i>),
}
