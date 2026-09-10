use pest::Span;
use serde_json::Value;

use crate::input::{Direction, FilterOp, OrderDirection, PropertyRef, TruncateUnit};

pub(super) struct Query<'i> {
    pub pattern: Pattern<'i>,
    pub predicates: Vec<Comparison<'i>>,
    pub projections: Projections<'i>,
    pub order: Option<Sort<'i>>,
    pub limit: Option<Limit<'i>>,
    pub debug: bool,
}

pub(super) enum Limit<'i> {
    Rows(u32),
    Page {
        span: Span<'i>,
        size: u32,
        after: Option<String>,
    },
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

impl From<Property<'_>> for PropertyRef {
    fn from(property: Property<'_>) -> Self {
        PropertyRef {
            node: property.node.value,
            property: property.property.value,
        }
    }
}

pub(super) enum Pattern<'i> {
    Element(PatternElement<'i>),
    Shortest {
        variable: Name<'i>,
        element: PatternElement<'i>,
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

pub(super) struct MapLiteral<'i> {
    pub span: Span<'i>,
    pub entries: Vec<MapEntry<'i>>,
}

pub(super) struct MapEntry<'i> {
    pub key: Name<'i>,
    pub value: Value,
}

pub(super) struct Relationship<'i> {
    pub direction: Direction,
    pub variable: Option<Name<'i>>,
    pub types: Vec<Name<'i>>,
    pub range: Option<Range<'i>>,
    pub properties: Option<MapLiteral<'i>>,
}

pub(super) struct Range<'i> {
    pub span: Span<'i>,
    pub start: Option<u32>,
    pub dots: bool,
    pub end: Option<u32>,
}

pub(super) struct Comparison<'i> {
    pub span: Span<'i>,
    pub property: Property<'i>,
    pub op: FilterOp,
    pub value: Option<Value>,
}

pub(super) enum Projections<'i> {
    Star(Span<'i>),
    Items {
        span: Span<'i>,
        items: Vec<ProjectionItem<'i>>,
    },
}

pub(super) struct ProjectionItem<'i> {
    pub expression: Expression<'i>,
    pub alias: Option<Name<'i>>,
}

pub(super) enum Expression<'i> {
    Aggregate {
        function: AggregateFunction,
        target: Target<'i>,
    },
    DateTrunc {
        span: Span<'i>,
        unit: TruncateUnit,
        property: Property<'i>,
    },
    AllProperties {
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
    pub key: Target<'i>,
    pub direction: OrderDirection,
}
