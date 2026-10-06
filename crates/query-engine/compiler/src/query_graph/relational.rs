use super::*;

#[derive(Clone, Copy, Debug)]
pub enum ReadMode {
    Raw,
    Current,
}

#[derive(Clone, Copy, Debug)]
pub enum JoinKind {
    Inner,
    Cross,
    Semi,
    Membership,
}

#[derive(Clone, Debug)]
pub struct LatestRows<'catalog> {
    pub version: ColumnRef<'catalog>,
    pub(super) deletion: Option<ColumnRef<'catalog>>,
}

#[derive(Clone, Debug)]
pub enum Relational<'catalog, Latest> {
    One,
    Source {
        relation: RelationId,
        read: ReadMode,
    },
    Filter {
        input: Box<Self>,
        predicate: Expression<'catalog>,
    },
    Join {
        left: Box<Self>,
        right: Box<Self>,
        kind: JoinKind,
        condition: Expression<'catalog>,
    },
    Aggregate {
        input: Box<Self>,
        groups: Vec<Expression<'catalog>>,
    },
    Expand {
        input: Box<Self>,
        column: ColumnRef<'catalog>,
    },
    Materialize {
        input: Box<Self>,
        relation: RelationId,
    },
    Latest {
        input: Box<Self>,
        requirement: Latest,
    },
    Sort {
        input: Box<Self>,
        keys: Vec<(ColumnRef<'catalog>, bool)>,
    },
    FirstBy {
        input: Box<Self>,
        keys: Vec<ColumnRef<'catalog>>,
    },
    Limit {
        input: Box<Self>,
        count: u32,
    },
}

pub type PhysicalOperation<'a> = Relational<'a, LatestRows<'a>>;
pub type LoweredOperation<'a> = Relational<'a, std::convert::Infallible>;

impl<'a, L> Relational<'a, L> {
    pub(super) fn bind_parameters(
        &mut self,
        bindings: &mut orbit_utils::query_types::ParamBindings,
    ) {
        match self {
            Self::Filter { input, predicate } => {
                predicate.bind_parameters(bindings);
                input.bind_parameters(bindings);
            }
            Self::Aggregate { input, groups } => {
                for group in groups {
                    group.bind_parameters(bindings);
                }
                input.bind_parameters(bindings);
            }
            Self::Join {
                left,
                right,
                condition,
                ..
            } => {
                condition.bind_parameters(bindings);
                left.bind_parameters(bindings);
                right.bind_parameters(bindings);
            }
            Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.bind_parameters(bindings),
            Self::One | Self::Source { .. } => {}
        }
    }

    pub fn reads_current(&self) -> bool {
        match self {
            Self::Source { read, .. } => matches!(read, ReadMode::Current),
            Self::Join { left, right, .. } => left.reads_current() || right.reads_current(),
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.reads_current(),
            Self::One => false,
        }
    }
    pub fn source(relation: RelationId) -> Self {
        Self::Source {
            relation,
            read: ReadMode::Raw,
        }
    }
    pub fn current(relation: RelationId) -> Self {
        Self::Source {
            relation,
            read: ReadMode::Current,
        }
    }
    pub fn filter(self, predicate: Expression<'a>) -> Self {
        Self::Filter {
            input: Box::new(self),
            predicate,
        }
    }
    pub fn join(self, right: Self, condition: Expression<'a>) -> Self {
        Self::Join {
            left: Box::new(self),
            right: Box::new(right),
            kind: JoinKind::Inner,
            condition,
        }
    }
    pub fn semi_join(self, right: Self, condition: Expression<'a>) -> Self {
        Self::Join {
            left: Box::new(self),
            right: Box::new(right),
            kind: JoinKind::Semi,
            condition,
        }
    }

    pub fn membership(self, value: ColumnRef<'a>, key: ColumnRef<'a>) -> Self {
        Self::Join {
            left: Box::new(self),
            right: Box::new(Self::source(key.relation)),
            kind: JoinKind::Membership,
            condition: Expression::equal(Expression::Column(value), Expression::Column(key)),
        }
    }
    pub fn aggregate(self, groups: Vec<ColumnRef<'a>>) -> Self {
        self.group_by(groups.into_iter().map(Expression::Column).collect())
    }
    pub fn group_by(self, groups: Vec<Expression<'a>>) -> Self {
        Self::Aggregate {
            input: Box::new(self),
            groups,
        }
    }
    pub fn expand(self, column: ColumnRef<'a>) -> Self {
        Self::Expand {
            input: Box::new(self),
            column,
        }
    }
    pub fn materialize(self, relation: RelationId) -> Self {
        Self::Materialize {
            input: Box::new(self),
            relation,
        }
    }
    pub fn sort(self, keys: Vec<(ColumnRef<'a>, bool)>) -> Self {
        Self::Sort {
            input: Box::new(self),
            keys,
        }
    }
    pub fn limit(self, count: u32) -> Self {
        Self::Limit {
            input: Box::new(self),
            count,
        }
    }

    pub(super) fn fuse_filters(&mut self) {
        match self {
            Self::Source { .. } | Self::One => return,
            Self::Join { left, right, .. } => {
                left.fuse_filters();
                right.fuse_filters();
                return;
            }
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.fuse_filters(),
        }
        let Self::Filter { input, predicate } = self else {
            return;
        };
        if let Self::Filter {
            input: inner,
            predicate: earlier,
        } = input.as_mut()
        {
            let earlier = std::mem::replace(earlier, Expression::Boolean(true));
            let later = std::mem::replace(predicate, Expression::Boolean(true));
            *predicate = Expression::And(Box::new(earlier), Box::new(later));
            *input = std::mem::replace(inner, Box::new(Self::One));
        }
    }

    pub(super) fn expands(&self, column: ColumnRef<'a>) -> bool {
        match self {
            Self::Expand {
                input,
                column: expanded,
            } => *expanded == column || input.expands(column),
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.expands(column),
            Self::Join { left, right, .. } => left.expands(column) || right.expands(column),
            _ => false,
        }
    }

    pub(super) fn aggregate_input(&self) -> Option<&Self> {
        match self {
            Self::Aggregate { input, .. } => Some(input),
            Self::Sort { input, .. } | Self::Limit { input, .. } => input.aggregate_input(),
            _ => None,
        }
    }

    pub(crate) fn groups(&self) -> &[Expression<'a>] {
        match self {
            Self::Aggregate { groups, .. } => groups,
            Self::Sort { input, .. } | Self::Limit { input, .. } => input.groups(),
            _ => &[],
        }
    }

    pub fn filter_source(&mut self, relation: RelationId, predicate: &Expression<'a>) -> usize {
        let Some(source) = self.source_mut(relation) else {
            return 0;
        };
        *source = std::mem::replace(source, Self::One).filter(predicate.clone());
        1
    }

    pub fn source_mut(&mut self, relation: RelationId) -> Option<&mut Self> {
        match self {
            Self::Source {
                relation: source, ..
            } if *source == relation => Some(self),
            Self::Join { left, right, .. } => left
                .source_mut(relation)
                .or_else(|| right.source_mut(relation)),
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.source_mut(relation),
            _ => None,
        }
    }

    pub fn has_source_filter(&self, relation: RelationId, expected: &Expression<'a>) -> bool {
        match self {
            Self::Filter { input, predicate } => {
                (predicate == expected
                    && matches!(input.as_ref(), Self::Source { relation: source, .. } if *source == relation))
                    || input.has_source_filter(relation, expected)
            }
            Self::Join { left, right, .. } => {
                left.has_source_filter(relation, expected)
                    || right.has_source_filter(relation, expected)
            }
            Self::Aggregate { input, .. }
            | Self::Expand { input, .. }
            | Self::Materialize { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.has_source_filter(relation, expected),
            _ => false,
        }
    }
}

impl<'a> PhysicalOperation<'a> {
    pub fn latest(self, version: ColumnRef<'a>, deletion: Option<ColumnRef<'a>>) -> Self {
        Self::Latest {
            input: Box::new(self),
            requirement: LatestRows { version, deletion },
        }
    }
}

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, LoweredOperation<'catalog>>
{
    pub fn fuse_filters(&mut self) {
        for block in &mut self.blocks {
            if let Body::Select { operation, .. } = &mut block.body {
                operation.fuse_filters();
            }
        }
    }
}
