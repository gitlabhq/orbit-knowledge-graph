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

#[derive(Debug)]
pub struct LatestRows<'a> {
    pub(super) version: ColumnRef<'a>,
    pub(super) keys: Vec<ColumnRef<'a>>,
    pub(super) deletion: Option<ColumnRef<'a>>,
}

impl<'a> LatestRows<'a> {
    pub fn version(&self) -> ColumnRef<'a> {
        self.version
    }
    pub fn keys(&self) -> &[ColumnRef<'a>] {
        &self.keys
    }
    pub fn deletion(&self) -> Option<ColumnRef<'a>> {
        self.deletion
    }
}

#[derive(Debug)]
pub struct Relational<'a, Latest> {
    pub(super) block: BlockId,
    pub(super) kind: OperationKind<'a, Latest>,
    pub(super) columns: Vec<ColumnRef<'a>>,
    pub(super) occurrences: HashSet<RelationId>,
    pub(super) expanded: Vec<ColumnRef<'a>>,
}

#[derive(Debug)]
pub enum OperationKind<'a, Latest> {
    One,
    Source {
        relation: RelationId,
        read: ReadMode,
    },
    Filter {
        input: Box<Relational<'a, Latest>>,
        predicate: Expression<'a>,
    },
    Join {
        left: Box<Relational<'a, Latest>>,
        right: Box<Relational<'a, Latest>>,
        kind: JoinKind,
        condition: Expression<'a>,
    },
    Aggregate {
        input: Box<Relational<'a, Latest>>,
        groups: Vec<Expression<'a>>,
    },
    Expand {
        input: Box<Relational<'a, Latest>>,
        column: ColumnRef<'a>,
    },
    Materialize {
        input: Box<Relational<'a, Latest>>,
        relation: RelationId,
    },
    Latest {
        input: Box<Relational<'a, Latest>>,
        requirement: Latest,
    },
    Sort {
        input: Box<Relational<'a, Latest>>,
        keys: Vec<(ColumnRef<'a>, bool)>,
    },
    FirstBy {
        input: Box<Relational<'a, Latest>>,
        keys: Vec<ColumnRef<'a>>,
    },
    Limit {
        input: Box<Relational<'a, Latest>>,
        count: u32,
    },
}

pub type PhysicalOperation<'a> = Relational<'a, LatestRows<'a>>;
pub type LoweredOperation<'a> = Relational<'a, std::convert::Infallible>;

impl<'a, L> Relational<'a, L> {
    pub fn kind(&self) -> &OperationKind<'a, L> {
        &self.kind
    }
    pub fn into_kind(self) -> OperationKind<'a, L> {
        self.kind
    }
    pub fn columns(&self) -> &[ColumnRef<'a>] {
        &self.columns
    }
    pub fn block(&self) -> BlockId {
        self.block
    }

    pub fn reads_current(&self) -> bool {
        matches!(
            self.kind,
            OperationKind::Source {
                read: ReadMode::Current,
                ..
            }
        ) || self.inputs().any(Self::reads_current)
    }

    pub fn groups(&self) -> &[Expression<'a>] {
        match &self.kind {
            OperationKind::Aggregate { groups, .. } => groups,
            OperationKind::Sort { input, .. } | OperationKind::Limit { input, .. } => {
                input.groups()
            }
            _ => &[],
        }
    }

    pub(super) fn aggregate_input(&self) -> Option<&Self> {
        match &self.kind {
            OperationKind::Aggregate { input, .. } => Some(input),
            OperationKind::Sort { input, .. } | OperationKind::Limit { input, .. } => {
                input.aggregate_input()
            }
            _ => None,
        }
    }

    pub(super) fn bind_parameters(
        &mut self,
        bindings: &mut orbit_utils::query_types::ParamBindings,
    ) {
        for input in self.inputs_mut() {
            input.bind_parameters(bindings);
        }
        match &mut self.kind {
            OperationKind::Filter { predicate, .. }
            | OperationKind::Join {
                condition: predicate,
                ..
            } => predicate.bind_parameters(bindings),
            OperationKind::Aggregate { groups, .. } => {
                for group in groups {
                    group.bind_parameters(bindings);
                }
            }
            _ => {}
        }
    }
}
