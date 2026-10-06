use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;

static NEXT_GRAPH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId {
    owner: u64,
    slot: usize,
}

impl std::fmt::Debug for BlockId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "b{}", self.slot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationId {
    block: BlockId,
    slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OutputId {
    block: BlockId,
    slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefinitionId {
    block: BlockId,
    slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnRef<'catalog> {
    relation: RelationId,
    port: Port<'catalog>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Port<'catalog> {
    Stored(&'catalog str),
    Output(OutputId),
}

impl<'catalog> ColumnRef<'catalog> {
    pub fn relation(self) -> RelationId {
        self.relation
    }

    pub fn port(self) -> Port<'catalog> {
        self.port
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GraphError {
    #[error("declaration belongs to another graph")]
    ForeignGraph,
    #[error("unknown stored table or column: {0}")]
    UnknownStored(String),
    #[error("operation requires a SELECT block")]
    ExpectedSelect,
    #[error("column is not exposed by this relation")]
    MissingOutput,
    #[error("relation is outside this query block")]
    OutsideBlock,
    #[error("UNION requires nonempty arms with equal output widths")]
    UnionShape,
    #[error("block has multiple structural owners or a structural cycle")]
    BlockOwnership,
    #[error("definition is not visible at this reference")]
    DefinitionVisibility,
    #[error("latest-row selection requires one stored relation and its full catalog sort key")]
    LatestShape,
    #[error("SELECT requires at least one output")]
    EmptyProjection,
    #[error("UNION outputs have incompatible types")]
    UnionType,
    #[error("recursive output type requires an explicit contract")]
    RecursiveType,
    #[error("expression operands have incompatible types")]
    ExpressionType,
    #[error("join conditions must match the declared relation occurrences")]
    JoinShape,
    #[error("semi-join outputs cannot reference the filtering relation")]
    SemiJoinOutput,
    #[error("nonaggregate outputs must reference grouping columns")]
    Grouping,
    #[error("operation references a value not exposed by its input")]
    OperationVisibility,
    #[error("relation occurrence is used more than once in a block operation")]
    ReusedRelation,
    #[error("expansion requires an array value")]
    ExpectedArray,
    #[error("aggregate expression requires an aggregation boundary and cannot be nested")]
    AggregatePlacement,
    #[error("grouped computations must be projected before another relational operation")]
    AggregateBoundary,
    #[error("prototype does not yet support this input: {0}")]
    UnsupportedInput(String),
}

type Result<T> = std::result::Result<T, GraphError>;

pub struct QueryGraph<'catalog, M: QueryDataModel + ?Sized, E, O> {
    catalog: &'catalog M,
    owner: u64,
    blocks: Vec<Block<'catalog, E, O>>,
}

struct Block<'catalog, E, O> {
    definitions: Vec<Definition>,
    body: Body<'catalog, E, O>,
}

enum Body<'catalog, E, O> {
    Select {
        relations: Vec<Relation<'catalog>>,
        outputs: Vec<Projection<E>>,
        operation: O,
    },
    UnionAll {
        arms: Vec<BlockId>,
        labels: Vec<String>,
    },
}

pub struct Projection<E> {
    pub label: String,
    pub value: E,
}

struct Definition {
    hint: String,
    body: BlockId,
    recursive: bool,
}

pub struct Relation<'catalog> {
    pub hint: String,
    pub source: Source<'catalog>,
}

#[derive(Debug, Clone, Copy)]
pub enum Source<'catalog> {
    Stored(&'catalog str),
    Derived(BlockId),
    Definition(DefinitionId),
}

impl<'catalog, M: QueryDataModel + ?Sized, E, O> QueryGraph<'catalog, M, E, O> {
    pub fn new(catalog: &'catalog M) -> Self {
        Self {
            catalog,
            owner: NEXT_GRAPH.fetch_add(1, Ordering::Relaxed),
            blocks: vec![],
        }
    }

    pub fn select(&mut self, operation: O) -> BlockId {
        self.allocate(Body::Select {
            relations: vec![],
            outputs: vec![],
            operation,
        })
    }

    pub fn scan(
        &mut self,
        block: BlockId,
        table: &'catalog str,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        if self.catalog.table_columns(table).is_none() {
            return Err(GraphError::UnknownStored(table.into()));
        }
        self.add_relation(block, Source::Stored(table), hint.into())
    }

    pub fn derive(
        &mut self,
        block: BlockId,
        body: BlockId,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        self.block(body)?;
        self.add_relation(block, Source::Derived(body), hint.into())
    }

    pub fn define(
        &mut self,
        block: BlockId,
        body: BlockId,
        hint: impl Into<String>,
        recursive: bool,
    ) -> Result<DefinitionId> {
        self.block(body)?;
        let definitions = &mut self.block_mut(block)?.definitions;
        let id = DefinitionId {
            block,
            slot: definitions.len(),
        };
        definitions.push(Definition {
            hint: hint.into(),
            body,
            recursive,
        });
        Ok(id)
    }

    pub fn reference(
        &mut self,
        block: BlockId,
        definition: DefinitionId,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        self.definition(definition)?;
        self.add_relation(block, Source::Definition(definition), hint.into())
    }

    pub fn project(
        &mut self,
        block: BlockId,
        label: impl Into<String>,
        value: E,
    ) -> Result<OutputId> {
        let Body::Select { outputs, .. } = &mut self.block_mut(block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        let id = OutputId {
            block,
            slot: outputs.len(),
        };
        outputs.push(Projection {
            label: label.into(),
            value,
        });
        Ok(id)
    }

    pub fn union_all(&mut self, arms: Vec<BlockId>, labels: Vec<String>) -> Result<BlockId> {
        if arms.is_empty() {
            return Err(GraphError::UnionShape);
        }
        for arm in &arms {
            if self.output_count(*arm)? != labels.len() {
                return Err(GraphError::UnionShape);
            }
        }
        Ok(self.allocate(Body::UnionAll { arms, labels }))
    }

    pub fn stored_column(&self, relation: RelationId, name: &str) -> Result<ColumnRef<'catalog>> {
        let Source::Stored(table) = self.relation(relation)?.source else {
            return Err(GraphError::MissingOutput);
        };
        let name = self
            .catalog
            .table_columns(table)
            .and_then(|columns| columns.get(name))
            .ok_or_else(|| GraphError::UnknownStored(format!("{table}.{name}")))?;
        Ok(ColumnRef {
            relation,
            port: Port::Stored(name),
        })
    }

    pub fn output_column(
        &self,
        relation: RelationId,
        output: OutputId,
    ) -> Result<ColumnRef<'catalog>> {
        self.output_label(output)?;
        let body = match self.relation(relation)?.source {
            Source::Derived(body) => body,
            Source::Definition(definition) => self.definition(definition)?.body,
            Source::Stored(_) => return Err(GraphError::MissingOutput),
        };
        if body != output.block {
            return Err(GraphError::MissingOutput);
        }
        Ok(ColumnRef {
            relation,
            port: Port::Output(output),
        })
    }

    pub fn check_column(&self, block: BlockId, column: ColumnRef<'catalog>) -> Result<()> {
        self.block(block)?;
        if column.relation.block != block {
            return Err(GraphError::OutsideBlock);
        }
        match column.port {
            Port::Stored(name) => {
                self.stored_column(column.relation, name)?;
            }
            Port::Output(output) => {
                self.output_column(column.relation, output)?;
            }
        }
        Ok(())
    }

    pub fn outputs(&self, block: BlockId) -> Result<impl Iterator<Item = OutputId>> {
        Ok((0..self.output_count(block)?).map(move |slot| OutputId { block, slot }))
    }

    pub fn output_label(&self, output: OutputId) -> Result<&str> {
        Ok(match &self.block(output.block)?.body {
            Body::Select { outputs, .. } => &outputs[output.slot].label,
            Body::UnionAll { labels, .. } => &labels[output.slot],
        })
    }

    pub fn union_inputs(&self, output: OutputId) -> Result<Vec<OutputId>> {
        self.output_label(output)?;
        let Body::UnionAll { arms, .. } = &self.block(output.block)?.body else {
            return Err(GraphError::UnionShape);
        };
        Ok(arms
            .iter()
            .map(|block| OutputId {
                block: *block,
                slot: output.slot,
            })
            .collect())
    }

    pub fn replace_output(&mut self, output: OutputId, value: E) -> Result<E> {
        let Body::Select { outputs, .. } = &mut self.block_mut(output.block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok(std::mem::replace(&mut outputs[output.slot].value, value))
    }

    pub fn projection(&self, output: OutputId) -> Result<&Projection<E>> {
        let Body::Select { outputs, .. } = &self.block(output.block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok(&outputs[output.slot])
    }

    pub fn lower<F, P>(
        self,
        mut expression: impl FnMut(E) -> Result<F>,
        mut operation: impl FnMut(O) -> Result<P>,
    ) -> Result<QueryGraph<'catalog, M, F, P>> {
        let blocks = self
            .blocks
            .into_iter()
            .map(|block| {
                let body = match block.body {
                    Body::Select {
                        relations,
                        outputs,
                        operation: value,
                    } => Body::Select {
                        relations,
                        outputs: outputs
                            .into_iter()
                            .map(|output| {
                                Ok(Projection {
                                    label: output.label,
                                    value: expression(output.value)?,
                                })
                            })
                            .collect::<Result<_>>()?,
                        operation: operation(value)?,
                    },
                    Body::UnionAll { arms, labels } => Body::UnionAll { arms, labels },
                };
                Ok(Block {
                    definitions: block.definitions,
                    body,
                })
            })
            .collect::<Result<_>>()?;
        Ok(QueryGraph {
            catalog: self.catalog,
            owner: self.owner,
            blocks,
        })
    }

    pub fn operation_mut(&mut self, block: BlockId) -> Result<&mut O> {
        let Body::Select { operation, .. } = &mut self.block_mut(block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok(operation)
    }

    pub fn operation(&self, block: BlockId) -> Result<&O> {
        let Body::Select { operation, .. } = &self.block(block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok(operation)
    }

    pub fn relation(&self, relation: RelationId) -> Result<&Relation<'catalog>> {
        let Body::Select { relations, .. } = &self.block(relation.block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok(&relations[relation.slot])
    }

    pub fn definition_hint(&self, definition: DefinitionId) -> Result<&str> {
        Ok(&self.definition(definition)?.hint)
    }

    pub fn definitions(
        &self,
        block: BlockId,
    ) -> Result<impl Iterator<Item = (DefinitionId, BlockId)> + '_> {
        Ok(self
            .block(block)?
            .definitions
            .iter()
            .enumerate()
            .map(move |(slot, definition)| (DefinitionId { block, slot }, definition.body)))
    }

    pub fn validate(
        &self,
        root: BlockId,
        check: impl Fn(BlockId, &[Projection<E>], &O) -> Result<()>,
    ) -> Result<()> {
        self.visit(root, &HashSet::new(), &mut HashSet::new(), &check)
    }

    fn visit(
        &self,
        id: BlockId,
        inherited: &HashSet<DefinitionId>,
        owned: &mut HashSet<BlockId>,
        check: &impl Fn(BlockId, &[Projection<E>], &O) -> Result<()>,
    ) -> Result<()> {
        let block = self.block(id)?;
        if !owned.insert(id) {
            return Err(GraphError::BlockOwnership);
        }
        let mut visible = inherited.clone();
        for (slot, definition) in block.definitions.iter().enumerate() {
            let declaration = DefinitionId { block: id, slot };
            if definition.recursive {
                visible.insert(declaration);
            }
            self.visit(definition.body, &visible, owned, check)?;
            visible.insert(declaration);
        }
        match &block.body {
            Body::Select {
                relations,
                outputs,
                operation,
                ..
            } => {
                for relation in relations {
                    match relation.source {
                        Source::Derived(body) => self.visit(body, &visible, owned, check)?,
                        Source::Definition(definition) if !visible.contains(&definition) => {
                            return Err(GraphError::DefinitionVisibility);
                        }
                        _ => {}
                    }
                }
                check(id, outputs, operation)?;
            }
            Body::UnionAll { arms, labels } => {
                for arm in arms {
                    if self.output_count(*arm)? != labels.len() {
                        return Err(GraphError::UnionShape);
                    }
                    self.visit(*arm, &visible, owned, check)?;
                }
            }
        }
        Ok(())
    }

    fn definition(&self, id: DefinitionId) -> Result<&Definition> {
        Ok(&self.block(id.block)?.definitions[id.slot])
    }

    fn output_count(&self, id: BlockId) -> Result<usize> {
        Ok(match &self.block(id)?.body {
            Body::Select { outputs, .. } => outputs.len(),
            Body::UnionAll { labels, .. } => labels.len(),
        })
    }

    fn block(&self, id: BlockId) -> Result<&Block<'catalog, E, O>> {
        if id.owner != self.owner {
            return Err(GraphError::ForeignGraph);
        }
        Ok(&self.blocks[id.slot])
    }

    fn block_mut(&mut self, id: BlockId) -> Result<&mut Block<'catalog, E, O>> {
        if id.owner != self.owner {
            return Err(GraphError::ForeignGraph);
        }
        Ok(&mut self.blocks[id.slot])
    }

    fn allocate(&mut self, body: Body<'catalog, E, O>) -> BlockId {
        let id = BlockId {
            owner: self.owner,
            slot: self.blocks.len(),
        };
        self.blocks.push(Block {
            definitions: vec![],
            body,
        });
        id
    }

    fn add_relation(
        &mut self,
        block: BlockId,
        source: Source<'catalog>,
        hint: String,
    ) -> Result<RelationId> {
        let Body::Select { relations, .. } = &mut self.block_mut(block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        let id = RelationId {
            block,
            slot: relations.len(),
        };
        relations.push(Relation { hint, source });
        Ok(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression<'catalog> {
    Column(ColumnRef<'catalog>),
    Integer(i64),
    Boolean(bool),
    Text(String),
    Count,
    CountIf(Box<Self>),
    Sum {
        value: Box<Self>,
        condition: Option<Box<Self>>,
    },
    Integers(Vec<i64>),
    Tuple(Vec<Self>),
    Array(Vec<Self>),
    Field {
        tuple: Box<Self>,
        index: usize,
    },
    Keep {
        condition: Box<Self>,
        value: Box<Self>,
    },
    Concat(Vec<Self>),
    Greater(Box<Self>, Box<Self>),
    StartsWith(Box<Self>, Box<Self>),
    Equal(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    In(Box<Self>, Box<Self>),
}

impl<'catalog> Expression<'catalog> {
    pub fn equal(left: Self, right: Self) -> Self {
        Self::Equal(Box::new(left), Box::new(right))
    }

    fn rebind(
        &self,
        map: &impl Fn(ColumnRef<'catalog>) -> Result<ColumnRef<'catalog>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Column(column) => Self::Column(map(*column)?),
            Self::Equal(left, right) => Self::equal(left.rebind(map)?, right.rebind(map)?),
            Self::In(left, right) => {
                Self::In(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::And(left, right) => {
                Self::And(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::Greater(left, right) => {
                Self::Greater(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::StartsWith(left, right) => {
                Self::StartsWith(Box::new(left.rebind(map)?), Box::new(right.rebind(map)?))
            }
            Self::Integer(_) | Self::Boolean(_) | Self::Text(_) | Self::Integers(_) => self.clone(),
            _ => {
                return Err(GraphError::UnsupportedInput(
                    "non-scalar candidate predicate".into(),
                ));
            }
        })
    }

    fn columns(&self, visit: &mut impl FnMut(ColumnRef<'catalog>) -> Result<()>) -> Result<()> {
        match self {
            Self::Column(column) => visit(*column),
            Self::CountIf(condition) => condition.columns(visit),
            Self::Sum { value, condition } => {
                value.columns(visit)?;
                if let Some(condition) = condition {
                    condition.columns(visit)?;
                }
                Ok(())
            }
            Self::Tuple(values) | Self::Array(values) | Self::Concat(values) => {
                for value in values {
                    value.columns(visit)?;
                }
                Ok(())
            }
            Self::Field { tuple, .. } => tuple.columns(visit),
            Self::Keep { condition, value } => {
                condition.columns(visit)?;
                value.columns(visit)
            }
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::In(left, right)
            | Self::Greater(left, right)
            | Self::StartsWith(left, right) => {
                left.columns(visit)?;
                right.columns(visit)
            }
            _ => Ok(()),
        }
    }

    fn aggregate(&self) -> bool {
        match self {
            Self::Count | Self::CountIf(_) | Self::Sum { .. } => true,
            Self::Tuple(values) | Self::Array(values) | Self::Concat(values) => {
                values.iter().any(Self::aggregate)
            }
            Self::Field { tuple, .. } => tuple.aggregate(),
            Self::Keep { condition, value } => condition.aggregate() || value.aggregate(),
            Self::Equal(left, right)
            | Self::And(left, right)
            | Self::In(left, right)
            | Self::Greater(left, right)
            | Self::StartsWith(left, right) => left.aggregate() || right.aggregate(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueType {
    Scalar(SqlType),
    Tuple(Vec<Self>),
    Array(Box<Self>),
}

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
}

#[derive(Clone, Debug)]
pub struct LatestRows<'catalog> {
    version: ColumnRef<'catalog>,
    deletion: Option<ColumnRef<'catalog>>,
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
        groups: Vec<ColumnRef<'catalog>>,
    },
    Expand {
        input: Box<Self>,
        column: ColumnRef<'catalog>,
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
    pub fn aggregate(self, groups: Vec<ColumnRef<'a>>) -> Self {
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

    fn fuse_filters(&mut self) {
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

    fn expands(&self, column: ColumnRef<'a>) -> bool {
        match self {
            Self::Expand {
                input,
                column: expanded,
            } => *expanded == column || input.expands(column),
            Self::Filter { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Latest { input, .. }
            | Self::Sort { input, .. }
            | Self::FirstBy { input, .. }
            | Self::Limit { input, .. } => input.expands(column),
            Self::Join { left, right, .. } => left.expands(column) || right.expands(column),
            _ => false,
        }
    }

    fn aggregate_input(&self) -> Option<&Self> {
        match self {
            Self::Aggregate { input, .. } => Some(input),
            Self::Sort { input, .. } | Self::Limit { input, .. } => input.aggregate_input(),
            _ => None,
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
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub fn traversal(&mut self, input: &crate::input::Input) -> Result<BlockId> {
        use crate::input::{ColumnSelection, Direction, FilterOp, QueryType};
        use std::collections::HashMap;
        if input.query_type != QueryType::Traversal
            || input.nodes.is_empty()
            || !input.join_predicates.is_empty()
        {
            return Err(GraphError::UnsupportedInput(
                "expected traversal without cross-node comparisons".into(),
            ));
        }
        let entity = |alias: &str| {
            input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .and_then(|node| node.entity.as_deref())
                .ok_or_else(|| GraphError::UnsupportedInput(format!("unknown entity for {alias}")))
        };
        let mut keys = Vec::new();
        let mut star_center = None;
        let mut star = !input.relationships.is_empty();
        let mut reached = HashSet::new();
        let mut eligible = input.relationships.len() >= 2
            && input.nodes.iter().any(|node| {
                node.entity
                    .as_deref()
                    .is_some_and(|entity| self.catalog.entity_has_traversal_path(entity))
            });
        for (index, relationship) in input.relationships.iter().enumerate() {
            let from = entity(&relationship.from)?;
            let to = entity(&relationship.to)?;
            let (source, target) = if relationship.direction == Direction::Incoming {
                (to, from)
            } else {
                (from, to)
            };
            let key = self
                .catalog
                .foreign_key(&relationship.types, source, target);
            let holder = key.as_ref().map(|key| {
                if matches!(
                    (relationship.direction, key.holder),
                    (Direction::Outgoing, query_data_model::Endpoint::Source)
                        | (Direction::Incoming, query_data_model::Endpoint::Target)
                ) {
                    relationship.from.as_str()
                } else {
                    relationship.to.as_str()
                }
            });
            if index == 0 {
                star_center = holder;
            }
            star &= holder.is_some()
                && holder == star_center
                && relationship.direction != Direction::Both
                && relationship.hops.min == 1
                && relationship.hops.max == 1
                && relationship.filters.is_empty();
            let scope_preserving = !relationship.types.is_empty()
                && relationship.types.iter().all(|kind| {
                    self.catalog
                        .variant_scope(kind, from, to)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                        || self
                            .catalog
                            .variant_scope(kind, to, from)
                            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
            let point_selective = input
                .nodes
                .iter()
                .filter(|node| node.id == relationship.from || node.id == relationship.to)
                .any(|node| !node.node_ids.is_empty() || node.id_range.is_some());
            eligible &= key.is_some()
                && relationship.direction != Direction::Both
                && relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !point_selective
                && (scope_preserving
                    || self.catalog.entity_is_global(from)
                    || self.catalog.entity_is_global(to))
                && (index == 0
                    || reached.contains(&relationship.from) != reached.contains(&relationship.to));
            reached.insert(relationship.from.clone());
            reached.insert(relationship.to.clone());
            keys.push(key);
        }
        let root = self.select(PhysicalOperation::One);
        let mut relations = HashMap::new();
        let mut node_operations = HashMap::new();
        let mut node_predicates = HashMap::new();
        for node in &input.nodes {
            let entity = node
                .entity
                .as_deref()
                .ok_or_else(|| GraphError::UnsupportedInput("missing node entity".into()))?;
            let table = self
                .catalog
                .entity_table(entity)
                .ok_or_else(|| GraphError::UnknownStored(entity.into()))?;
            let relation = self.scan(root, table, &node.id)?;
            let mut operation = PhysicalOperation::current(relation);
            let deleted = self.stored_column(relation, "_deleted")?;
            operation = operation.filter(Expression::equal(
                Expression::Column(deleted),
                Expression::Boolean(false),
            ));
            if !node.node_ids.is_empty() {
                let id = Expression::Column(self.stored_column(relation, &node.id_property)?);
                operation = operation.filter(if let [value] = node.node_ids.as_slice() {
                    Expression::equal(id, Expression::Integer(*value))
                } else {
                    Expression::In(
                        Box::new(id),
                        Box::new(Expression::Integers(node.node_ids.clone())),
                    )
                });
            }
            if node.id_range.is_some() {
                return Err(GraphError::UnsupportedInput("ID range predicate".into()));
            }
            let mut properties = node.filters.iter().collect::<Vec<_>>();
            properties.sort_by_key(|(name, _)| *name);
            for (name, filters) in properties {
                let column = self
                    .catalog
                    .property_column_named(entity, name)
                    .ok_or_else(|| GraphError::UnknownStored(name.clone()))?;
                for filter in filters {
                    if filter.op.unwrap_or(FilterOp::Eq) != FilterOp::Eq
                        || filter.rhs_column.is_some()
                    {
                        return Err(GraphError::UnsupportedInput(
                            "nonliteral equality filter".into(),
                        ));
                    }
                    let value = match filter.value.as_ref() {
                        Some(serde_json::Value::String(value)) => Expression::Text(value.clone()),
                        Some(serde_json::Value::Bool(value)) => Expression::Boolean(*value),
                        Some(serde_json::Value::Number(value)) if value.as_i64().is_some() => {
                            Expression::Integer(value.as_i64().unwrap())
                        }
                        _ => return Err(GraphError::UnsupportedInput("filter value".into())),
                    };
                    operation = operation.filter(Expression::equal(
                        Expression::Column(self.stored_column(relation, column)?),
                        value,
                    ));
                }
            }
            relations.insert(node.id.as_str(), relation);
            let mut predicates = Vec::new();
            let mut source = &operation;
            while let Relational::Filter { input, predicate } = source {
                predicates.push(predicate.clone());
                source = input;
            }
            predicates.reverse();
            node_predicates.insert(node.id.as_str(), predicates);
            node_operations.insert(node.id.as_str(), operation);
        }
        if star {
            let center = star_center.expect("FK star holder");
            let center_node = input
                .nodes
                .iter()
                .find(|node| node.id == center)
                .expect("center input");
            let center_selective = !center_node.node_ids.is_empty()
                || center_node.filters.keys().any(|property| {
                    self.catalog
                        .property(entity(center).unwrap(), property)
                        .is_some_and(|property| {
                            self.catalog.property_selectivity(property.id)
                                == ontology::FieldSelectivity::High
                        })
                });
            let mut candidates = HashMap::new();
            let mut center_memberships = Vec::new();
            let mut targets = input
                .relationships
                .iter()
                .zip(&keys)
                .map(|(relationship, key)| {
                    let target = if relationship.from == center {
                        relationship.to.as_str()
                    } else {
                        relationship.from.as_str()
                    };
                    (target, key.as_ref().expect("star key"))
                })
                .collect::<Vec<_>>();
            targets.sort_by_key(|(target, _)| *target);
            for (target, key) in &targets {
                let node = input
                    .nodes
                    .iter()
                    .find(|node| node.id == *target)
                    .expect("target input");
                let holder_column = self
                    .catalog
                    .property_column(key.property)
                    .ok_or(GraphError::MissingOutput)?;
                let target_column = self
                    .catalog
                    .property_column(key.referenced_key)
                    .ok_or(GraphError::MissingOutput)?;
                if target_column == "id" && !node.node_ids.is_empty() {
                    let column =
                        Expression::Column(self.stored_column(relations[center], holder_column)?);
                    node_predicates
                        .get_mut(center)
                        .unwrap()
                        .push(Expression::In(
                            Box::new(column),
                            Box::new(Expression::Integers(node.node_ids.clone())),
                        ));
                }
                if !node.filters.is_empty() || !node.node_ids.is_empty() {
                    let candidate = self.candidate(
                        root,
                        relations[target],
                        target_column,
                        &node_predicates[target],
                        &[],
                        &format!("_candidate_{target}"),
                    )?;
                    candidates.insert(*target, candidate);
                    center_memberships.push((holder_column, candidate));
                }
            }
            let mut center_operation = PhysicalOperation::current(relations[center]);
            for predicate in &node_predicates[center] {
                center_operation = center_operation.filter(predicate.clone());
            }
            if !center_memberships.is_empty() {
                let candidate = self.candidate(
                    root,
                    relations[center],
                    "id",
                    &node_predicates[center],
                    &center_memberships,
                    &format!("_candidate_{center}"),
                )?;
                center_operation = self.narrow(
                    root,
                    center_operation,
                    self.stored_column(relations[center], "id")?,
                    candidate,
                )?;
            }
            node_operations.insert(center, center_operation);
            for (target, key) in targets {
                let node = input
                    .nodes
                    .iter()
                    .find(|node| node.id == target)
                    .expect("target input");
                let target_column = self
                    .catalog
                    .property_column(key.referenced_key)
                    .ok_or(GraphError::MissingOutput)?;
                let candidate = if let Some(candidate) = candidates.get(target) {
                    Some(*candidate)
                } else if center_selective && node.filters.is_empty() && node.node_ids.is_empty() {
                    let holder_column = self
                        .catalog
                        .property_column(key.property)
                        .ok_or(GraphError::MissingOutput)?;
                    Some(self.candidate(
                        root,
                        relations[center],
                        holder_column,
                        &node_predicates[center],
                        &center_memberships,
                        &format!("_narrow_{target}"),
                    )?)
                } else {
                    None
                };
                if let Some(candidate) = candidate {
                    let relation = relations[target];
                    let mut source = self.narrow(
                        root,
                        PhysicalOperation::source(relation),
                        self.stored_column(relation, target_column)?,
                        candidate,
                    )?;
                    let table = self
                        .catalog
                        .entity_table(entity(target)?)
                        .ok_or(GraphError::MissingOutput)?;
                    let sort_key = self
                        .catalog
                        .table_sort_key(table)
                        .ok_or(GraphError::LatestShape)?;
                    for predicate in &node_predicates[target] {
                        let mut immutable = true;
                        predicate.columns(&mut |column| { immutable &= matches!(column.port, Port::Stored(name) if sort_key.iter().any(|key| key == name)); Ok(()) })?;
                        if immutable {
                            source = source.filter(predicate.clone());
                        }
                    }
                    source = source.latest(self.stored_column(relation, "_version")?, None);
                    for predicate in &node_predicates[target] {
                        source = source.filter(predicate.clone());
                    }
                    node_operations.insert(target, source);
                }
            }
        }
        let mut operation;
        if eligible || star {
            let first = if star {
                star_center.expect("FK star holder")
            } else {
                input.relationships[0].from.as_str()
            };
            operation = node_operations.remove(first).expect("declared node");
            let mut reached = HashSet::from([first]);
            for (relationship, key) in input.relationships.iter().zip(keys) {
                let key = key.expect("eligible FK");
                let holder_is_from = matches!(
                    (relationship.direction, key.holder),
                    (Direction::Outgoing, query_data_model::Endpoint::Source)
                        | (Direction::Incoming, query_data_model::Endpoint::Target)
                );
                let (holder, target) = if holder_is_from {
                    (&relationship.from, &relationship.to)
                } else {
                    (&relationship.to, &relationship.from)
                };
                let next = if reached.contains(relationship.from.as_str()) {
                    &relationship.to
                } else {
                    &relationship.from
                };
                let holder_column = self
                    .catalog
                    .property_column(key.property)
                    .ok_or(GraphError::MissingOutput)?;
                let target_column = self
                    .catalog
                    .property_column(key.referenced_key)
                    .ok_or(GraphError::MissingOutput)?;
                let condition = Expression::equal(
                    Expression::Column(
                        self.stored_column(relations[holder.as_str()], holder_column)?,
                    ),
                    Expression::Column(
                        self.stored_column(relations[target.as_str()], target_column)?,
                    ),
                );
                operation = if let Some(next) = node_operations.remove(next.as_str()) {
                    operation.join(next, condition)
                } else {
                    operation.filter(condition)
                };
                reached.insert(next.as_str());
            }
        } else {
            operation = PhysicalOperation::One;
            let mut edges = Vec::new();
            for (index, relationship) in input.relationships.iter().enumerate() {
                if relationship.hops.max != 1 || relationship.direction == Direction::Both {
                    return Err(GraphError::UnsupportedInput(
                        "variable or bidirectional traversal".into(),
                    ));
                }
                let table = self
                    .catalog
                    .relationship_table_for_query(&relationship.types);
                let edge = self.scan(root, table, format!("e{index}"))?;
                let (start, end) = relationship.direction.edge_columns();
                let mut scan = PhysicalOperation::current(edge).filter(Expression::equal(
                    Expression::Column(self.stored_column(edge, "_deleted")?),
                    Expression::Boolean(false),
                ));
                for (column, kind) in [
                    (
                        "source_kind",
                        if relationship.direction == Direction::Incoming {
                            entity(&relationship.to)?
                        } else {
                            entity(&relationship.from)?
                        },
                    ),
                    (
                        "target_kind",
                        if relationship.direction == Direction::Incoming {
                            entity(&relationship.from)?
                        } else {
                            entity(&relationship.to)?
                        },
                    ),
                ] {
                    scan = scan.filter(Expression::equal(
                        Expression::Column(self.stored_column(edge, column)?),
                        Expression::Text(kind.into()),
                    ));
                }
                if let [kind] = relationship.types.as_slice() {
                    scan = scan.filter(Expression::equal(
                        Expression::Column(self.stored_column(edge, "relationship_kind")?),
                        Expression::Text(kind.clone()),
                    ));
                } else {
                    return Err(GraphError::UnsupportedInput(
                        "multiple relationship kinds".into(),
                    ));
                }
                for (column, filters) in &relationship.filters {
                    for filter in filters {
                        let value = filter
                            .value
                            .as_ref()
                            .and_then(serde_json::Value::as_i64)
                            .ok_or_else(|| GraphError::UnsupportedInput("edge predicate".into()))?;
                        if filter.op.unwrap_or(FilterOp::Eq) != FilterOp::Eq {
                            return Err(GraphError::UnsupportedInput("edge operator".into()));
                        }
                        scan = scan.filter(Expression::equal(
                            Expression::Column(self.stored_column(edge, column)?),
                            Expression::Integer(value),
                        ));
                    }
                }
                let endpoints = [
                    (relationship.from.as_str(), start),
                    (relationship.to.as_str(), end),
                ];
                if index == 0 {
                    operation = scan;
                } else {
                    let (previous, previous_column, current_column) = edges
                        .iter()
                        .rev()
                        .find_map(|(previous, ends): &(RelationId, [(&str, &str); 2])| {
                            ends.iter().find_map(|(node, column)| {
                                endpoints
                                    .iter()
                                    .find(|(next, _)| next == node)
                                    .map(|(_, next_column)| (*previous, *column, *next_column))
                            })
                        })
                        .ok_or(GraphError::JoinShape)?;
                    operation = operation.join(
                        scan,
                        Expression::equal(
                            Expression::Column(self.stored_column(previous, previous_column)?),
                            Expression::Column(self.stored_column(edge, current_column)?),
                        ),
                    );
                }
                edges.push((edge, endpoints));
            }
            for node in &input.nodes {
                let (edge, column) = edges
                    .iter()
                    .find_map(|(edge, endpoints)| {
                        endpoints
                            .iter()
                            .find(|(name, _)| *name == node.id)
                            .map(|(_, column)| (*edge, *column))
                    })
                    .ok_or(GraphError::JoinShape)?;
                operation = operation.join(
                    node_operations
                        .remove(node.id.as_str())
                        .expect("declared node"),
                    Expression::equal(
                        Expression::Column(self.stored_column(edge, column)?),
                        Expression::Column(
                            self.stored_column(relations[node.id.as_str()], &node.id_property)?,
                        ),
                    ),
                );
            }
        }
        if !node_operations.is_empty() {
            return Err(GraphError::UnsupportedInput("disconnected nodes".into()));
        }
        for node in &input.nodes {
            let Some(ColumnSelection::List(columns)) = &node.columns else {
                return Err(GraphError::UnsupportedInput(
                    "expected normalized columns".into(),
                ));
            };
            for property in columns {
                let entity = node.entity.as_deref().unwrap();
                let Some(column) = self.catalog.property_column_named(entity, property) else {
                    continue;
                };
                self.project(
                    root,
                    format!("{}_{property}", node.id),
                    Expression::Column(self.stored_column(relations[node.id.as_str()], column)?),
                )?;
            }
        }
        for (index, relationship) in input.relationships.iter().enumerate() {
            let (source, target) = if relationship.direction == Direction::Incoming {
                (&relationship.to, &relationship.from)
            } else {
                (&relationship.from, &relationship.to)
            };
            self.project(
                root,
                format!("e{index}_src"),
                Expression::Column(self.stored_column(relations[source.as_str()], "id")?),
            )?;
            self.project(
                root,
                format!("e{index}_dst"),
                Expression::Column(self.stored_column(relations[target.as_str()], "id")?),
            )?;
        }
        if let Some(order) = &input.order_by {
            operation = operation.sort(vec![(
                self.stored_column(relations[order.node.as_str()], &order.property)?,
                order.direction == crate::input::OrderDirection::Desc,
            )]);
        }
        *self.operation_mut(root)? = operation.limit(input.limit);
        Ok(root)
    }

    fn candidate(
        &mut self,
        parent: BlockId,
        original: RelationId,
        key: &str,
        predicates: &[Expression<'catalog>],
        memberships: &[(&str, (DefinitionId, OutputId))],
        hint: &str,
    ) -> Result<(DefinitionId, OutputId)> {
        let Source::Stored(table) = self.relation(original)?.source else {
            return Err(GraphError::LatestShape);
        };
        let alias = self.relation(original)?.hint.clone();
        let body = self.select(PhysicalOperation::One);
        let relation = self.scan(body, table, alias)?;
        let mut operation = PhysicalOperation::source(relation);
        for predicate in predicates {
            operation = operation.filter(predicate.rebind(&|column| {
                if column.relation != original {
                    return Err(GraphError::OutsideBlock);
                }
                let Port::Stored(name) = column.port else {
                    return Err(GraphError::MissingOutput);
                };
                self.stored_column(relation, name)
            })?);
        }
        for (column, candidate) in memberships {
            operation = self.narrow(
                body,
                operation,
                self.stored_column(relation, column)?,
                *candidate,
            )?;
        }
        let output = self.project(
            body,
            "id",
            Expression::Column(self.stored_column(relation, key)?),
        )?;
        *self.operation_mut(body)? = operation;
        Ok((self.define(parent, body, hint, false)?, output))
    }

    fn narrow(
        &mut self,
        block: BlockId,
        input: PhysicalOperation<'catalog>,
        column: ColumnRef<'catalog>,
        (definition, output): (DefinitionId, OutputId),
    ) -> Result<PhysicalOperation<'catalog>> {
        let hint = self.definition_hint(definition)?.to_owned();
        let keys = self.reference(block, definition, hint)?;
        Ok(input.semi_join(
            PhysicalOperation::source(keys),
            Expression::equal(
                Expression::Column(column),
                Expression::Column(self.output_column(keys, output)?),
            ),
        ))
    }

    pub fn lower_operations(
        self,
    ) -> Result<QueryGraph<'catalog, M, Expression<'catalog>, LoweredOperation<'catalog>>> {
        let operations = self
            .blocks
            .iter()
            .enumerate()
            .filter_map(|(slot, block)| {
                let Body::Select { operation, .. } = &block.body else {
                    return None;
                };
                Some(self.lower_operation(
                    BlockId {
                        owner: self.owner,
                        slot,
                    },
                    operation,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut operations = operations.into_iter();
        self.lower(Ok, |_| {
            Ok(operations.next().expect("one lowered operation per SELECT"))
        })
    }

    fn lower_operation(
        &self,
        block: BlockId,
        operation: &PhysicalOperation<'catalog>,
    ) -> Result<LoweredOperation<'catalog>> {
        use Relational::*;
        Ok(match operation {
            One => One,
            Source { relation, read } => Source {
                relation: *relation,
                read: *read,
            },
            Filter { input, predicate } => self
                .lower_operation(block, input)?
                .filter(predicate.clone()),
            Join {
                left,
                right,
                kind,
                condition,
            } => Join {
                left: Box::new(self.lower_operation(block, left)?),
                right: Box::new(self.lower_operation(block, right)?),
                kind: *kind,
                condition: condition.clone(),
            },
            Aggregate { input, groups } => self
                .lower_operation(block, input)?
                .aggregate(groups.clone()),
            Expand { input, column } => self.lower_operation(block, input)?.expand(*column),
            Sort { input, keys } => self.lower_operation(block, input)?.sort(keys.clone()),
            FirstBy { input, keys } => FirstBy {
                input: Box::new(self.lower_operation(block, input)?),
                keys: keys.clone(),
            },
            Limit { input, count } => self.lower_operation(block, input)?.limit(*count),
            Latest {
                input,
                requirement: LatestRows { version, deletion },
            } => {
                let mut scan = input.as_ref();
                loop {
                    match scan {
                        Filter { input, .. } => scan = input,
                        Join {
                            left,
                            kind: JoinKind::Semi,
                            ..
                        } => scan = left,
                        _ => break,
                    }
                }
                if !matches!(scan, Source { relation, read: ReadMode::Raw } if *relation == version.relation)
                {
                    return Err(GraphError::LatestShape);
                }
                self.check_column(block, *version)?;
                let crate::query_graph::Source::Stored(table) =
                    self.relation(version.relation)?.source
                else {
                    return Err(GraphError::LatestShape);
                };
                let keys = self
                    .catalog
                    .table_sort_key(table)
                    .filter(|keys| !keys.is_empty())
                    .ok_or(GraphError::LatestShape)?
                    .iter()
                    .map(|name| self.stored_column(version.relation, name))
                    .collect::<Result<Vec<_>>>()?;
                let order = keys
                    .iter()
                    .map(|column| (*column, false))
                    .chain([(*version, true)])
                    .collect();
                let latest = FirstBy {
                    input: Box::new(self.lower_operation(block, input)?.sort(order)),
                    keys,
                };
                if let Some(deleted) = deletion {
                    if deleted.relation != version.relation {
                        return Err(GraphError::LatestShape);
                    }
                    latest.filter(Expression::equal(
                        Expression::Column(*deleted),
                        Expression::Boolean(false),
                    ))
                } else {
                    latest
                }
            }
        })
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

    fn source_columns(&self, relation: RelationId) -> Result<Vec<ColumnRef<'catalog>>> {
        match self.relation(relation)?.source {
            Source::Stored(table) => {
                let mut names = self
                    .catalog
                    .table_columns(table)
                    .ok_or_else(|| GraphError::UnknownStored(table.into()))?
                    .iter()
                    .collect::<Vec<_>>();
                names.sort();
                names
                    .into_iter()
                    .map(|name| self.stored_column(relation, name))
                    .collect()
            }
            Source::Derived(body) => self
                .outputs(body)?
                .map(|output| self.output_column(relation, output))
                .collect(),
            Source::Definition(definition) => self
                .outputs(self.definition(definition)?.body)?
                .map(|output| self.output_column(relation, output))
                .collect(),
        }
    }

    fn operation_columns(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        used: &mut HashSet<RelationId>,
    ) -> Result<Vec<ColumnRef<'catalog>>> {
        use Relational::*;
        let check = |expression: &Expression<'catalog>, columns: &[ColumnRef<'catalog>]| {
            expression.columns(&mut |column| {
                if columns.contains(&column) {
                    Ok(())
                } else {
                    Err(GraphError::OperationVisibility)
                }
            })?;
            if expression.aggregate() {
                return Err(GraphError::AggregatePlacement);
            }
            if self.expression_type(expression, &mut HashSet::new())?
                != ValueType::Scalar(SqlType::Bool)
            {
                return Err(GraphError::ExpressionType);
            }
            Ok(())
        };
        match operation {
            One => Ok(vec![]),
            Source { relation, read } => {
                if relation.block != block {
                    return Err(GraphError::OutsideBlock);
                }
                if !used.insert(*relation) {
                    return Err(GraphError::ReusedRelation);
                }
                if matches!(read, ReadMode::Current)
                    && !matches!(
                        self.relation(*relation)?.source,
                        crate::query_graph::Source::Stored(_)
                    )
                {
                    return Err(GraphError::LatestShape);
                }
                self.source_columns(*relation)
            }
            Filter { input, predicate } => {
                let columns = self.operation_columns(block, input, used)?;
                check(predicate, &columns)?;
                Ok(columns)
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left = self.operation_columns(block, left, used)?;
                let right = self.operation_columns(block, right, used)?;
                let mut all = left.clone();
                all.extend(right);
                check(condition, &all)?;
                Ok(if matches!(kind, JoinKind::Semi) {
                    left
                } else {
                    all
                })
            }
            Aggregate { input, groups } => {
                let columns = self.operation_columns(block, input, used)?;
                if groups.iter().any(|column| !columns.contains(column)) {
                    return Err(GraphError::Grouping);
                }
                Ok(groups.clone())
            }
            Expand { input, column } => {
                let columns = self.operation_columns(block, input, used)?;
                if !columns.contains(column) {
                    return Err(GraphError::OperationVisibility);
                }
                if !matches!(
                    self.column_type(*column, &mut HashSet::new())?,
                    ValueType::Array(_)
                ) {
                    return Err(GraphError::ExpectedArray);
                }
                Ok(columns)
            }
            Sort { input, keys } => {
                let columns = self.operation_columns(block, input, used)?;
                if keys.iter().any(|(column, _)| !columns.contains(column)) {
                    return Err(GraphError::OperationVisibility);
                }
                Ok(columns)
            }
            FirstBy { input, keys } => {
                let columns = self.operation_columns(block, input, used)?;
                if keys.is_empty() || keys.iter().any(|column| !columns.contains(column)) {
                    return Err(GraphError::LatestShape);
                }
                Ok(columns)
            }
            Limit { input, .. } => self.operation_columns(block, input, used),
            Latest { requirement, .. } => match *requirement {},
        }
    }

    fn expression_type(
        &self,
        expression: &Expression<'catalog>,
        visiting: &mut HashSet<OutputId>,
    ) -> Result<ValueType> {
        match expression {
            Expression::Integer(_) | Expression::Count => Ok(ValueType::Scalar(SqlType::Int64)),
            Expression::Boolean(_) => Ok(ValueType::Scalar(SqlType::Bool)),
            Expression::Text(_) => Ok(ValueType::Scalar(SqlType::String)),
            Expression::Integers(_) => Ok(ValueType::Array(Box::new(ValueType::Scalar(
                SqlType::Int64,
            )))),
            Expression::Tuple(values) => Ok(ValueType::Tuple(
                values
                    .iter()
                    .map(|value| self.expression_type(value, visiting))
                    .collect::<Result<_>>()?,
            )),
            Expression::Array(values) => {
                let first = values.first().ok_or(GraphError::ExpectedArray)?;
                let element = self.expression_type(first, visiting)?;
                for value in &values[1..] {
                    if self.expression_type(value, visiting)? != element {
                        return Err(GraphError::ExpressionType);
                    }
                }
                Ok(ValueType::Array(Box::new(element)))
            }
            Expression::Field { tuple, index } => {
                let ValueType::Tuple(fields) = self.expression_type(tuple, visiting)? else {
                    return Err(GraphError::ExpressionType);
                };
                fields
                    .get(*index)
                    .cloned()
                    .ok_or(GraphError::ExpressionType)
            }
            Expression::Keep { condition, value } => {
                if self.expression_type(condition, visiting)? != ValueType::Scalar(SqlType::Bool) {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Array(Box::new(
                    self.expression_type(value, visiting)?,
                )))
            }
            Expression::Concat(arrays) => {
                let ty = self
                    .expression_type(arrays.first().ok_or(GraphError::ExpectedArray)?, visiting)?;
                if !matches!(ty, ValueType::Array(_)) {
                    return Err(GraphError::ExpectedArray);
                }
                for array in &arrays[1..] {
                    if self.expression_type(array, visiting)? != ty {
                        return Err(GraphError::ExpressionType);
                    }
                }
                Ok(ty)
            }
            Expression::CountIf(condition) => {
                if condition.aggregate()
                    || self.expression_type(condition, visiting)?
                        != ValueType::Scalar(SqlType::Bool)
                {
                    return Err(GraphError::AggregatePlacement);
                }
                Ok(ValueType::Scalar(SqlType::Int64))
            }
            Expression::Sum { value, condition } => {
                if value.aggregate() {
                    return Err(GraphError::AggregatePlacement);
                }
                let ty = self.expression_type(value, visiting)?;
                if !matches!(
                    ty,
                    ValueType::Scalar(SqlType::Int64 | SqlType::UInt32 | SqlType::Float64)
                ) {
                    return Err(GraphError::ExpressionType);
                }
                if let Some(condition) = condition
                    && (condition.aggregate()
                        || self.expression_type(condition, visiting)?
                            != ValueType::Scalar(SqlType::Bool))
                {
                    return Err(GraphError::AggregatePlacement);
                }
                Ok(ty)
            }
            Expression::Column(column) => {
                let ty = self.column_type(*column, visiting)?;
                if let Body::Select { operation, .. } = &self.block(column.relation.block)?.body
                    && operation.expands(*column)
                {
                    let ValueType::Array(element) = ty else {
                        return Err(GraphError::ExpectedArray);
                    };
                    return Ok(*element);
                }
                Ok(ty)
            }
            Expression::In(left, right) => {
                let element = self.expression_type(left, visiting)?;
                let ValueType::Array(expected) = self.expression_type(right, visiting)? else {
                    return Err(GraphError::ExpressionType);
                };
                if element != *expected {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
            Expression::Equal(left, right)
            | Expression::Greater(left, right)
            | Expression::And(left, right)
            | Expression::StartsWith(left, right) => {
                let left = self.expression_type(left, visiting)?;
                let right = self.expression_type(right, visiting)?;
                if left != right
                    || matches!(expression, Expression::And(..))
                        && left != ValueType::Scalar(SqlType::Bool)
                    || matches!(expression, Expression::StartsWith(..))
                        && left != ValueType::Scalar(SqlType::String)
                {
                    return Err(GraphError::ExpressionType);
                }
                Ok(ValueType::Scalar(SqlType::Bool))
            }
        }
    }

    fn column_type(
        &self,
        column: ColumnRef<'catalog>,
        visiting: &mut HashSet<OutputId>,
    ) -> Result<ValueType> {
        match column.port {
            Port::Stored(name) => {
                let Source::Stored(table) = self.relation(column.relation)?.source else {
                    return Err(GraphError::MissingOutput);
                };
                let ty = self
                    .catalog
                    .table_column_type(table, name)
                    .ok_or_else(|| GraphError::UnknownStored(format!("{table}.{name}")))?;
                Ok(ValueType::Scalar(match ty {
                    ontology::DataType::Int => SqlType::Int64,
                    ontology::DataType::Bool => SqlType::Bool,
                    ontology::DataType::Float => SqlType::Float64,
                    ontology::DataType::Date => SqlType::Date,
                    ontology::DataType::DateTime => SqlType::Timestamp {
                        precision: 6,
                        timezone: None,
                    },
                    _ => SqlType::String,
                }))
            }
            Port::Output(output) => self.output_type(output, visiting),
        }
    }

    fn output_type(&self, output: OutputId, visiting: &mut HashSet<OutputId>) -> Result<ValueType> {
        if !visiting.insert(output) {
            return Err(GraphError::RecursiveType);
        }
        let result = match &self.block(output.block)?.body {
            Body::Select { outputs, .. } => {
                self.expression_type(&outputs[output.slot].value, visiting)
            }
            Body::UnionAll { arms, .. } => {
                let first = self.output_type(
                    OutputId {
                        block: arms[0],
                        slot: output.slot,
                    },
                    visiting,
                )?;
                for arm in &arms[1..] {
                    if self.output_type(
                        OutputId {
                            block: *arm,
                            slot: output.slot,
                        },
                        visiting,
                    )? != first
                    {
                        return Err(GraphError::UnionType);
                    }
                }
                Ok(first)
            }
        };
        visiting.remove(&output);
        result
    }

    fn check_union_types(&self) -> Result<()> {
        for (slot, block) in self.blocks.iter().enumerate() {
            if let Body::UnionAll { labels, .. } = &block.body {
                for output in 0..labels.len() {
                    self.output_type(
                        OutputId {
                            block: BlockId {
                                owner: self.owner,
                                slot,
                            },
                            slot: output,
                        },
                        &mut HashSet::new(),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn validate_lowered(&self, root: BlockId) -> Result<()> {
        self.validate(root, |block, outputs, operation| {
            if outputs.is_empty() {
                return Err(GraphError::EmptyProjection);
            }
            for output in outputs {
                output
                    .value
                    .columns(&mut |column| self.check_column(block, column))?;
                self.expression_type(&output.value, &mut HashSet::new())?;
            }
            let mut used = HashSet::new();
            let available = self.operation_columns(block, operation, &mut used)?;
            let aggregate_input = operation
                .aggregate_input()
                .map(|input| self.operation_columns(block, input, &mut HashSet::new()))
                .transpose()?;
            let Body::Select { relations, .. } = &self.block(block)?.body else {
                unreachable!()
            };
            if used.len() != relations.len() {
                return Err(GraphError::JoinShape);
            }
            for output in outputs {
                self.check_projection(&output.value, &available, aggregate_input.as_deref())?;
            }
            Ok(())
        })?;
        self.check_union_types()
    }

    pub fn render(&self, root: BlockId) -> Result<String> {
        self.validate_lowered(root)?;
        self.render_block(root, true)
    }

    fn check_projection(
        &self,
        expression: &Expression<'catalog>,
        available: &[ColumnRef<'catalog>],
        aggregate_input: Option<&[ColumnRef<'catalog>]>,
    ) -> Result<()> {
        match expression {
            Expression::Count | Expression::CountIf(_) | Expression::Sum { .. } => {
                let input = aggregate_input.ok_or(GraphError::AggregatePlacement)?;
                expression.columns(&mut |column| {
                    if input.contains(&column) {
                        Ok(())
                    } else {
                        Err(GraphError::OperationVisibility)
                    }
                })
            }
            Expression::Equal(left, right)
            | Expression::And(left, right)
            | Expression::In(left, right)
            | Expression::Greater(left, right)
            | Expression::StartsWith(left, right) => {
                self.check_projection(left, available, aggregate_input)?;
                self.check_projection(right, available, aggregate_input)
            }
            Expression::Tuple(values) | Expression::Array(values) | Expression::Concat(values) => {
                for value in values {
                    self.check_projection(value, available, aggregate_input)?;
                }
                Ok(())
            }
            Expression::Field { tuple, .. } => {
                self.check_projection(tuple, available, aggregate_input)
            }
            Expression::Keep { condition, value } => {
                self.check_projection(condition, available, aggregate_input)?;
                self.check_projection(value, available, aggregate_input)
            }
            _ => expression.columns(&mut |column| {
                if available.contains(&column) {
                    Ok(())
                } else {
                    Err(GraphError::OperationVisibility)
                }
            }),
        }
    }

    fn render_block(&self, id: BlockId, public: bool) -> Result<String> {
        let block = self.block(id)?;
        let mut sql = String::new();
        if !block.definitions.is_empty() {
            sql.push_str(
                if block
                    .definitions
                    .iter()
                    .any(|definition| definition.recursive)
                {
                    "WITH RECURSIVE "
                } else {
                    "WITH "
                },
            );
            let definitions = block
                .definitions
                .iter()
                .enumerate()
                .map(|(slot, definition)| {
                    Ok(format!(
                        "{} AS ({})",
                        definition_name(DefinitionId { block: id, slot }),
                        self.render_block(definition.body, false)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&definitions.join(", "));
            sql.push(' ');
        }
        match &block.body {
            Body::Select {
                outputs, operation, ..
            } => {
                let projection = outputs
                    .iter()
                    .enumerate()
                    .map(|(slot, output)| {
                        let name = if public {
                            quoted(&output.label)
                        } else {
                            output_name(OutputId { block: id, slot })
                        };
                        Ok(format!(
                            "{} AS {name}",
                            self.render_expression_at(&output.value, true)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&format!("SELECT {}", projection.join(", ")));
                let mut needed = Vec::new();
                for output in outputs {
                    collect_columns(&output.value, &mut needed)?;
                }
                let (input, tail, groups) = self.render_operation(id, operation, &needed)?;
                sql.push_str(&format!(" FROM ({input}) AS q"));
                if !groups.is_empty() {
                    sql.push_str(&format!(
                        " GROUP BY {}",
                        groups
                            .iter()
                            .map(|column| format!("q.{}", value_name(*column)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                sql.push_str(&tail);
            }
            Body::UnionAll { arms, labels } => {
                let arms = arms
                    .iter()
                    .map(|arm| {
                        let projection = labels
                            .iter()
                            .enumerate()
                            .map(|(slot, label)| {
                                let name = if public {
                                    quoted(label)
                                } else {
                                    output_name(OutputId { block: id, slot })
                                };
                                format!(
                                    "u.{} AS {name}",
                                    output_name(OutputId { block: *arm, slot })
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        Ok(format!(
                            "(SELECT {projection} FROM ({}) AS u)",
                            self.render_block(*arm, false)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&arms.join(" UNION ALL "));
            }
        }
        Ok(sql)
    }

    fn render_column(&self, column: ColumnRef<'catalog>) -> Result<String> {
        let port = match column.port {
            Port::Stored(name) => quoted(name),
            Port::Output(output) => {
                self.output_label(output)?;
                output_name(output)
            }
        };
        Ok(format!("{}.{port}", relation_name(column.relation)))
    }

    fn render_operation(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        needed: &[ColumnRef<'catalog>],
    ) -> Result<(String, String, Vec<ColumnRef<'catalog>>)> {
        use Relational::*;
        let columns = needed;
        let selection = |columns: &[ColumnRef<'catalog>]| {
            if columns.is_empty() {
                "1 AS _unit".into()
            } else {
                columns
                    .iter()
                    .map(|column| format!("q.{}", value_name(*column)))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        Ok(match operation {
            One => ("SELECT 1 AS _unit".into(), String::new(), vec![]),
            Source { relation, read } => {
                let source = match self.relation(*relation)?.source {
                    crate::query_graph::Source::Stored(table) => quoted(table),
                    crate::query_graph::Source::Derived(body) => {
                        format!("({})", self.render_block(body, false)?)
                    }
                    crate::query_graph::Source::Definition(definition) => {
                        definition_name(definition)
                    }
                };
                let projection = columns
                    .iter()
                    .map(|column| {
                        Ok(format!(
                            "{} AS {}",
                            self.render_column(*column)?,
                            value_name(*column)
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let projection = if projection.is_empty() {
                    "1 AS _unit".into()
                } else {
                    projection
                };
                (
                    format!(
                        "SELECT {projection} FROM {source} AS {}{}",
                        relation_name(*relation),
                        if matches!(read, ReadMode::Current) {
                            " FINAL"
                        } else {
                            ""
                        }
                    ),
                    String::new(),
                    vec![],
                )
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left_columns = self.operation_columns(block, left, &mut HashSet::new())?;
                let right_columns = self.operation_columns(block, right, &mut HashSet::new())?;
                let mut required = needed.to_vec();
                collect_columns(condition, &mut required)?;
                let left_needed = required
                    .iter()
                    .filter(|column| left_columns.contains(column))
                    .copied()
                    .collect::<Vec<_>>();
                let right_needed = required
                    .iter()
                    .filter(|column| right_columns.contains(column))
                    .copied()
                    .collect::<Vec<_>>();
                let left = self.materialize_operation(block, left, &left_needed)?;
                let right = self.materialize_operation(block, right, &right_needed)?;
                let projection = columns
                    .iter()
                    .map(|column| {
                        format!(
                            "{}.{}",
                            if left_columns.contains(column) {
                                "l"
                            } else {
                                "r"
                            },
                            value_name(*column)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let projection = if projection.is_empty() {
                    "1 AS _unit".into()
                } else {
                    projection
                };
                let condition = self.render_expression_with(condition, &|column| {
                    let side = if left_columns.contains(&column) {
                        "l"
                    } else if right_columns.contains(&column) {
                        "r"
                    } else {
                        return Err(GraphError::OperationVisibility);
                    };
                    Ok(format!("{side}.{}", value_name(column)))
                })?;
                let keyword = match kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Cross => "CROSS JOIN",
                    JoinKind::Semi => "LEFT SEMI JOIN",
                };
                (
                    format!(
                        "SELECT {projection} FROM ({left}) AS l {keyword} ({right}) AS r{}",
                        if matches!(kind, JoinKind::Cross) {
                            String::new()
                        } else {
                            format!(" ON {condition}")
                        }
                    ),
                    String::new(),
                    vec![],
                )
            }
            Filter { input, predicate } => {
                let mut required = needed.to_vec();
                collect_columns(predicate, &mut required)?;
                if let Source { relation, read } = input.as_ref()
                    && let crate::query_graph::Source::Stored(table) =
                        self.relation(*relation)?.source
                {
                    let projection = needed
                        .iter()
                        .map(|column| {
                            Ok(format!(
                                "{} AS {}",
                                self.render_column(*column)?,
                                value_name(*column)
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    let projection = if projection.is_empty() {
                        "1 AS _unit".into()
                    } else {
                        projection
                    };
                    return Ok((
                        format!(
                            "SELECT {projection} FROM {} AS {}{} WHERE {}",
                            quoted(table),
                            relation_name(*relation),
                            if matches!(read, ReadMode::Current) {
                                " FINAL"
                            } else {
                                ""
                            },
                            self.render_expression_at(predicate, false)?
                        ),
                        String::new(),
                        vec![],
                    ));
                }
                let input = self.materialize_operation(block, input, &required)?;
                (
                    format!(
                        "SELECT {} FROM ({input}) AS q WHERE {}",
                        selection(columns),
                        self.render_expression_at(predicate, true)?
                    ),
                    String::new(),
                    vec![],
                )
            }
            Aggregate { input, groups } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, groups.iter().copied());
                (
                    self.materialize_operation(block, input, &required)?,
                    String::new(),
                    groups.clone(),
                )
            }
            Sort { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().map(|(column, _)| *column));
                let (sql, tail, groups) = self.render_operation(block, input, &required)?;
                if !tail.is_empty() {
                    return Err(GraphError::JoinShape);
                }
                let order = keys
                    .iter()
                    .map(|(column, descending)| {
                        format!(
                            "q.{} {}",
                            value_name(*column),
                            if *descending { "DESC" } else { "ASC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (sql, format!(" ORDER BY {order}"), groups)
            }
            FirstBy { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().copied());
                let (sql, tail, groups) = self.render_operation(block, input, &required)?;
                (
                    sql,
                    format!(
                        "{tail} LIMIT 1 BY {}",
                        keys.iter()
                            .map(|column| format!("q.{}", value_name(*column)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    groups,
                )
            }
            Limit { input, count } => {
                let (sql, tail, groups) = self.render_operation(block, input, needed)?;
                (sql, format!("{tail} LIMIT {count}"), groups)
            }
            Expand { input, column } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, [*column]);
                let sql = self.materialize_operation(block, input, &required)?;
                let projection = required
                    .iter()
                    .map(|value| {
                        if value == column {
                            format!("arrayJoin(q.{0}) AS {0}", value_name(*value))
                        } else {
                            format!("q.{}", value_name(*value))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    format!("SELECT {projection} FROM ({sql}) AS q"),
                    String::new(),
                    vec![],
                )
            }
            Latest { requirement, .. } => match *requirement {},
        })
    }

    fn materialize_operation(
        &self,
        block: BlockId,
        operation: &LoweredOperation<'catalog>,
        needed: &[ColumnRef<'catalog>],
    ) -> Result<String> {
        if operation.aggregate_input().is_some() {
            return Err(GraphError::AggregateBoundary);
        }
        let (sql, tail, groups) = self.render_operation(block, operation, needed)?;
        if tail.is_empty() && groups.is_empty() {
            return Ok(sql);
        }
        let projection = if needed.is_empty() {
            "1 AS _unit".into()
        } else {
            needed
                .iter()
                .map(|column| format!("q.{}", value_name(*column)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let group = if groups.is_empty() {
            String::new()
        } else {
            format!(
                " GROUP BY {}",
                groups
                    .iter()
                    .map(|column| format!("q.{}", value_name(*column)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        Ok(format!(
            "SELECT {projection} FROM ({sql}) AS q{group}{tail}"
        ))
    }

    fn render_expression_at(
        &self,
        expression: &Expression<'catalog>,
        materialized: bool,
    ) -> Result<String> {
        self.render_expression_with(expression, &|column| {
            if materialized {
                Ok(format!("q.{}", value_name(column)))
            } else {
                self.render_column(column)
            }
        })
    }

    fn render_expression_with(
        &self,
        expression: &Expression<'catalog>,
        column: &dyn Fn(ColumnRef<'catalog>) -> Result<String>,
    ) -> Result<String> {
        Ok(match expression {
            Expression::Column(reference) => column(*reference)?,
            Expression::Integer(value) => value.to_string(),
            Expression::Boolean(value) => value.to_string(),
            Expression::Text(value) => {
                format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
            }
            Expression::Count => "COUNT(*)".into(),
            Expression::CountIf(condition) => format!(
                "countIf({})",
                self.render_expression_with(condition, column)?
            ),
            Expression::Sum { value, condition } => match condition {
                Some(condition) => format!(
                    "sumIf({}, {})",
                    self.render_expression_with(value, column)?,
                    self.render_expression_with(condition, column)?
                ),
                None => format!("SUM({})", self.render_expression_with(value, column)?),
            },
            Expression::Tuple(values) | Expression::Array(values) | Expression::Concat(values) => {
                let function = match expression {
                    Expression::Tuple(_) => "tuple",
                    Expression::Array(_) => "array",
                    _ => "arrayConcat",
                };
                let values = values
                    .iter()
                    .map(|value| self.render_expression_with(value, column))
                    .collect::<Result<Vec<_>>>()?;
                format!("{function}({})", values.join(", "))
            }
            Expression::Field { tuple, index } => format!(
                "tupleElement({}, {})",
                self.render_expression_with(tuple, column)?,
                index + 1
            ),
            Expression::Keep { condition, value } => format!(
                "arrayFilter(_keep -> {}, [{}])",
                self.render_expression_with(condition, column)?,
                self.render_expression_with(value, column)?
            ),
            Expression::Integers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expression::StartsWith(left, right) => format!(
                "startsWith({}, {})",
                self.render_expression_with(left, column)?,
                self.render_expression_with(right, column)?
            ),
            Expression::Equal(left, right)
            | Expression::And(left, right)
            | Expression::In(left, right)
            | Expression::Greater(left, right) => {
                let operator = match expression {
                    Expression::Equal(..) => "=",
                    Expression::Greater(..) => ">",
                    Expression::In(..) => "IN",
                    _ => "AND",
                };
                format!(
                    "({} {operator} {})",
                    self.render_expression_with(left, column)?,
                    self.render_expression_with(right, column)?
                )
            }
        })
    }
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn extend_columns<'a>(
    needed: &mut Vec<ColumnRef<'a>>,
    columns: impl IntoIterator<Item = ColumnRef<'a>>,
) {
    for column in columns {
        if !needed.contains(&column) {
            needed.push(column);
        }
    }
}

fn collect_columns<'a>(expression: &Expression<'a>, needed: &mut Vec<ColumnRef<'a>>) -> Result<()> {
    expression.columns(&mut |column| {
        extend_columns(needed, [column]);
        Ok(())
    })
}
fn value_name(column: ColumnRef<'_>) -> String {
    let port = match column.port {
        Port::Stored(name) => format!("stored_{name}"),
        Port::Output(output) => output_name(output),
    };
    quoted(&format!("{}_{}", relation_name(column.relation), port))
}
fn relation_name(id: RelationId) -> String {
    format!("r{}_{}", id.block.slot, id.slot)
}
fn output_name(id: OutputId) -> String {
    format!("o{}_{}", id.block.slot, id.slot)
}
fn definition_name(id: DefinitionId) -> String {
    format!("d{}_{}", id.block.slot, id.slot)
}

impl<M: QueryDataModel + ?Sized, E: std::fmt::Debug, O: std::fmt::Debug> QueryGraph<'_, M, E, O> {
    pub fn explain(&self, root: BlockId) -> Result<String> {
        self.validate(root, |_, _, _| Ok(()))?;
        let mut text = String::new();
        self.explain_block(root, 0, &mut text)?;
        Ok(text)
    }

    fn explain_block(&self, id: BlockId, depth: usize, text: &mut String) -> Result<()> {
        use std::fmt::Write;
        let block = self.block(id)?;
        let indent = "  ".repeat(depth);
        writeln!(text, "{indent}(Block b{}", id.slot).unwrap();
        for (slot, definition) in block.definitions.iter().enumerate() {
            writeln!(
                text,
                "{indent}  (CTE {} {:?} recursive={}",
                definition_name(DefinitionId { block: id, slot }),
                definition.hint,
                definition.recursive
            )
            .unwrap();
            self.explain_block(definition.body, depth + 2, text)?;
            writeln!(text, "{indent}  )").unwrap();
        }
        match &block.body {
            Body::Select {
                relations,
                outputs,
                operation,
            } => {
                writeln!(text, "{indent}  (Operation {operation:#?})").unwrap();
                for (slot, relation) in relations.iter().enumerate() {
                    writeln!(
                        text,
                        "{indent}  (Relation {} {:?}",
                        relation_name(RelationId { block: id, slot }),
                        relation.hint
                    )
                    .unwrap();
                    match relation.source {
                        Source::Stored(table) => {
                            writeln!(text, "{indent}    (Scan {table})").unwrap()
                        }
                        Source::Derived(body) => self.explain_block(body, depth + 2, text)?,
                        Source::Definition(definition) => writeln!(
                            text,
                            "{indent}    (Reference {})",
                            definition_name(definition)
                        )
                        .unwrap(),
                    }
                    writeln!(text, "{indent}  )").unwrap();
                }
                for (slot, output) in outputs.iter().enumerate() {
                    writeln!(
                        text,
                        "{indent}  (Output {} {:?} {:?})",
                        output_name(OutputId { block: id, slot }),
                        output.label,
                        output.value
                    )
                    .unwrap();
                }
            }
            Body::UnionAll { arms, labels } => {
                writeln!(text, "{indent}  (UnionAll {labels:?}").unwrap();
                for arm in arms {
                    self.explain_block(*arm, depth + 2, text)?;
                }
                writeln!(text, "{indent}  )").unwrap();
            }
        }
        writeln!(text, "{indent})").unwrap();
        Ok(())
    }
}
