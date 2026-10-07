use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;
use query_data_model::storage::{StoredColumnRef, StoredTableRef};

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
    Stored(StoredColumnRef<'catalog>),
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
    #[error("operation requires a projection")]
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
    #[error("query requires at least one output")]
    EmptyProjection,
    #[error("UNION outputs have incompatible types")]
    UnionType,
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
    #[error("replacement must preserve output identities, labels, and types")]
    OutputContract,
    #[error("query has already been constructed")]
    FinishedQuery,
    #[error("unsupported input: {0}")]
    UnsupportedInput(String),
}

type Result<T> = std::result::Result<T, GraphError>;

impl From<GraphError> for crate::error::QueryError {
    fn from(error: GraphError) -> Self {
        Self::Lowering(error.to_string())
    }
}

pub struct QueryGraph<'catalog, M: QueryDataModel + ?Sized, L> {
    catalog: &'catalog M,
    owner: u64,
    blocks: Vec<Block<'catalog, L>>,
}

struct Block<'a, L> {
    owner: Option<BlockId>,
    visible: HashSet<DefinitionId>,
    required: HashSet<DefinitionId>,
    definitions: Vec<Definition>,
    relations: Vec<Relation<'a>>,
    operation: Option<QueryOperation<'a, L>>,
}

pub struct QueryOperation<'a, L> {
    block: BlockId,
    outputs: Vec<Output<'a>>,
    kind: QueryKind<'a, L>,
}

enum QueryKind<'a, L> {
    Project(Relational<'a, L>),
    UnionAll(Vec<BlockId>),
}

pub struct Output<'a> {
    label: String,
    data_type: ValueType,
    value: Option<Expression<'a>>,
}

impl<'a> Output<'a> {
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn data_type(&self) -> &ValueType {
        &self.data_type
    }
    pub fn value(&self) -> Option<&Expression<'a>> {
        self.value.as_ref()
    }
}

struct Definition {
    hint: String,
    body: BlockId,
}

pub struct Relation<'catalog> {
    pub hint: String,
    pub source: Source<'catalog>,
    pub input: Option<ScanInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanInput {
    Node(usize),
    Relationship(usize),
}

#[derive(Debug, Clone, Copy)]
pub enum Source<'catalog> {
    Stored(StoredTableRef<'catalog>),
    Derived(BlockId),
    Definition(DefinitionId),
}

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub fn new(catalog: &'a M) -> Self {
        Self {
            catalog,
            owner: NEXT_GRAPH.fetch_add(1, Ordering::Relaxed),
            blocks: vec![],
        }
    }

    pub fn query(&mut self) -> BlockId {
        self.allocate(HashSet::new())
    }

    pub fn query_in(&mut self, scope: BlockId) -> Result<BlockId> {
        let visible = self.visible_definitions(scope)?;
        Ok(self.allocate(visible))
    }

    pub fn scan(
        &mut self,
        block: BlockId,
        table: &str,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        let table = self
            .catalog
            .stored_table(table)
            .ok_or_else(|| GraphError::UnknownStored(table.into()))?;
        self.scan_stored(block, table, hint)
    }

    pub fn scan_stored(
        &mut self,
        block: BlockId,
        table: StoredTableRef<'a>,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        if self
            .catalog
            .stored_table(table.name())
            .is_none_or(|owned| owned.id() != table.id())
        {
            return Err(GraphError::ForeignGraph);
        }
        self.add_relation(block, Source::Stored(table), hint.into())
    }

    pub fn derive(
        &mut self,
        block: BlockId,
        body: BlockId,
        hint: impl Into<String>,
    ) -> Result<RelationId> {
        self.require_building(block)?;
        self.require_attachment(block, body)?;
        self.attach(block, body)?;
        self.add_relation(block, Source::Derived(body), hint.into())
    }

    pub fn define(
        &mut self,
        block: BlockId,
        body: BlockId,
        hint: impl Into<String>,
    ) -> Result<DefinitionId> {
        self.require_building(block)?;
        self.require_attachment(block, body)?;
        self.attach(block, body)?;
        let definitions = &mut self.block_mut(block)?.definitions;
        let id = DefinitionId {
            block,
            slot: definitions.len(),
        };
        definitions.push(Definition {
            hint: hint.into(),
            body,
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
        self.require_building(block)?;
        if !self.visible_definitions(block)?.contains(&definition) {
            return Err(GraphError::DefinitionVisibility);
        }
        if definition.block != block {
            self.block_mut(block)?.required.insert(definition);
        }
        self.add_relation(block, Source::Definition(definition), hint.into())
    }

    pub fn union_all(&mut self, arms: Vec<BlockId>, labels: Vec<String>) -> Result<BlockId> {
        let first = *arms.first().ok_or(GraphError::UnionShape)?;
        if labels.is_empty() || self.query_operation(first)?.outputs.len() != labels.len() {
            return Err(GraphError::UnionShape);
        }
        let types = self
            .query_operation(first)?
            .outputs
            .iter()
            .map(|output| output.data_type.clone())
            .collect::<Vec<_>>();
        let mut visible = self.block(first)?.visible.clone();
        let mut seen = HashSet::new();
        for arm in &arms {
            let block = self.block(*arm)?;
            if block.owner.is_some() || !seen.insert(*arm) {
                return Err(GraphError::BlockOwnership);
            }
            let operation = self.query_operation(*arm)?;
            if operation.outputs.len() != labels.len() {
                return Err(GraphError::UnionShape);
            }
            if operation
                .outputs
                .iter()
                .zip(&types)
                .any(|(output, ty)| output.data_type != *ty)
            {
                return Err(GraphError::UnionType);
            }
            visible.retain(|definition| block.visible.contains(definition));
        }
        for arm in &arms {
            if !self.block(*arm)?.required.is_subset(&visible) {
                return Err(GraphError::DefinitionVisibility);
            }
        }
        let block = self.allocate(visible);
        for arm in &arms {
            self.attach(block, *arm)?;
        }
        let outputs = labels
            .into_iter()
            .zip(types)
            .map(|(label, data_type)| Output {
                label,
                data_type,
                value: None,
            })
            .collect();
        self.block_mut(block)?.operation = Some(QueryOperation {
            block,
            outputs,
            kind: QueryKind::UnionAll(arms),
        });
        Ok(block)
    }

    pub fn stored_column(&self, relation: RelationId, name: &str) -> Result<ColumnRef<'a>> {
        let Source::Stored(table) = self.relation(relation)?.source else {
            return Err(GraphError::MissingOutput);
        };
        let column = table
            .column(name)
            .ok_or_else(|| GraphError::UnknownStored(format!("{}.{name}", table.name())))?;
        self.stored_port(relation, column)
    }

    pub fn column(&self, relation: RelationId, name: &str) -> Result<ColumnRef<'a>> {
        let Some(body) = self.relation_body(relation)? else {
            return self.stored_column(relation, name);
        };
        let output = self
            .outputs(body)?
            .find(|output| self.output_label(*output).is_ok_and(|label| label == name))
            .ok_or(GraphError::MissingOutput)?;
        self.output_column(relation, output)
    }

    pub fn stored_port(
        &self,
        relation: RelationId,
        column: StoredColumnRef<'a>,
    ) -> Result<ColumnRef<'a>> {
        let Source::Stored(table) = self.relation(relation)?.source else {
            return Err(GraphError::MissingOutput);
        };
        if table.id() != column.table().id() {
            return Err(GraphError::OutsideBlock);
        }
        Ok(ColumnRef {
            relation,
            port: Port::Stored(column),
        })
    }

    pub fn output_column(&self, relation: RelationId, output: OutputId) -> Result<ColumnRef<'a>> {
        self.output(output)?;
        if self.relation_body(relation)? != Some(output.block) {
            return Err(GraphError::MissingOutput);
        }
        Ok(ColumnRef {
            relation,
            port: Port::Output(output),
        })
    }

    pub fn check_column(&self, block: BlockId, column: ColumnRef<'a>) -> Result<()> {
        self.block(block)?;
        if column.relation.block != block {
            return Err(GraphError::OutsideBlock);
        }
        match column.port {
            Port::Stored(stored) => {
                self.stored_port(column.relation, stored)?;
            }
            Port::Output(output) => {
                self.output_column(column.relation, output)?;
            }
        }
        Ok(())
    }

    pub fn outputs(
        &self,
        block: BlockId,
    ) -> Result<impl Iterator<Item = OutputId> + use<'a, M, L>> {
        let count = self.query_operation(block)?.outputs.len();
        Ok((0..count).map(move |slot| OutputId { block, slot }))
    }

    pub fn output(&self, output: OutputId) -> Result<&Output<'a>> {
        self.query_operation(output.block)?
            .outputs
            .get(output.slot)
            .ok_or(GraphError::MissingOutput)
    }

    pub fn output_label(&self, output: OutputId) -> Result<&str> {
        Ok(self.output(output)?.label())
    }

    pub fn projection(&self, output: OutputId) -> Result<&Expression<'a>> {
        self.output(output)?
            .value()
            .ok_or(GraphError::ExpectedSelect)
    }

    pub fn union_inputs(&self, output: OutputId) -> Result<Vec<OutputId>> {
        self.output(output)?;
        let arms = self
            .union_arms(output.block)?
            .ok_or(GraphError::UnionShape)?;
        Ok(arms
            .iter()
            .map(|block| OutputId {
                block: *block,
                slot: output.slot,
            })
            .collect())
    }

    pub fn union_arms(&self, block: BlockId) -> Result<Option<&[BlockId]>> {
        Ok(match &self.query_operation(block)?.kind {
            QueryKind::UnionAll(arms) => Some(arms),
            QueryKind::Project(_) => None,
        })
    }

    pub fn operation(&self, block: BlockId) -> Result<&Relational<'a, L>> {
        match &self.query_operation(block)?.kind {
            QueryKind::Project(input) => Ok(input),
            QueryKind::UnionAll(_) => Err(GraphError::ExpectedSelect),
        }
    }

    pub fn catalog(&self) -> &'a M {
        self.catalog
    }

    pub fn relations(
        &self,
        block: BlockId,
    ) -> Result<impl Iterator<Item = RelationId> + use<'a, M, L>> {
        let count = self.block(block)?.relations.len();
        Ok((0..count).map(move |slot| RelationId { block, slot }))
    }

    pub fn blocks(&self) -> impl Iterator<Item = BlockId> + use<'a, M, L> {
        let owner = self.owner;
        (0..self.blocks.len()).map(move |slot| BlockId { owner, slot })
    }

    pub fn relation(&self, relation: RelationId) -> Result<&Relation<'a>> {
        self.block(relation.block)?
            .relations
            .get(relation.slot)
            .ok_or(GraphError::MissingOutput)
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

    fn query_operation(&self, block: BlockId) -> Result<&QueryOperation<'a, L>> {
        self.block(block)?
            .operation
            .as_ref()
            .ok_or(GraphError::EmptyProjection)
    }

    fn definition(&self, id: DefinitionId) -> Result<&Definition> {
        self.block(id.block)?
            .definitions
            .get(id.slot)
            .ok_or(GraphError::DefinitionVisibility)
    }

    fn relation_body(&self, relation: RelationId) -> Result<Option<BlockId>> {
        Ok(match self.relation(relation)?.source {
            Source::Stored(_) => None,
            Source::Derived(body) => Some(body),
            Source::Definition(definition) => Some(self.definition(definition)?.body),
        })
    }

    fn visible_definitions(&self, block: BlockId) -> Result<HashSet<DefinitionId>> {
        let declaration = self.block(block)?;
        let mut visible = declaration.visible.clone();
        visible.extend((0..declaration.definitions.len()).map(|slot| DefinitionId { block, slot }));
        Ok(visible)
    }

    fn require_building(&self, block: BlockId) -> Result<()> {
        if self.block(block)?.operation.is_some() {
            return Err(GraphError::FinishedQuery);
        }
        Ok(())
    }

    fn require_attachment(&self, parent: BlockId, child: BlockId) -> Result<()> {
        self.query_operation(child)?;
        let declaration = self.block(child)?;
        if declaration.owner.is_some() {
            return Err(GraphError::BlockOwnership);
        }
        let mut ancestor = Some(parent);
        while let Some(block) = ancestor {
            if block == child {
                return Err(GraphError::BlockOwnership);
            }
            ancestor = self.block(block)?.owner;
        }
        if !declaration
            .required
            .is_subset(&self.visible_definitions(parent)?)
        {
            return Err(GraphError::DefinitionVisibility);
        }
        Ok(())
    }

    fn attach(&mut self, parent: BlockId, child: BlockId) -> Result<()> {
        let required = self
            .block(child)?
            .required
            .iter()
            .filter(|definition| definition.block != parent)
            .copied()
            .collect::<Vec<_>>();
        self.block_mut(parent)?.required.extend(required);
        self.block_mut(child)?.owner = Some(parent);
        Ok(())
    }

    fn block(&self, id: BlockId) -> Result<&Block<'a, L>> {
        if id.owner != self.owner {
            return Err(GraphError::ForeignGraph);
        }
        self.blocks.get(id.slot).ok_or(GraphError::BlockOwnership)
    }

    fn block_mut(&mut self, id: BlockId) -> Result<&mut Block<'a, L>> {
        if id.owner != self.owner {
            return Err(GraphError::ForeignGraph);
        }
        self.blocks
            .get_mut(id.slot)
            .ok_or(GraphError::BlockOwnership)
    }

    fn allocate(&mut self, visible: HashSet<DefinitionId>) -> BlockId {
        let id = BlockId {
            owner: self.owner,
            slot: self.blocks.len(),
        };
        self.blocks.push(Block {
            owner: None,
            visible,
            required: HashSet::new(),
            definitions: vec![],
            relations: vec![],
            operation: None,
        });
        id
    }

    fn add_relation(
        &mut self,
        block: BlockId,
        source: Source<'a>,
        hint: String,
    ) -> Result<RelationId> {
        self.require_building(block)?;
        let relations = &mut self.block_mut(block)?.relations;
        let id = RelationId {
            block,
            slot: relations.len(),
        };
        relations.push(Relation {
            hint,
            source,
            input: None,
        });
        Ok(id)
    }
}

mod construction;
mod explain;
mod expression;
mod lower;
mod outputs;
mod plan;
mod relational;
mod render;
mod types;
mod walk;

pub use expression::{Expression, ValueType};
pub use relational::{
    JoinKind, LatestRows, LoweredOperation, OperationKind, PhysicalOperation, ReadMode, Relational,
};
pub use walk::BlockView;
