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

impl From<GraphError> for crate::error::QueryError {
    fn from(error: GraphError) -> Self {
        Self::Lowering(error.to_string())
    }
}

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
        let table = self
            .catalog
            .stored_table(table)
            .ok_or_else(|| GraphError::UnknownStored(table.into()))?;
        self.scan_stored(block, table, hint)
    }

    pub fn scan_stored(
        &mut self,
        block: BlockId,
        table: StoredTableRef<'catalog>,
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
        let column = table
            .column(name)
            .ok_or_else(|| GraphError::UnknownStored(format!("{}.{name}", table.name())))?;
        self.stored_port(relation, column)
    }

    pub fn column(&self, relation: RelationId, name: &str) -> Result<ColumnRef<'catalog>> {
        let body = match self.relation(relation)?.source {
            Source::Stored(_) => return self.stored_column(relation, name),
            Source::Derived(body) => body,
            Source::Definition(definition) => self.definition(definition)?.body,
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
        column: StoredColumnRef<'catalog>,
    ) -> Result<ColumnRef<'catalog>> {
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
            Port::Stored(stored) => {
                self.stored_port(column.relation, stored)?;
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

    pub fn union_arms(&self, block: BlockId) -> Result<Option<&[BlockId]>> {
        Ok(match &self.block(block)?.body {
            Body::UnionAll { arms, .. } => Some(arms),
            _ => None,
        })
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

    pub fn catalog(&self) -> &'catalog M {
        self.catalog
    }

    pub fn relations(&self, block: BlockId) -> Result<impl Iterator<Item = RelationId>> {
        let Body::Select { relations, .. } = &self.block(block)?.body else {
            return Err(GraphError::ExpectedSelect);
        };
        Ok((0..relations.len()).map(move |slot| RelationId { block, slot }))
    }

    pub fn blocks(&self) -> impl Iterator<Item = BlockId> {
        let owner = self.owner;
        (0..self.blocks.len()).map(move |slot| BlockId { owner, slot })
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
        relations.push(Relation {
            hint,
            source,
            input: None,
        });
        Ok(id)
    }
}

mod explain;
mod expression;
mod lower;
mod outputs;
mod plan;
mod relational;
mod render;
mod validate;
mod walk;

pub use walk::BlockView;

pub use expression::{Expression, ValueType};
pub use relational::{
    JoinKind, LatestRows, LoweredOperation, PhysicalOperation, ReadMode, Relational,
};
