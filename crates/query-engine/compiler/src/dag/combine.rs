use query_data_model::DataType;
use query_data_model::plan::combine::Position;
use query_data_model::plan::logical::Order;

use super::StageId;

/// Rows of two stages that match on every column pair; `left` belongs to the earlier stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombineJoin {
    pub left: Vec<Position>,
    pub right: Vec<Position>,
    /// The declared type of each column pair's key.
    pub types: Vec<DataType>,
}

/// What the Combine stage computes over the other stages' rows: their joins, then the query's
/// result columns, order, limit and skip.
#[derive(Debug, Clone, PartialEq)]
pub struct CombineQuery {
    pub joins: Vec<CombineJoin>,
    pub columns: Vec<(String, Position)>,
    pub order: Vec<(Position, Order)>,
    pub limit: u64,
    pub skip: Option<u64>,
    /// The result columns that hold text, which the response policy excerpts.
    pub excerpted: Vec<String>,
    /// The nodes the result carries, each with the columns redaction reads.
    pub nodes: Vec<CombineNode>,
    /// The edges the result carries between its nodes.
    pub edges: Vec<CombineEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombineNode {
    pub alias: String,
    pub entity: String,
    pub stage: StageId,
    /// The node's identity, one column per part, each read as its type.
    pub identity: Vec<(Position, DataType)>,
    /// The key a row check reads when it is not the identity.
    pub key: Option<Position>,
    /// Whether the stage put the rows through every check of the entity.
    pub checked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CombineEdge {
    /// A step that crosses stages, between its endpoint nodes.
    Crossing {
        step: usize,
        relationship: String,
        source: String,
        target: String,
    },
    /// A step within one stage, in the edge columns that stage returns; an endpoint id is one
    /// column, or the endpoint node's identity parts when its identity is composite.
    Stage {
        step: usize,
        rel_types: Vec<String>,
        kind: Position,
        source: Vec<Position>,
        source_kind: Position,
        target: Vec<Position>,
        target_kind: Position,
        path: Option<Position>,
    },
}

impl CombineEdge {
    pub fn step(&self) -> usize {
        match self {
            Self::Crossing { step, .. } | Self::Stage { step, .. } => *step,
        }
    }
}
