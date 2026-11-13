use query_data_model::plan::logical::Order;
use serde::Serialize;

use super::StageId;

/// A column of an earlier stage's rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct EngineColumn {
    pub stage: StageId,
    pub column: String,
}

/// Rows of two stages that match on every column pair; `left` belongs to the earlier stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EngineJoin {
    pub left: Vec<EngineColumn>,
    pub right: Vec<EngineColumn>,
    /// The declared type of each column pair's key.
    pub types: Vec<query_data_model::DataType>,
}

/// What the engine stage computes over the other stages' rows: their joins, then the query's
/// result columns, order, limit and skip.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EngineQuery {
    pub joins: Vec<EngineJoin>,
    pub columns: Vec<(String, EngineColumn)>,
    #[serde(serialize_with = "orders")]
    pub order: Vec<(EngineColumn, Order)>,
    pub limit: u64,
    pub skip: Option<u64>,
    /// The result columns that hold text, which the response policy excerpts.
    pub excerpted: Vec<String>,
    /// The nodes of remote stages whose identity the stage returns, which the result carries
    /// like the nodes of relational stages.
    pub nodes: Vec<EngineNode>,
    /// The relationships the joins follow, which the result carries as edges between their
    /// endpoint nodes.
    pub edges: Vec<EngineEdge>,
    /// The pattern step each stage's own relationships stand for, by stage and in the order the
    /// stage's statement follows them.
    pub steps: Vec<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EngineEdge {
    /// The step's index among the pattern's steps.
    pub step: usize,
    pub relationship: String,
    /// The aliases of the relationship variant's source and target.
    pub source: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EngineNode {
    pub alias: String,
    pub entity: String,
    pub id: EngineColumn,
}

fn orders<S: serde::Serializer>(
    order: &[(EngineColumn, Order)],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(order.len()))?;
    for (column, order) in order {
        let direction = match order {
            Order::Ascending => "asc",
            Order::Descending => "desc",
        };
        seq.serialize_element(&(column, direction))?;
    }
    seq.end()
}
