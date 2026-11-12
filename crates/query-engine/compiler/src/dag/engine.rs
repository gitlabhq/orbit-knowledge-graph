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
