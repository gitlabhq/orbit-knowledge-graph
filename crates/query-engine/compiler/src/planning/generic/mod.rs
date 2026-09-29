mod expression;
mod plan;

pub use expression::{Expr, Function, ValueId, ValueType, Values};
pub use plan::{Assignment, JoinKind, Node, Op, Operation, Schema, SortKey};

use crate::error::{QueryError, Result};

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(QueryError::PipelineInvariant(message.into()))
    }
}
