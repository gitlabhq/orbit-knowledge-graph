mod expression;
pub mod facts;
mod plan;

pub use expression::{Expr, Function, ValueId, ValueType, Values};
pub use plan::{
    AggregateFunction, Assignment, JoinKind, Measure, Node, Op, Operation, Program, Schema,
    SortKey, SubplanId,
};

use crate::error::{QueryError, Result};

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(QueryError::PipelineInvariant(message.into()))
    }
}
