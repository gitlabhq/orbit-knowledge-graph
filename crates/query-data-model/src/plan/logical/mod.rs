mod builder;
mod catalog;
mod check;
mod conditions;
mod detach;
mod error;
mod expr;
mod filter;
mod handle;
pub mod limits;
mod measure;
mod pattern;
mod predicate;
mod results;
mod shape;
mod step;
mod time;

use crate::DataType;

pub use builder::{InternalName, LogicalBuilder};
pub use catalog::LogicalCatalog;
pub use detach::Detached;
pub use error::{ErrorClass, PlanError};
pub use expr::{EdgeField, Expr, Literal, Operand, TimeUnit};
pub use handle::{Binding, Rows};
pub use measure::{Columns, Measure, Order, Projection, count};
pub use predicate::{Comparison, Conjunct, Matching, Predicate};
pub use step::{Hops, Step};

use crate::plan::tracking::{Body, Language, Plan, Ref};
use crate::{Direction, EntityId, Hop, RelationshipId};

#[derive(Debug)]
pub struct Logical;

pub type LogicalPlan = Plan<Logical>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    Entity(EntityId),
    Node,
    Edge,
    Path,
    Scalar(DataType),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperation {
    Entities(EntityId),
    Filter(Predicate),
    Follow {
        from: Ref,
        relationships: Vec<RelationshipId>,
        direction: Direction,
        range: Hops,
        hops: Vec<Hop>,
    },
    Neighbors {
        from: Ref,
        relationships: Vec<RelationshipId>,
        direction: Direction,
    },
    Shortest {
        from: Ref,
        to: Ref,
        relationships: Vec<RelationshipId>,
        max_depth: u32,
    },
    Aggregate {
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    },
    Select(Vec<Projection>),
    Order(Vec<(Expr, Order)>),
    Limit {
        rows: u64,
        skip: Option<u64>,
    },
    Union,
    Read(Body),
}

impl Language for Logical {
    type Operation = LogicalOperation;
    type Value = LogicalType;

    fn references(operation: &mut LogicalOperation, visit: &mut dyn FnMut(&mut Ref)) {
        match operation {
            LogicalOperation::Filter(predicate) => predicate.references(visit),
            LogicalOperation::Follow { from, .. } | LogicalOperation::Neighbors { from, .. } => {
                visit(from)
            }
            LogicalOperation::Shortest { from, to, .. } => {
                visit(from);
                visit(to);
            }
            LogicalOperation::Aggregate { groups, measures } => {
                let inputs = measures.iter_mut().filter_map(Measure::input);
                groups
                    .iter_mut()
                    .chain(inputs)
                    .for_each(|expr| expr.references(visit));
            }
            LogicalOperation::Select(items) => {
                items.iter_mut().for_each(|item| item.references(visit))
            }
            LogicalOperation::Order(keys) => {
                keys.iter_mut().for_each(|(key, _)| key.references(visit))
            }
            LogicalOperation::Entities(_)
            | LogicalOperation::Limit { .. }
            | LogicalOperation::Union
            | LogicalOperation::Read(_) => {}
        }
    }
}
