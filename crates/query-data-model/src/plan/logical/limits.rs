use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::{Expr, PlanError, Predicate};
use crate::Model;

pub const RESERVED_PREFIX: &str = "_orbit_";
pub const MAX_NODES: usize = 5;
pub const MAX_HOPS: u32 = 3;
pub const MAX_PATH_DEPTH: u32 = 3;
pub const MAX_RELATIONSHIP_TYPES: usize = 10;
pub const MAX_ROWS: u64 = 1000;
pub const MAX_SKIP: u64 = 10_000;
pub const MAX_IN_VALUES: usize = 100;
pub const MAX_PINS: usize = 500;
pub const MAX_COLUMNS: usize = 50;
pub const MAX_MEASURES: usize = 10;
pub const MAX_GROUPS: usize = 4;
pub const MAX_TEXT: usize = 1024;
pub const MAX_IDENTIFIER: usize = 64;
pub const MAX_NESTING: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub pins: usize,
    pub columns: usize,
}

impl Limits {
    pub const QUERY: Self = Self {
        pins: MAX_PINS,
        columns: MAX_COLUMNS,
    };
}

pub(super) fn at_most(what: &str, found: usize, max: usize) -> Result<(), PlanError> {
    if found > max {
        return Err(PlanError::limit(what, found, max));
    }
    Ok(())
}

pub(super) fn in_values(
    model: &Model,
    predicate: &Predicate,
    limits: Limits,
) -> Result<(), PlanError> {
    match predicate {
        Predicate::In {
            left: Expr::Property { property, .. },
            values,
        } if model.graph().property(*property).name == DEFAULT_PRIMARY_KEY => {
            at_most("pinned ids", values.len(), limits.pins)
        }
        Predicate::And(all) => all
            .iter()
            .try_for_each(|inner| in_values(model, inner, limits)),
        nested => in_lists(nested),
    }
}

fn in_lists(predicate: &Predicate) -> Result<(), PlanError> {
    match predicate {
        Predicate::In { values, .. } => at_most("an IN list", values.len(), MAX_IN_VALUES),
        Predicate::And(all) => all.iter().try_for_each(in_lists),
        Predicate::Not(inner) => in_lists(inner),
        _ => Ok(()),
    }
}

pub(super) fn within(what: &str, found: u64, min: u64, max: u64) -> Result<(), PlanError> {
    if found < min || found > max {
        return Err(PlanError::shape(format!(
            "{what} must be between {min} and {max}, got {found}"
        )));
    }
    Ok(())
}
