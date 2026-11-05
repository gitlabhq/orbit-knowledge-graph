use super::{PlanError, Predicate};

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

pub(super) fn at_most(what: &str, found: usize, max: usize) -> Result<(), PlanError> {
    if found > max {
        return Err(PlanError::limit(what, found, max));
    }
    Ok(())
}

pub(super) fn in_values(predicate: &Predicate) -> Result<(), PlanError> {
    match predicate {
        Predicate::In { values, .. } => at_most("an IN list", values.len(), MAX_IN_VALUES),
        Predicate::And(all) => all.iter().try_for_each(in_values),
        Predicate::Not(inner) => in_values(inner),
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
