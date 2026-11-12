use orbit_server_config::QueryLimits;

use super::{Expr, PlanError, Predicate};
use crate::Model;

pub const RESERVED_PREFIX: &str = "_orbit_";
pub(super) fn at_most(what: &str, found: usize, max: usize) -> Result<(), PlanError> {
    if found > max {
        return Err(PlanError::limit(what, found, max));
    }
    Ok(())
}

pub(super) fn in_values(
    model: &Model,
    predicate: &Predicate,
    limits: &QueryLimits,
) -> Result<(), PlanError> {
    match predicate {
        Predicate::In {
            left: Expr::Property { property, .. },
            values,
        } if model
            .graph()
            .entity(model.graph().property(*property).entity)
            .identity
            == [*property] =>
        {
            at_most("pinned ids", values.len(), limits.pinned_ids)
        }
        Predicate::And(all) => all
            .iter()
            .try_for_each(|inner| in_values(model, inner, limits)),
        nested => in_lists(nested, limits.in_values),
    }
}

fn in_lists(predicate: &Predicate, max: usize) -> Result<(), PlanError> {
    match predicate {
        Predicate::In { values, .. } => at_most("an IN list", values.len(), max),
        Predicate::InTuple { values, .. } => at_most("a tuple IN list", values.len(), max),
        Predicate::And(all) => all.iter().try_for_each(|inner| in_lists(inner, max)),
        Predicate::Not(inner) => in_lists(inner, max),
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
