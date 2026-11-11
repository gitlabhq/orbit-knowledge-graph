use query_data_model::Catalog;
use query_data_model::plan::logical::{Columns, Expr, Measure, Projection};

use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::codegen::SqlBackend;

/// Every property and attribute the plan reads, wherever it is read: none hidden, and none
/// administrator-only unless the caller is an administrator.
pub(super) fn fields(
    query: Query<'_>,
    model: &Catalog<impl SqlBackend>,
    admin: bool,
) -> Result<()> {
    let access = model.model().access();
    let check = |expr: Expr, what: &str| -> Result<()> {
        let (owner, name, declared) = match expr {
            Expr::Property { binding, property }
            | Expr::Truncate {
                binding, property, ..
            } => (
                query.entity_name(binding).unwrap_or_default().to_string(),
                query.property_name(property).to_string(),
                access.property(property),
            ),
            Expr::Attribute { attribute, .. } => {
                let declared = query.model.graph().attribute(attribute);
                let relationship = &query.model.graph().relationship(declared.relationship).name;
                (
                    relationship.clone(),
                    declared.name.clone(),
                    access.attribute(attribute),
                )
            }
            Expr::Edge { .. } | Expr::Node(_) | Expr::Output(_) => return Ok(()),
        };
        if declared.hidden && !model.trusted() {
            return Err(QueryError::Restrict(format!(
                "{what} \"{name}\" for {owner}: field is hidden"
            )));
        }
        if admin || model.trusted() || !declared.admin_only {
            return Ok(());
        }
        Err(QueryError::Restrict(format!(
            "{what} \"{name}\" for {owner}: field requires administrator access"
        )))
    };
    for leaf in query.leaves() {
        for expr in leaf.predicate.exprs() {
            check(expr, "filter on")?;
        }
    }
    for item in query.selected() {
        match item {
            Projection::Graph {
                binding,
                columns: Columns::Listed(listed),
            } => {
                for property in listed {
                    let binding = *binding;
                    check(
                        Expr::Property {
                            binding,
                            property: *property,
                        },
                        "column",
                    )?;
                }
            }
            Projection::Value(expr) => check(*expr, "column")?,
            Projection::Graph { .. } => {}
        }
    }
    for key in query.ordered() {
        check(key, "order_by on")?;
    }
    if let Some((groups, measures)) = query.aggregate() {
        for measure in measures {
            let input = match measure {
                Measure::Count(input) => *input,
                Measure::CountDistinct(expr)
                | Measure::Sum(expr)
                | Measure::Avg(expr)
                | Measure::Min(expr)
                | Measure::Max(expr) => Some(*expr),
            };
            if let Some(expr) = input {
                check(expr, "aggregation on")?;
            }
        }
        for group in groups {
            check(*group, "group_by on")?;
        }
    }
    Ok(())
}
