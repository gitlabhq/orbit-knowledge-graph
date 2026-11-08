use query_data_model::plan::logical::{Columns, Expr, Measure, Projection};

use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::codegen::RelationalModel;

pub(super) fn admin_only(query: Query<'_>, model: &impl RelationalModel) -> Result<()> {
    let check = |expr: Expr, what: &str| -> Result<()> {
        let (Expr::Property { binding, property }
        | Expr::Truncate {
            binding, property, ..
        }) = expr
        else {
            return Ok(());
        };
        if !model.property_is_admin_only(property) {
            return Ok(());
        }
        let entity = query.entity_name(binding).unwrap_or_default();
        let name = query.property_name(property);
        Err(QueryError::Restrict(format!(
            "{what} \"{name}\" for {entity}: field requires administrator access"
        )))
    };
    for conjunct in query.conjuncts() {
        for expr in conjunct.exprs() {
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
