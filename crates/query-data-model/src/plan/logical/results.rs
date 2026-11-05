use std::collections::HashSet;

use super::limits::{self, MAX_COLUMNS, MAX_GROUPS, MAX_MEASURES, MAX_ROWS, MAX_SKIP};
use super::{
    Columns, Expr, LogicalBuilder, LogicalCatalog, LogicalOperation, LogicalType, Measure, Order,
    PlanError, Projection, Rows, check,
};
use crate::plan::tracking::{Construction, Declare, Named, Output, Outputs};

impl<'m> LogicalBuilder<'m> {
    pub fn aggregate(
        &mut self,
        rows: Rows<'m>,
        groups: impl IntoIterator<Item = Named<Expr>>,
        measures: impl IntoIterator<Item = Named<Measure>>,
    ) -> Result<Rows<'m>, PlanError> {
        let (groups, group_names): (Vec<_>, Vec<_>) = groups
            .into_iter()
            .map(|named| (named.item, named.name))
            .unzip();
        let (measures, measure_names): (Vec<_>, Vec<_>) = measures
            .into_iter()
            .map(|named| (named.item, named.name))
            .unzip();
        let operation = LogicalOperation::Aggregate {
            groups: groups.clone(),
            measures: measures.clone(),
        };
        self.connect(operation, vec![rows], |catalog, construction, _| {
            limits::at_most("group keys", groups.len(), MAX_GROUPS)?;
            limits::at_most("aggregates", measures.len(), MAX_MEASURES)?;
            let model = catalog.model();
            let mut types = Vec::new();
            for group in &groups {
                looked_up(catalog, construction, *group)?;
                types.push(check::group(model, construction, *group)?);
            }
            for measure in &measures {
                let mut input = *measure;
                if let Some(expr) = input.input() {
                    looked_up(catalog, construction, *expr)?;
                }
                types.push(LogicalType::Scalar(check::measure(
                    model,
                    construction,
                    *measure,
                )?));
            }
            declare(group_names.into_iter().chain(measure_names), types)
        })
    }

    pub fn select<P: Into<Projection>>(
        &mut self,
        rows: Rows<'m>,
        items: impl IntoIterator<Item = Named<P>>,
    ) -> Result<Rows<'m>, PlanError> {
        let (items, names): (Vec<Projection>, Vec<_>) = items
            .into_iter()
            .map(|named| (named.item.into(), named.name))
            .unzip();
        let operation = LogicalOperation::Select(items.clone());
        self.connect(operation, vec![rows], |catalog, construction, _| {
            let types = items
                .iter()
                .map(|item| projection(catalog, construction, item))
                .collect::<Result<Vec<_>, _>>()?;
            declare(names, types)
        })
    }

    pub fn order(
        &mut self,
        rows: Rows<'m>,
        keys: impl IntoIterator<Item = (Expr, Order)>,
    ) -> Result<Rows<'m>, PlanError> {
        let keys: Vec<_> = keys.into_iter().collect();
        let checked: Vec<Expr> = keys.iter().map(|(key, _)| *key).collect();
        self.connect(
            LogicalOperation::Order(keys),
            vec![rows],
            |catalog, construction, _| {
                for key in checked {
                    check::scalar(catalog.model(), construction, key)?;
                }
                Ok(Outputs::preserve())
            },
        )
    }

    pub fn limit(&mut self, rows: Rows<'m>, count: u64) -> Result<Rows<'m>, PlanError> {
        self.slice(rows, count, 0)
    }

    pub fn slice(&mut self, rows: Rows<'m>, count: u64, skip: u64) -> Result<Rows<'m>, PlanError> {
        let operation = LogicalOperation::Limit { rows: count, skip };
        self.connect(operation, vec![rows], |_, _, _| {
            limits::within("limit", count, 1, MAX_ROWS)?;
            limits::within("skip", skip, 0, MAX_SKIP)?;
            Ok(Outputs::preserve())
        })
    }

    pub fn union(&mut self, inputs: Vec<Rows<'m>>) -> Result<Rows<'m>, PlanError> {
        self.connect(
            LogicalOperation::Union,
            inputs,
            |_, construction, inputs| {
                let signature = |rows: &Rows<'m>| {
                    construction
                        .outputs(rows.inner())?
                        .into_iter()
                        .map(|output| {
                            let (name, value) = construction.value(output)?;
                            Ok(Declare {
                                name: name.map(str::to_string),
                                value: *value,
                            })
                        })
                        .collect::<Result<Vec<_>, PlanError>>()
                };
                let mut signatures = inputs.iter().map(signature);
                let first = signatures.next().transpose()?.unwrap_or_default();
                match signatures
                    .find(|other| other.as_ref() != Ok(&first))
                    .transpose()?
                {
                    Some(other) => Err(PlanError::Type {
                        expr: "union".to_string(),
                        expected: format!("{first:?}"),
                        found: format!("{other:?}"),
                    }),
                    None => Ok(Outputs::replace(first.into_iter().map(Output::Declare))),
                }
            },
        )
    }
}

fn projection(
    catalog: LogicalCatalog<'_>,
    construction: &Construction<super::Logical>,
    item: &Projection,
) -> Result<LogicalType, PlanError> {
    match item {
        Projection::Value(expr) => Ok(LogicalType::Scalar(
            check::scalar(catalog.model(), construction, *expr)?.data_type,
        )),
        Projection::Graph { binding, columns } => {
            let (name, kind) = construction.value(*binding)?;
            let name = name.unwrap_or("node");
            match (kind, columns) {
                (LogicalType::Scalar(_), _) => Err(check::mismatch(name, "a graph value", *kind)),
                (LogicalType::Entity(entity), Columns::Listed(listed)) => {
                    limits::at_most("columns", listed.len(), MAX_COLUMNS)?;
                    let graph = catalog.model().graph();
                    let foreign = listed
                        .iter()
                        .find(|property| graph.property(**property).entity != *entity);
                    match foreign {
                        Some(property) => Err(check::mismatch(
                            &graph.property(*property).name,
                            &graph.entity(*entity).name,
                            *kind,
                        )),
                        None => Ok(*kind),
                    }
                }
                (_, Columns::Listed(_)) => Err(check::mismatch(name, "an entity", *kind)),
                _ => Ok(*kind),
            }
        }
    }
}

fn looked_up(
    catalog: LogicalCatalog<'_>,
    construction: &Construction<super::Logical>,
    expr: Expr,
) -> Result<(), PlanError> {
    let model = catalog.model();
    if let Expr::Property { property, .. } | Expr::Truncate { property, .. } = expr
        && model.lookup(property).is_some()
    {
        let typed = check::scalar(model, construction, expr)?;
        return Err(PlanError::shape(format!(
            "{} is looked up from a service and cannot be aggregated or grouped",
            typed.name
        )));
    }
    Ok(())
}

fn declare(
    names: impl IntoIterator<Item = String>,
    types: Vec<LogicalType>,
) -> Result<Outputs<LogicalType>, PlanError> {
    let mut seen = HashSet::new();
    let outputs = names
        .into_iter()
        .zip(types)
        .map(|(name, value)| {
            if !seen.insert(name.clone()) {
                return Err(PlanError::shape(format!("duplicate output column {name}")));
            }
            Ok(Output::Declare(Declare::named(name, value)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Outputs::replace(outputs))
}
