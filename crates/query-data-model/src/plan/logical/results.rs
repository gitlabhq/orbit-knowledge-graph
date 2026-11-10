use std::collections::HashSet;

use super::limits::{self, DEFAULT_ROWS, MAX_GROUPS, MAX_MEASURES, MAX_ROWS, MAX_SKIP};
use super::{
    Columns, Expr, LogicalBuilder, LogicalCatalog, LogicalOperation, LogicalType, Measure, Order,
    PlanError, Projection, Rows, check,
};
use crate::plan::tracking::{Construction, Declare, Named, Output, Outputs, Ref};

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
        let shape = Shape::of(&rows);
        self.connect(operation, vec![rows], |catalog, construction, _| {
            match shape {
                Shape::Match => {}
                Shape::Neighbors => {
                    return Err(PlanError::shape(
                        "neighbors cannot be aggregated; use labeled node patterns",
                    ));
                }
                Shape::Path => {
                    return Err(PlanError::shape(
                        "aggregation over shortest paths is not supported",
                    ));
                }
            }
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
        let shape = Shape::of(&rows);
        let clash = items.iter().zip(&names).find(|(item, name)| {
            rows.binding(name).is_ok_and(|bound| {
                !matches!(item, Projection::Graph { binding, .. } if *binding == bound.output())
            })
        });
        let clash = match clash {
            Some((_, name)) => Err(PlanError::shape(format!(
                "{name} names a variable of the pattern; return this column under another alias"
            ))),
            None => Ok(()),
        };
        self.attempt(|_, _| clash)?;
        self.connect(operation, vec![rows], |catalog, construction, _| {
            let mut projected: Vec<Ref> = Vec::new();
            for (item, name) in items.iter().zip(&names) {
                allowed(shape, Item::of(construction, item)?)?;
                if let Projection::Graph { binding, .. } = item {
                    once(construction, *binding, name, &mut projected)?;
                }
            }
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
        let shape = Shape::of(&rows);
        self.connect(
            LogicalOperation::Order(keys),
            vec![rows],
            |catalog, construction, _| {
                if shape != Shape::Match {
                    return Err(PlanError::shape(
                        "path finding and neighbors have fixed result ordering",
                    ));
                }
                for key in checked {
                    if matches!(key, Expr::Attribute { .. })
                        || matches!(key, Expr::Edge { field, .. } if field != super::EdgeField::Kind)
                    {
                        return Err(PlanError::shape(
                            "ORDER BY cannot sort by a relationship property; sort by a node property or by type(r)",
                        ));
                    }
                    check::scalar(catalog.model(), construction, key)?;
                }
                Ok(Outputs::preserve())
            },
        )
    }

    pub fn limit(&mut self, rows: Rows<'m>, count: u64) -> Result<Rows<'m>, PlanError> {
        self.slice(rows, Some(count), None)
    }

    /// Keeps `count` rows after skipping `skip`; a query that names no count returns [`DEFAULT_ROWS`].
    pub fn slice(
        &mut self,
        rows: Rows<'m>,
        count: Option<u64>,
        skip: Option<u64>,
    ) -> Result<Rows<'m>, PlanError> {
        let count = count.unwrap_or(DEFAULT_ROWS);
        let operation = LogicalOperation::Limit { rows: count, skip };
        self.connect(operation, vec![rows], |_, _, _| {
            limits::within("limit", count, 1, MAX_ROWS)?;
            if skip.unwrap_or_default() > MAX_SKIP {
                return Err(PlanError::shape(format!(
                    "skip must be at most {MAX_SKIP}; to page deeper, ORDER BY a unique key such as id and filter past the last value from the previous page"
                )));
            }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Match,
    Neighbors,
    Path,
}

impl Shape {
    fn of(rows: &Rows<'_>) -> Self {
        let kinds: Vec<LogicalType> = rows.bound().map(super::Binding::kind).collect();
        match () {
            () if kinds.contains(&LogicalType::Path) => Self::Path,
            () if kinds.contains(&LogicalType::Node) => Self::Neighbors,
            () => Self::Match,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Item {
    Node,
    Relationship,
    Type,
    Match,
    Dynamic,
    Truncate,
}

impl Item {
    fn of(
        construction: &Construction<super::Logical>,
        item: &Projection,
    ) -> Result<Self, PlanError> {
        Ok(match item {
            Projection::Graph { binding, .. } => match construction.value(*binding)?.1 {
                LogicalType::Edge => Self::Relationship,
                LogicalType::Node | LogicalType::Path => Self::Dynamic,
                _ => Self::Node,
            },
            Projection::Value(Expr::Edge { .. } | Expr::Attribute { .. }) => Self::Type,
            Projection::Value(Expr::Truncate { .. }) => Self::Truncate,
            Projection::Value(Expr::Output(_)) => Self::Match,
            Projection::Value(_) => Self::Node,
        })
    }
}

const FIXED: &str = "neighbors and path finding return each relationship in the path column; remove the relationship from RETURN";

/// What each query shape may return: `None` allows the item, a message rejects it.
const ALLOWED: [[Option<&str>; 6]; 3] = [
    [None, None, None, None, None, Some(TRUNCATE)],
    [
        None,
        Some(FIXED),
        Some(FIXED),
        Some(MATCHED),
        None,
        Some(TRUNCATE),
    ],
    [
        Some(PATH),
        Some(FIXED),
        Some(FIXED),
        Some(MATCHED),
        None,
        Some(TRUNCATE),
    ],
];
const TRUNCATE: &str = "date_trunc is only supported as an aggregation group key";
const MATCHED: &str = "match columns need a single-node match";
const PATH: &str = "path finding returns its path variable only";

fn allowed(shape: Shape, item: Item) -> Result<(), PlanError> {
    match ALLOWED[shape as usize][item as usize] {
        Some(message) => Err(PlanError::shape(message)),
        None => Ok(()),
    }
}

fn once(
    construction: &Construction<super::Logical>,
    binding: Ref,
    name: &str,
    projected: &mut Vec<Ref>,
) -> Result<(), PlanError> {
    let (bound, kind) = construction.value(binding)?;
    let bound = bound.unwrap_or(name);
    if matches!(kind, LogicalType::Node | LogicalType::Path) && bound != name {
        return Err(PlanError::shape(format!(
            "{bound} is a dynamic graph result and cannot be renamed"
        )));
    }
    if projected.contains(&binding) {
        return Err(PlanError::shape(format!(
            "{bound} is returned twice; return each node once"
        )));
    }
    projected.push(binding);
    Ok(())
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
                (LogicalType::Edge, Columns::All | Columns::Listed(_)) => {
                    Err(PlanError::shape(format!(
                        "{name} is a relationship and has no properties; return {name} or type({name})"
                    )))
                }
                (LogicalType::Entity(entity), Columns::Listed(listed)) => {
                    limits::at_most("columns", listed.len(), catalog.limits().columns)?;
                    let graph = catalog.model().graph();
                    if let Some((index, _)) = listed
                        .iter()
                        .enumerate()
                        .find(|(index, property)| listed[..*index].contains(property))
                    {
                        return Err(PlanError::shape(format!(
                            "{name}.{} is returned twice",
                            graph.property(listed[index]).name
                        )));
                    }
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
