use super::*;
use crate::ast::{JoinType, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::passes::shared::data_type_to_ch;
use std::collections::HashMap;

pub fn lower_clickhouse(
    bound: &BoundCatalog,
    selected: SelectedPlan<ClickHouse>,
) -> Result<LoweredPlan> {
    let aliases = aliases_clickhouse(bound, &selected.candidate.plan);
    let physical_columns = physical_columns(&selected.candidate.plan);
    let query = lower_ch(
        bound,
        selected.candidate.plan.clone(),
        &aliases,
        &physical_columns,
    )?;
    lowered(bound, selected.candidate, aliases, query)
}

pub fn lower_duckdb(bound: &BoundCatalog, selected: SelectedPlan<DuckDb>) -> Result<LoweredPlan> {
    let aliases = aliases_duckdb(bound, &selected.candidate.plan);
    let query = lower_duck(bound, selected.candidate.plan.clone(), &aliases)?;
    lowered(bound, selected.candidate, aliases, query)
}

fn aliases_clickhouse(
    bound: &BoundCatalog,
    plan: &Plan<ClickHouse>,
) -> HashMap<RelationId, String> {
    let mut aliases = HashMap::new();
    visit_ch(plan, &mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            let alias = relation_alias(bound, scan.relation);
            aliases.insert(scan.relation, alias.clone());
            if let ClickHouseAccess::DenormalizedJoin(access) = &scan.access {
                for relation in &access.relations {
                    aliases.insert(*relation, alias.clone());
                }
            }
        }
        Operator::Bind(relation) => {
            aliases.insert(*relation, relation_alias(bound, *relation));
        }
        _ => {}
    });
    aliases
}

fn aliases_duckdb(bound: &BoundCatalog, plan: &Plan<DuckDb>) -> HashMap<RelationId, String> {
    let mut aliases = HashMap::new();
    visit_duck(plan, &mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            aliases.insert(scan.relation, relation_alias(bound, scan.relation));
        }
        Operator::Bind(relation) => {
            aliases.insert(*relation, relation_alias(bound, *relation));
        }
        _ => {}
    });
    aliases
}

fn visit_ch(plan: &Plan<ClickHouse>, visitor: &mut impl FnMut(&Plan<ClickHouse>)) {
    visitor(plan);
    plan.inputs
        .iter()
        .for_each(|input| visit_ch(input, visitor));
}

fn visit_duck(plan: &Plan<DuckDb>, visitor: &mut impl FnMut(&Plan<DuckDb>)) {
    visitor(plan);
    plan.inputs
        .iter()
        .for_each(|input| visit_duck(input, visitor));
}

fn lower_ch(
    bound: &BoundCatalog,
    plan: Plan<ClickHouse>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    match plan.operator {
        Operator::Scan(scan) => Ok(Query {
            from: match scan.access {
                ClickHouseAccess::Table(access) => {
                    TableRef::scan(access.layout.table.0, &aliases[&scan.relation])
                }
                ClickHouseAccess::EdgeTables(access) => {
                    edge_tables(&access.layouts, &aliases[&scan.relation])
                }
                ClickHouseAccess::DenormalizedJoin(access) => {
                    TableRef::scan(access.layout.table.0, &aliases[&scan.relation])
                }
            },
            ..Default::default()
        }),
        Operator::Filter(expression) => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            and_where(
                &mut query,
                lower_expr_ch(bound, &expression, aliases, physical_columns)?,
            );
            Ok(query)
        }
        Operator::Project(columns) => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            query.select = projection_ch(bound, columns, aliases, physical_columns)?;
            Ok(query)
        }
        Operator::Join(conditions) => {
            lower_join_ch(bound, plan.inputs, conditions, aliases, physical_columns)
        }
        Operator::SemiJoin(condition) => {
            lower_semi_ch(bound, plan.inputs, condition, aliases, physical_columns)
        }
        Operator::Aggregate { groups, metrics } => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            lower_aggregate_ch(
                bound,
                &mut query,
                groups,
                metrics,
                aliases,
                physical_columns,
            )?;
            Ok(query)
        }
        Operator::Union => {
            let queries = plan
                .inputs
                .into_iter()
                .map(|input| lower_ch(bound, input, aliases, physical_columns))
                .collect::<Result<Vec<_>>>()?;
            Ok(Query {
                select: union_projection(&queries),
                from: TableRef::union_all(queries, "union"),
                ..Default::default()
            })
        }
        Operator::Bind(relation) => Ok(Query {
            from: TableRef::subquery(
                select_star(only_ch(bound, plan.inputs, aliases, physical_columns)?),
                relation_alias(bound, relation),
            ),
            ..Default::default()
        }),
        Operator::Sort(keys) => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            query.order_by = order_by_ch(bound, keys, aliases, physical_columns)?;
            Ok(query)
        }
        Operator::Limit(limit) => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            query.limit = Some(limit);
            Ok(query)
        }
        Operator::CurrentRows { keys, strategy } => {
            let mut query = only_ch(bound, plan.inputs, aliases, physical_columns)?;
            match strategy {
                ClickHouseCurrentRows::Final => set_final(&mut query.from),
                ClickHouseCurrentRows::LimitBy => {
                    let keys = keys
                        .iter()
                        .map(|key| lower_expr_ch(bound, key, aliases, physical_columns))
                        .collect::<Result<Vec<_>>>()?;
                    query.limit_by = Some((1, keys));
                }
            }
            Ok(query)
        }
        Operator::Extension(_) => Err(QueryError::Lowering(
            "ClickHouse extension is not implemented".into(),
        )),
    }
}

fn lower_duck(
    bound: &BoundCatalog,
    plan: Plan<DuckDb>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    match plan.operator {
        Operator::Scan(scan) => match scan.access {
            DuckDbAccess::Table(access) => Ok(Query {
                from: TableRef::scan(access.layout.table.0, &aliases[&scan.relation]),
                ..Default::default()
            }),
        },
        Operator::Filter(expression) => {
            let mut query = only_duck(bound, plan.inputs, aliases)?;
            and_where(&mut query, lower_expr(bound, &expression, aliases)?);
            Ok(query)
        }
        Operator::Project(columns) => {
            let mut query = only_duck(bound, plan.inputs, aliases)?;
            query.select = projection(bound, columns, aliases)?;
            Ok(query)
        }
        Operator::Join(conditions) => lower_join_duck(bound, plan.inputs, conditions, aliases),
        Operator::SemiJoin(condition) => lower_semi_duck(bound, plan.inputs, condition, aliases),
        Operator::Aggregate { groups, metrics } => {
            let mut query = only_duck(bound, plan.inputs, aliases)?;
            lower_aggregate(bound, &mut query, groups, metrics, aliases)?;
            Ok(query)
        }
        Operator::Union => {
            let queries = plan
                .inputs
                .into_iter()
                .map(|input| lower_duck(bound, input, aliases))
                .collect::<Result<Vec<_>>>()?;
            Ok(Query {
                select: union_projection(&queries),
                from: TableRef::union_all(queries, "union"),
                ..Default::default()
            })
        }
        Operator::Bind(relation) => Ok(Query {
            from: TableRef::subquery(
                select_star(only_duck(bound, plan.inputs, aliases)?),
                relation_alias(bound, relation),
            ),
            ..Default::default()
        }),
        Operator::Sort(keys) => {
            let mut query = only_duck(bound, plan.inputs, aliases)?;
            query.order_by = order_by(bound, keys, aliases)?;
            Ok(query)
        }
        Operator::Limit(limit) => {
            let mut query = only_duck(bound, plan.inputs, aliases)?;
            query.limit = Some(limit);
            Ok(query)
        }
        Operator::CurrentRows { .. } => only_duck(bound, plan.inputs, aliases),
        _ => Err(QueryError::Lowering("operator is not populated".into())),
    }
}

fn lower_join_ch(
    bound: &BoundCatalog,
    inputs: Vec<Plan<ClickHouse>>,
    conditions: Vec<Expr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let mut inputs = inputs.into_iter();
    let first = inputs
        .next()
        .ok_or_else(|| QueryError::Lowering("join needs an input".into()))?;
    let mut query = lower_ch(bound, first, aliases, physical_columns)?;
    for input in inputs {
        let right_relations = relations_ch(&input);
        let condition = join_condition_ch(
            bound,
            &conditions,
            &right_relations,
            aliases,
            physical_columns,
        )?;
        query.from = TableRef::join(
            JoinType::Inner,
            query.from,
            TableRef::subquery(
                select_star(lower_ch(bound, input, aliases, physical_columns)?),
                "right",
            ),
            condition,
        );
    }
    Ok(query)
}

fn lower_join_duck(
    bound: &BoundCatalog,
    inputs: Vec<Plan<DuckDb>>,
    conditions: Vec<Expr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let mut inputs = inputs.into_iter();
    let first = inputs
        .next()
        .ok_or_else(|| QueryError::Lowering("join needs an input".into()))?;
    let mut query = lower_duck(bound, first, aliases)?;
    for input in inputs {
        let right_relations = relations_duck(&input);
        let condition = join_condition(bound, &conditions, &right_relations, aliases)?;
        query.from = TableRef::join(
            JoinType::Inner,
            query.from,
            TableRef::subquery(select_star(lower_duck(bound, input, aliases)?), "right"),
            condition,
        );
    }
    Ok(query)
}

fn lower_semi_ch(
    bound: &BoundCatalog,
    inputs: Vec<Plan<ClickHouse>>,
    condition: Expr,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let [consumer, producer]: [Plan<ClickHouse>; 2] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("semi-join needs two inputs".into()))?;
    let mut consumer_query = lower_ch(bound, consumer, aliases, physical_columns)?;
    let mut producer_query = lower_ch(bound, producer, aliases, physical_columns)?;
    let Expr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = condition
    else {
        return Err(QueryError::Lowering("semi-join needs equality".into()));
    };
    producer_query.select = vec![SelectExpr {
        expr: lower_expr_ch(bound, &right, aliases, physical_columns)?,
        alias: None,
    }];
    and_where(
        &mut consumer_query,
        ast::Expr::InSelect {
            expr: Box::new(lower_expr_ch(bound, &left, aliases, physical_columns)?),
            query: Box::new(producer_query),
        },
    );
    Ok(consumer_query)
}

fn lower_semi_duck(
    bound: &BoundCatalog,
    inputs: Vec<Plan<DuckDb>>,
    condition: Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let [consumer, producer]: [Plan<DuckDb>; 2] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("semi-join needs two inputs".into()))?;
    lower_semi(
        bound,
        lower_duck(bound, consumer, aliases)?,
        lower_duck(bound, producer, aliases)?,
        condition,
        aliases,
    )
}

fn lower_semi(
    bound: &BoundCatalog,
    mut consumer_query: Query,
    mut producer_query: Query,
    condition: Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let Expr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = condition
    else {
        return Err(QueryError::Lowering("semi-join needs equality".into()));
    };
    producer_query.select = vec![SelectExpr {
        expr: lower_expr(bound, &right, aliases)?,
        alias: None,
    }];
    and_where(
        &mut consumer_query,
        ast::Expr::InSelect {
            expr: Box::new(lower_expr(bound, &left, aliases)?),
            query: Box::new(producer_query),
        },
    );
    Ok(consumer_query)
}

fn join_condition(
    bound: &BoundCatalog,
    conditions: &[Expr],
    right_relations: &BTreeSet<RelationId>,
    aliases: &HashMap<RelationId, String>,
) -> Result<ast::Expr> {
    Ok(conditions
        .iter()
        .filter(|condition| !expression_relations(bound, condition).is_disjoint(right_relations))
        .map(|condition| lower_expr(bound, condition, aliases))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .reduce(ast::Expr::and)
        .unwrap_or_else(|| ast::Expr::lit(1)))
}

fn join_condition_ch(
    bound: &BoundCatalog,
    conditions: &[Expr],
    right_relations: &BTreeSet<RelationId>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<ast::Expr> {
    Ok(conditions
        .iter()
        .filter(|condition| !expression_relations(bound, condition).is_disjoint(right_relations))
        .map(|condition| lower_expr_ch(bound, condition, aliases, physical_columns))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .reduce(ast::Expr::and)
        .unwrap_or_else(|| ast::Expr::lit(1)))
}

fn relations_ch(plan: &Plan<ClickHouse>) -> BTreeSet<RelationId> {
    let mut relations = BTreeSet::new();
    visit_ch(plan, &mut |plan| {
        if let Operator::Scan(scan) = &plan.operator {
            relations.insert(scan.relation);
        }
    });
    relations
}

fn relations_duck(plan: &Plan<DuckDb>) -> BTreeSet<RelationId> {
    let mut relations = BTreeSet::new();
    visit_duck(plan, &mut |plan| {
        if let Operator::Scan(scan) = &plan.operator {
            relations.insert(scan.relation);
        }
    });
    relations
}

fn expression_relations(bound: &BoundCatalog, expression: &Expr) -> BTreeSet<RelationId> {
    let mut columns = BTreeSet::new();
    collect_columns(expression, &mut columns);
    columns
        .into_iter()
        .filter_map(|column| bound.columns.get(&column).map(|column| column.relation))
        .collect()
}

fn collect_columns(expression: &Expr, columns: &mut BTreeSet<ColumnId>) {
    match expression {
        Expr::Column(column) => {
            columns.insert(*column);
        }
        Expr::Compare { left, right, .. } => {
            collect_columns(left, columns);
            collect_columns(right, columns);
        }
        Expr::Filter { left, right, .. } => {
            collect_columns(left, columns);
            right
                .iter()
                .for_each(|right| collect_columns(right, columns));
        }
        Expr::And(values) | Expr::Or(values) | Expr::Array(values) | Expr::Tuple(values) => {
            values
                .iter()
                .for_each(|value| collect_columns(value, columns));
        }
        Expr::In { value, .. }
        | Expr::DateTrunc { value, .. }
        | Expr::Stringify(value)
        | Expr::ListContains { list: value, .. }
        | Expr::TokenMatch { value, .. } => collect_columns(value, columns),
        Expr::Aggregate { value, .. } => value
            .iter()
            .for_each(|value| collect_columns(value, columns)),
        Expr::JsonObject(entries) => entries
            .iter()
            .for_each(|(_, value)| collect_columns(value, columns)),
        Expr::Output(_) | Expr::Literal(_) => {}
    }
}

fn only_ch(
    bound: &BoundCatalog,
    inputs: Vec<Plan<ClickHouse>>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let [input]: [Plan<ClickHouse>; 1] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("unary operator needs one input".into()))?;
    lower_ch(bound, input, aliases, physical_columns)
}

fn only_duck(
    bound: &BoundCatalog,
    inputs: Vec<Plan<DuckDb>>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let [input]: [Plan<DuckDb>; 1] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("unary operator needs one input".into()))?;
    lower_duck(bound, input, aliases)
}

fn projection(
    bound: &BoundCatalog,
    columns: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Vec<SelectExpr>> {
    columns
        .into_iter()
        .map(|column| {
            Ok(SelectExpr::new(
                lower_expr(bound, &column.expression, aliases)?,
                &bound.outputs[&column.output].name,
            ))
        })
        .collect()
}

fn projection_ch(
    bound: &BoundCatalog,
    columns: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Vec<SelectExpr>> {
    columns
        .into_iter()
        .map(|column| {
            Ok(SelectExpr::new(
                lower_expr_ch(bound, &column.expression, aliases, physical_columns)?,
                &bound.outputs[&column.output].name,
            ))
        })
        .collect()
}

fn lower_aggregate(
    bound: &BoundCatalog,
    query: &mut Query,
    groups: Vec<NamedExpr>,
    metrics: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<()> {
    for group in groups {
        let expression = lower_expr(bound, &group.expression, aliases)?;
        query.select.push(SelectExpr::new(
            expression.clone(),
            &bound.outputs[&group.output].name,
        ));
        query.group_by.push(expression);
    }
    for metric in metrics {
        query.select.push(SelectExpr::new(
            lower_expr(bound, &metric.expression, aliases)?,
            &bound.outputs[&metric.output].name,
        ));
    }
    Ok(())
}

fn lower_aggregate_ch(
    bound: &BoundCatalog,
    query: &mut Query,
    groups: Vec<NamedExpr>,
    metrics: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<()> {
    for group in groups {
        let expression = lower_expr_ch(bound, &group.expression, aliases, physical_columns)?;
        query.select.push(SelectExpr::new(
            expression.clone(),
            &bound.outputs[&group.output].name,
        ));
        query.group_by.push(expression);
    }
    for metric in metrics {
        query.select.push(SelectExpr::new(
            lower_expr_ch(bound, &metric.expression, aliases, physical_columns)?,
            &bound.outputs[&metric.output].name,
        ));
    }
    Ok(())
}

fn order_by(
    bound: &BoundCatalog,
    keys: Vec<SortKey>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Vec<OrderExpr>> {
    keys.into_iter()
        .map(|key| {
            let expression = lower_expr(bound, &key.expression, aliases)?;
            Ok(if key.descending {
                OrderExpr::desc(expression)
            } else {
                OrderExpr::asc(expression)
            })
        })
        .collect()
}

fn order_by_ch(
    bound: &BoundCatalog,
    keys: Vec<SortKey>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Vec<OrderExpr>> {
    keys.into_iter()
        .map(|key| {
            let expression = lower_expr_ch(bound, &key.expression, aliases, physical_columns)?;
            Ok(if key.descending {
                OrderExpr::desc(expression)
            } else {
                OrderExpr::asc(expression)
            })
        })
        .collect()
}

fn lower_expr(
    bound: &BoundCatalog,
    expression: &Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<ast::Expr> {
    Ok(match expression {
        Expr::Column(column) => {
            let column = &bound.columns[column];
            let alias = aliases.get(&column.relation).ok_or_else(|| {
                QueryError::Lowering(format!(
                    "relation {} has no physical alias",
                    column.relation.0
                ))
            })?;
            ast::Expr::col(alias, &column.name)
        }
        Expr::Output(output) => ast::Expr::ident(&bound.outputs[output].name),
        Expr::Literal(value) => literal(value),
        Expr::Compare { op, left, right } => ast::Expr::binary(
            compare_op(*op),
            lower_expr(bound, left, aliases)?,
            lower_expr(bound, right, aliases)?,
        ),
        Expr::Filter {
            op,
            left,
            right,
            data_type,
        } => {
            let left = lower_expr(bound, left, aliases)?;
            let right = right.as_ref().map_or_else(
                || ast::Expr::param(data_type_to_ch(data_type.as_ref()), serde_json::Value::Null),
                |right| lower_expr(bound, right, aliases).unwrap(),
            );
            match op {
                FilterOp::IsNull => ast::Expr::unary(ast::Op::IsNull, left),
                FilterOp::IsNotNull => ast::Expr::unary(ast::Op::IsNotNull, left),
                FilterOp::Contains => ast::Expr::func("positionCaseInsensitive", vec![left, right]),
                FilterOp::StartsWith => ast::Expr::func("startsWith", vec![left, right]),
                FilterOp::EndsWith => ast::Expr::func("endsWith", vec![left, right]),
                FilterOp::TokenMatch => ast::Expr::func("hasToken", vec![left, right]),
                FilterOp::AllTokens => ast::Expr::func("hasAllTokens", vec![left, right]),
                FilterOp::AnyTokens => ast::Expr::func("hasAnyTokens", vec![left, right]),
                op => ast::Expr::binary(filter_op(*op), left, right),
            }
        }
        Expr::And(values) => values
            .iter()
            .map(|value| lower_expr(bound, value, aliases))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(ast::Expr::and)
            .unwrap_or_else(|| ast::Expr::lit(1)),
        Expr::Or(values) => values
            .iter()
            .map(|value| lower_expr(bound, value, aliases))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| ast::Expr::binary(ast::Op::Or, left, right))
            .unwrap_or_else(|| ast::Expr::lit(0)),
        Expr::In {
            value,
            values,
            data_type,
        } => {
            let left = lower_expr(bound, value, aliases)?;
            let values = values.iter().map(json_value).collect::<Vec<_>>();
            if values.len() == 1 {
                ast::Expr::eq(
                    left,
                    ast::Expr::param(data_type_to_ch(data_type.as_ref()), values[0].clone()),
                )
            } else {
                ast::Expr::binary(
                    ast::Op::In,
                    left,
                    ast::Expr::param(
                        data_type_to_ch(data_type.as_ref()).to_array(),
                        serde_json::Value::Array(values),
                    ),
                )
            }
        }
        Expr::DateTrunc { unit, value } => {
            let truncated =
                ast::Expr::func(unit.ch_function(), vec![lower_expr(bound, value, aliases)?]);
            match unit {
                TruncateUnit::Minute | TruncateUnit::Hour => {
                    ast::Expr::func("toDateTime64", vec![truncated, ast::Expr::ident("0")])
                }
                _ => ast::Expr::func("toDate32", vec![truncated]),
            }
        }
        Expr::Aggregate { function, value } => ast::Expr::func(
            function.as_sql(),
            value
                .iter()
                .map(|value| lower_expr(bound, value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::Array(values) => ast::Expr::func(
            "array",
            values
                .iter()
                .map(|value| lower_expr(bound, value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::Tuple(values) => ast::Expr::func(
            "tuple",
            values
                .iter()
                .map(|value| lower_expr(bound, value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::JsonObject(entries) => {
            if entries.is_empty() {
                return Ok(ast::Expr::string("{}"));
            }
            let mut arguments = Vec::new();
            for (key, value) in entries {
                arguments.push(ast::Expr::string(key));
                arguments.push(lower_expr(bound, value, aliases)?);
            }
            ast::Expr::func("map", arguments)
        }
        Expr::Stringify(value) => {
            if matches!(value.as_ref(), Expr::JsonObject(_)) {
                ast::Expr::func("toJSONString", vec![lower_expr(bound, value, aliases)?])
            } else {
                ast::Expr::func("toString", vec![lower_expr(bound, value, aliases)?])
            }
        }
        Expr::ListContains { list, value } => ast::Expr::func(
            "has",
            vec![lower_expr(bound, list, aliases)?, literal(value)],
        ),
        Expr::TokenMatch { value, token } => ast::Expr::func(
            "hasToken",
            vec![lower_expr(bound, value, aliases)?, literal(token)],
        ),
    })
}

fn lower_expr_ch(
    bound: &BoundCatalog,
    expression: &Expr,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<ast::Expr> {
    if let Expr::Column(column) = expression
        && let Some((relation, physical)) = physical_columns.get(column)
    {
        return Ok(ast::Expr::col(&aliases[relation], &physical.0));
    }
    lower_expr(bound, expression, aliases)
}

fn physical_columns(plan: &Plan<ClickHouse>) -> BTreeMap<ColumnId, (RelationId, PhysicalColumn)> {
    let mut columns = BTreeMap::new();
    visit_ch(plan, &mut |plan| {
        if let Operator::Scan(scan) = &plan.operator
            && let ClickHouseAccess::DenormalizedJoin(access) = &scan.access
        {
            columns.extend(
                access
                    .columns
                    .iter()
                    .map(|(column, physical)| (*column, (scan.relation, physical.clone()))),
            );
        }
    });
    columns
}

fn lowered<B: Flavor>(
    bound: &BoundCatalog,
    candidate: Candidate<B>,
    aliases: HashMap<RelationId, String>,
    query: Query,
) -> Result<LoweredPlan> {
    let columns: BTreeMap<_, _> = candidate
        .columns
        .columns
        .into_iter()
        .map(|(id, expression)| Ok((id, lower_expr(bound, &expression, &aliases)?)))
        .collect::<Result<_>>()?;
    let nodes = candidate
        .outputs
        .nodes
        .into_iter()
        .map(|(node, output)| {
            Ok((
                node,
                LoweredOutputBinding {
                    primary_key: columns[&output.primary_key].clone(),
                },
            ))
        })
        .collect::<Result<_>>()?;
    Ok(LoweredPlan {
        ast: Node::Query(Box::new(query)),
        bindings: LoweredBindings { columns, nodes },
        explain: format!("scans={}", candidate.cost.scans),
    })
}

fn edge_tables(layouts: &[TableLayout], alias: &str) -> TableRef {
    match layouts {
        [layout] => TableRef::scan(&layout.table.0, alias),
        _ => TableRef::union_all(
            layouts
                .iter()
                .map(|layout| Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(&layout.table.0, alias),
                    ..Default::default()
                })
                .collect(),
            alias,
        ),
    }
}

fn union_projection(queries: &[Query]) -> Vec<SelectExpr> {
    queries
        .first()
        .map(|query| {
            query
                .select
                .iter()
                .filter_map(|select| select.alias.as_ref())
                .map(|alias| SelectExpr::new(ast::Expr::ident(alias), alias))
                .collect()
        })
        .unwrap_or_default()
}

fn select_star(mut query: Query) -> Query {
    if query.select.is_empty() {
        query.select.push(SelectExpr::star());
    }
    query
}

fn literal(value: &Value) -> ast::Expr {
    match value {
        Value::Int(value) => ast::Expr::int(*value),
        Value::Float(value) => ast::Expr::param(
            ast::ChType::Float64,
            value.parse::<f64>().unwrap_or_default(),
        ),
        Value::String(value) => ast::Expr::string(value),
        Value::Bool(value) => ast::Expr::param(ast::ChType::Bool, *value),
    }
}

fn json_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Int(value) => (*value).into(),
        Value::Float(value) => value
            .parse::<serde_json::Number>()
            .map(serde_json::Value::Number)
            .unwrap_or_default(),
        Value::String(value) => value.clone().into(),
        Value::Bool(value) => (*value).into(),
    }
}

fn compare_op(op: CompareOp) -> ast::Op {
    match op {
        CompareOp::Eq => ast::Op::Eq,
        CompareOp::Ne => ast::Op::Ne,
        CompareOp::Lt => ast::Op::Lt,
        CompareOp::Le => ast::Op::Le,
        CompareOp::Gt => ast::Op::Gt,
        CompareOp::Ge => ast::Op::Ge,
    }
}

fn filter_op(op: FilterOp) -> ast::Op {
    match op {
        FilterOp::Eq => ast::Op::Eq,
        FilterOp::Ne => ast::Op::Ne,
        FilterOp::Lt => ast::Op::Lt,
        FilterOp::Lte => ast::Op::Le,
        FilterOp::Gt => ast::Op::Gt,
        FilterOp::Gte => ast::Op::Ge,
        _ => ast::Op::Eq,
    }
}

fn relation_alias(bound: &BoundCatalog, relation: RelationId) -> String {
    match bound.relations[&relation].origin {
        RelationOrigin::Node { input } => bound.input.nodes[input.0].id.clone(),
        RelationOrigin::Edge {
            input: Some(input), ..
        } => format!("e{}", input.0),
        RelationOrigin::Edge {
            depth: Some(_),
            hop: Some(hop),
            ..
        } => format!("e{hop}"),
        RelationOrigin::Edge { .. } => format!("e{}", relation.0),
    }
}

fn and_where(query: &mut Query, expression: ast::Expr) {
    query.where_clause = Some(
        query
            .where_clause
            .take()
            .map_or(expression.clone(), |current| {
                ast::Expr::and(current, expression)
            }),
    );
}

fn set_final(table: &mut TableRef) {
    match table {
        TableRef::Scan { final_, .. } => *final_ = true,
        TableRef::Subquery { query, .. } => set_final(&mut query.from),
        TableRef::Union { queries, .. } => {
            queries
                .iter_mut()
                .for_each(|query| set_final(&mut query.from));
        }
        TableRef::Join { left, right, .. } => {
            set_final(left);
            set_final(right);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, InputNode, InputRelationship, QueryType};

    #[test]
    fn lowers_one_hop_to_existing_ast() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "mr".into(),
                hops: Default::default(),
                direction: Direction::Outgoing,
                filters: Default::default(),
                fk_column: None,
                scope_prefix: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let (bound, logical) = bind(input, std::sync::Arc::new(ontology)).unwrap();
        let clickhouse = plan_clickhouse(&bound, logical.clone()).unwrap();
        let duckdb = plan_duckdb(&bound, logical).unwrap();
        assert!(matches!(
            lower_clickhouse(&bound, clickhouse.selected).unwrap().ast,
            Node::Query(_)
        ));
        assert!(matches!(
            lower_duckdb(&bound, duckdb.selected).unwrap().ast,
            Node::Query(_)
        ));
    }
}
