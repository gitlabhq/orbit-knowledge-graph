use super::*;
use crate::ast::{JoinType, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::passes::shared::data_type_to_ch;
use std::collections::HashMap;

pub fn lower_clickhouse(
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    selected: SelectedPlan<ClickHouse>,
) -> Result<LoweredPlan> {
    lower(bound, selected.candidate)
}

pub fn lower_duckdb(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    selected: SelectedPlan<DuckDb>,
) -> Result<LoweredPlan> {
    lower(bound, selected.candidate)
}

trait LowerFlavor: Flavor {
    type Model: QueryDataModel;

    fn scan(bound: &BoundCatalog<Self::Model>, scan: Self::Scan, alias: &str) -> Result<Query>;

    fn physical_columns(scan: &Self::Scan) -> BTreeMap<ColumnId, PhysicalColumn>;

    fn filter(
        bound: &BoundCatalog<Self::Model>,
        query: Query,
        expression: ast::Expr,
        aliases: &HashMap<RelationId, String>,
    ) -> Query;

    fn current_rows(
        bound: &BoundCatalog<Self::Model>,
        strategy: Self::CurrentRows,
        keys: Vec<Expr>,
        query: Query,
        aliases: &HashMap<RelationId, String>,
        columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
    ) -> Result<Query>;
}

impl LowerFlavor for ClickHouse {
    type Model = query_data_model::ClickHouseDataModel;

    fn scan(bound: &BoundCatalog<Self::Model>, scan: Self::Scan, alias: &str) -> Result<Query> {
        let from = match scan.access {
            ClickHouseAccess::Table(access) => TableRef::scan(access.layout.table.0, alias),
            ClickHouseAccess::EdgeTables(access) => match access.layouts.as_slice() {
                [layout] => TableRef::scan(&layout.table.0, alias),
                layouts => TableRef::union_all(
                    layouts
                        .iter()
                        .map(|layout| Query {
                            select: bound
                                .columns
                                .values()
                                .filter(|column| column.relation == scan.relation)
                                .map(|column| SelectExpr::col(alias, &column.name))
                                .collect(),
                            from: TableRef::scan(&layout.table.0, alias),
                            ..Default::default()
                        })
                        .collect(),
                    alias,
                ),
            },
        };
        Ok(Query {
            from,
            ..Default::default()
        })
    }

    fn physical_columns(scan: &Self::Scan) -> BTreeMap<ColumnId, PhysicalColumn> {
        match &scan.access {
            ClickHouseAccess::EdgeTables(access) => access.columns.clone(),
            ClickHouseAccess::Table(_) => BTreeMap::new(),
        }
    }

    fn filter(
        bound: &BoundCatalog<Self::Model>,
        mut query: Query,
        expression: ast::Expr,
        aliases: &HashMap<RelationId, String>,
    ) -> Query {
        and_where(&mut query, expression);
        let TableRef::Scan {
            alias,
            final_: true,
            ..
        } = &query.from
        else {
            return query;
        };
        let Some(relation) = aliases
            .iter()
            .find_map(|(relation, candidate)| (candidate == alias).then_some(*relation))
        else {
            return query;
        };
        if !matches!(bound.relation(relation).origin, RelationOrigin::Edge { .. }) {
            return query;
        }
        let alias = alias.clone();
        Query {
            from: TableRef::subquery(select_star(query), alias),
            ..Default::default()
        }
    }

    fn current_rows(
        bound: &BoundCatalog<Self::Model>,
        strategy: Self::CurrentRows,
        keys: Vec<Expr>,
        mut query: Query,
        aliases: &HashMap<RelationId, String>,
        columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
    ) -> Result<Query> {
        let predicates = query.where_clause.take();
        match strategy {
            ClickHouseCurrentRows::Final => {
                set_final(&mut query.from);
                if let Some(predicate) = predicates {
                    let alias = table_aliases(&query.from)
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| "current".into());
                    query = Query {
                        from: TableRef::subquery(
                            Query {
                                where_clause: Some(predicate),
                                ..query
                            },
                            alias,
                        ),
                        ..Default::default()
                    };
                }
            }
            ClickHouseCurrentRows::LimitBy => {
                let keys = if keys.is_empty() {
                    scan_sort_key(&query.from, bound, aliases)
                } else {
                    keys.iter()
                        .map(|key| lower_expr(bound, key, aliases, columns))
                        .collect::<Result<_>>()?
                };
                let alias = table_aliases(&query.from)
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                query.order_by = keys
                    .iter()
                    .cloned()
                    .map(OrderExpr::asc)
                    .chain(std::iter::once(OrderExpr::desc(ast::Expr::col(
                        alias,
                        ontology::constants::VERSION_COLUMN,
                    ))))
                    .collect();
                query.limit_by = Some((1, keys));
                query.where_clause = predicates;
            }
        }
        Ok(query)
    }
}

impl LowerFlavor for DuckDb {
    type Model = query_data_model::DuckDbDataModel;

    fn scan(_bound: &BoundCatalog<Self::Model>, scan: Self::Scan, alias: &str) -> Result<Query> {
        let DuckDbAccess::Table(access) = scan.access;
        Ok(Query {
            from: TableRef::scan(access.layout.table.0, alias),
            ..Default::default()
        })
    }

    fn physical_columns(_scan: &Self::Scan) -> BTreeMap<ColumnId, PhysicalColumn> {
        BTreeMap::new()
    }

    fn filter(
        _bound: &BoundCatalog<Self::Model>,
        mut query: Query,
        expression: ast::Expr,
        _aliases: &HashMap<RelationId, String>,
    ) -> Query {
        and_where(&mut query, expression);
        query
    }

    fn current_rows(
        _bound: &BoundCatalog<Self::Model>,
        _strategy: Self::CurrentRows,
        _keys: Vec<Expr>,
        query: Query,
        _aliases: &HashMap<RelationId, String>,
        _columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
    ) -> Result<Query> {
        Ok(query)
    }
}

fn lower<F: LowerFlavor>(
    bound: &BoundCatalog<F::Model>,
    candidate: Candidate<F>,
) -> Result<LoweredPlan> {
    let aliases = aliases(bound, &candidate.plan);
    let columns = physical_columns::<F>(&candidate.plan);
    let query = lower_plan::<F>(bound, candidate.plan.clone(), &aliases, &columns)?;
    lowered(bound, candidate, aliases, query)
}

fn physical_columns<F: LowerFlavor>(
    plan: &Plan<F>,
) -> BTreeMap<ColumnId, (RelationId, PhysicalColumn)> {
    let mut columns = BTreeMap::new();
    plan.visit(&mut |plan| {
        if let Operator::Scan(scan) = &plan.operator {
            columns.extend(
                F::physical_columns(scan)
                    .into_iter()
                    .map(|(column, physical)| (column, (scan.relation(), physical))),
            );
        }
    });
    columns
}

fn aliases<M: QueryDataModel, F: Flavor>(
    bound: &BoundCatalog<M>,
    plan: &Plan<F>,
) -> HashMap<RelationId, String> {
    let mut aliases = HashMap::new();
    let mut manager = crate::aliases::AliasManager::default();
    for node in &bound.input.nodes {
        manager.reserve(&node.id);
    }
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            let relation = scan.relation();
            let preferred = relation_alias(bound, relation);
            let key = match bound.relation(relation).origin {
                RelationOrigin::Node { input, .. } => bound.input.nodes[input.0].id.clone(),
                RelationOrigin::Edge { .. } => format!("source:{}", relation.0),
            };
            aliases.insert(relation, manager.generated(&preferred, key));
        }
        Operator::Bind(relation) => {
            let preferred = relation_alias(bound, *relation);
            aliases.insert(
                *relation,
                manager.generated(&preferred, format!("bind:{}", relation.0)),
            );
        }
        _ => {}
    });
    aliases
}

fn lower_plan<F: LowerFlavor>(
    bound: &BoundCatalog<F::Model>,
    plan: Plan<F>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    match plan.operator {
        Operator::Scan(scan) => F::scan(bound, scan.clone(), &aliases[&scan.relation()]),
        Operator::Filter(expression) => {
            let query = only(bound, plan.inputs, aliases, physical_columns)?;
            Ok(F::filter(
                bound,
                query,
                lower_expr(bound, &expression, aliases, physical_columns)?,
                aliases,
            ))
        }
        Operator::Project(columns) => {
            let mut query = only(bound, plan.inputs, aliases, physical_columns)?;
            query.select = projection(bound, columns, aliases, physical_columns)?;
            Ok(query)
        }
        Operator::Join(conditions) => {
            lower_join::<F>(bound, plan.inputs, conditions, aliases, physical_columns)
        }
        Operator::SemiJoin(condition) => {
            lower_semi::<F>(bound, plan.inputs, condition, aliases, physical_columns)
        }
        Operator::Aggregate { groups, metrics } => {
            let mut query = only(bound, plan.inputs, aliases, physical_columns)?;
            let limit_by_predicate = query.limit_by.is_some().then(|| {
                query
                    .where_clause
                    .clone()
                    .unwrap_or_else(|| ast::Expr::lit(1))
            });
            lower_aggregate(
                bound,
                &mut query,
                groups,
                metrics,
                aliases,
                physical_columns,
                limit_by_predicate.as_ref(),
            )?;
            Ok(query)
        }
        Operator::Union => {
            let queries = plan
                .inputs
                .into_iter()
                .map(|input| lower_plan::<F>(bound, input, aliases, physical_columns))
                .collect::<Result<Vec<_>>>()?;
            Ok(Query {
                select: union_projection(&queries),
                from: TableRef::union_all(queries, union_alias(bound)),
                ..Default::default()
            })
        }
        Operator::Bind(relation) => Ok(Query {
            from: TableRef::subquery(
                select_star(only(bound, plan.inputs, aliases, physical_columns)?),
                &aliases[&relation],
            ),
            ..Default::default()
        }),
        Operator::Sort(keys) => {
            let mut query = only(bound, plan.inputs, aliases, physical_columns)?;
            query.order_by = order_by(bound, keys, aliases, physical_columns)?;
            Ok(query)
        }
        Operator::Limit(limit) => {
            let mut query = only(bound, plan.inputs, aliases, physical_columns)?;
            query.limit = Some(limit);
            Ok(query)
        }
        Operator::CurrentRows { keys, strategy } => F::current_rows(
            bound,
            strategy,
            keys,
            only(bound, plan.inputs, aliases, physical_columns)?,
            aliases,
            physical_columns,
        ),
        _ => Err(QueryError::Lowering("operator is not populated".into())),
    }
}

fn lower_join<F: LowerFlavor>(
    bound: &BoundCatalog<F::Model>,
    inputs: Vec<Plan<F>>,
    conditions: Vec<Expr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let mut inputs = inputs;
    let first = (!inputs.is_empty())
        .then_some(())
        .ok_or_else(|| QueryError::Lowering("join needs an input".into()))
        .map(|_| inputs.remove(0))?;
    let mut available = first.visible_relations();
    let mut query = lower_plan::<F>(bound, first, aliases, physical_columns)?;
    while !inputs.is_empty() {
        let index = inputs
            .iter()
            .position(|input| {
                let right = input.visible_relations();
                conditions.iter().any(|condition| {
                    let relations = expression_relations(bound, condition);
                    !relations.is_disjoint(&right)
                        && !relations.is_disjoint(&available)
                        && relations.is_subset(&available.union(&right).copied().collect())
                })
            })
            .unwrap_or(0);
        let input = inputs.remove(index);
        let right_relations = input.visible_relations();
        let joined_relations = available.union(&right_relations).copied().collect();
        let alias = input
            .relation()
            .and_then(|relation| aliases.get(&relation))
            .cloned()
            .unwrap_or_else(|| "right".into());
        let condition = join_condition(
            bound,
            &conditions,
            &right_relations,
            &joined_relations,
            aliases,
            physical_columns,
        )?;
        query.from = TableRef::join(
            JoinType::Inner,
            query.from,
            TableRef::subquery(
                select_star(lower_plan::<F>(bound, input, aliases, physical_columns)?),
                alias,
            ),
            condition,
        );
        available = joined_relations;
    }
    Ok(query)
}

fn lower_semi<F: LowerFlavor>(
    bound: &BoundCatalog<F::Model>,
    inputs: Vec<Plan<F>>,
    condition: Expr,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let [consumer, producer]: [Plan<F>; 2] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("semi-join needs two inputs".into()))?;
    let mut consumer_query = lower_plan::<F>(bound, consumer, aliases, physical_columns)?;
    let mut producer_query = lower_plan::<F>(bound, producer, aliases, physical_columns)?;
    let Expr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = condition
    else {
        return Err(QueryError::Lowering("semi-join needs equality".into()));
    };
    producer_query.select = vec![SelectExpr {
        expr: lower_expr(bound, &right, aliases, physical_columns)?,
        alias: None,
    }];
    and_where(
        &mut consumer_query,
        ast::Expr::InSelect {
            expr: Box::new(lower_expr(bound, &left, aliases, physical_columns)?),
            query: Box::new(producer_query),
        },
    );
    Ok(consumer_query)
}

fn join_condition<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    conditions: &[Expr],
    right_relations: &BTreeSet<RelationId>,
    joined_relations: &BTreeSet<RelationId>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<ast::Expr> {
    Ok(conditions
        .iter()
        .filter(|condition| {
            let relations = expression_relations(bound, condition);
            !relations.is_disjoint(right_relations) && relations.is_subset(joined_relations)
        })
        .map(|condition| lower_expr(bound, condition, aliases, physical_columns))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .reduce(ast::Expr::and)
        .unwrap_or_else(|| ast::Expr::lit(1)))
}

fn expression_relations<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    expression: &Expr,
) -> BTreeSet<RelationId> {
    expression
        .columns()
        .into_iter()
        .map(|column| bound.column(column).relation)
        .collect()
}

fn only<F: LowerFlavor>(
    bound: &BoundCatalog<F::Model>,
    inputs: Vec<Plan<F>>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Query> {
    let [input]: [Plan<F>; 1] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("unary operator needs one input".into()))?;
    lower_plan::<F>(bound, input, aliases, physical_columns)
}

fn projection<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    columns: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Vec<SelectExpr>> {
    columns
        .into_iter()
        .map(|column| {
            Ok(SelectExpr::new(
                lower_expr(bound, &column.expression, aliases, physical_columns)?,
                &bound.outputs[&column.output].name,
            ))
        })
        .collect()
}

fn lower_aggregate<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    query: &mut Query,
    groups: Vec<NamedExpr>,
    metrics: Vec<NamedExpr>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
    limit_by_predicate: Option<&ast::Expr>,
) -> Result<()> {
    for group in groups {
        let expression = lower_expr(bound, &group.expression, aliases, physical_columns)?;
        query.select.push(SelectExpr::new(
            expression.clone(),
            &bound.outputs[&group.output].name,
        ));
        query.group_by.push(expression);
    }
    for metric in metrics {
        let expression = match (&metric.expression, limit_by_predicate) {
            (Expr::Aggregate { function, value }, Some(predicate)) => {
                let mut arguments = value
                    .iter()
                    .map(|value| lower_expr(bound, value, aliases, physical_columns))
                    .collect::<Result<Vec<_>>>()?;
                arguments.push(predicate.clone());
                ast::Expr::func(function.as_sql_if(), arguments)
            }
            _ => lower_expr(bound, &metric.expression, aliases, physical_columns)?,
        };
        query.select.push(SelectExpr::new(
            expression,
            &bound.outputs[&metric.output].name,
        ));
    }
    Ok(())
}

fn order_by<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    keys: Vec<SortKey>,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<Vec<OrderExpr>> {
    keys.into_iter()
        .map(|key| {
            let expression = lower_expr(bound, &key.expression, aliases, physical_columns)?;
            Ok(if key.descending {
                OrderExpr::desc(expression)
            } else {
                OrderExpr::asc(expression)
            })
        })
        .collect()
}

fn lower_expr<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    expression: &Expr,
    aliases: &HashMap<RelationId, String>,
    physical_columns: &BTreeMap<ColumnId, (RelationId, PhysicalColumn)>,
) -> Result<ast::Expr> {
    Ok(match expression {
        Expr::Column(column) => {
            if let Some((relation, physical)) = physical_columns.get(column) {
                return Ok(ast::Expr::col(&aliases[relation], &physical.0));
            }
            let column = bound.column(*column);
            let alias = aliases.get(&column.relation).ok_or_else(|| {
                QueryError::Lowering(format!(
                    "relation {} has no physical alias",
                    column.relation.0
                ))
            })?;
            ast::Expr::col(alias, &column.name)
        }
        Expr::Output(output) => {
            let name = &bound.outputs[output].name;
            if bound.input.query_type == crate::input::QueryType::PathFinding {
                ast::Expr::col("paths", name)
            } else {
                ast::Expr::ident(name)
            }
        }
        Expr::Literal(value) => literal(value),
        Expr::Compare { op, left, right } => ast::Expr::binary(
            compare_op(*op),
            lower_expr(bound, left, aliases, physical_columns)?,
            lower_expr(bound, right, aliases, physical_columns)?,
        ),
        Expr::Filter {
            op,
            left,
            right,
            data_type,
        } => {
            let left = lower_expr(bound, left, aliases, physical_columns)?;
            let right = right.as_ref().map_or_else(
                || ast::Expr::param(data_type_to_ch(data_type.as_ref()), serde_json::Value::Null),
                |right| match right.as_ref() {
                    Expr::Literal(value) => {
                        ast::Expr::param(data_type_to_ch(data_type.as_ref()), json_value(value))
                    }
                    right => lower_expr(bound, right, aliases, physical_columns).unwrap(),
                },
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
            .map(|value| lower_expr(bound, value, aliases, physical_columns))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(ast::Expr::and)
            .unwrap_or_else(|| ast::Expr::lit(1)),
        Expr::Or(values) => values
            .iter()
            .map(|value| lower_expr(bound, value, aliases, physical_columns))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| ast::Expr::binary(ast::Op::Or, left, right))
            .unwrap_or_else(|| ast::Expr::lit(0)),
        Expr::In {
            value,
            values,
            data_type,
        } => {
            let left = lower_expr(bound, value, aliases, physical_columns)?;
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
            let truncated = ast::Expr::func(
                unit.ch_function(),
                vec![lower_expr(bound, value, aliases, physical_columns)?],
            );
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
                .map(|value| lower_expr(bound, value, aliases, physical_columns))
                .collect::<Result<_>>()?,
        ),
        Expr::Array(values) => ast::Expr::func(
            "array",
            values
                .iter()
                .map(|value| lower_expr(bound, value, aliases, physical_columns))
                .collect::<Result<_>>()?,
        ),
        Expr::Tuple(values) => ast::Expr::func(
            "tuple",
            values
                .iter()
                .map(|value| lower_expr(bound, value, aliases, physical_columns))
                .collect::<Result<_>>()?,
        ),
        Expr::JsonObject(entries) => {
            if entries.is_empty() {
                return Ok(ast::Expr::string("{}"));
            }
            let mut arguments = Vec::new();
            for (key, value) in entries {
                arguments.push(ast::Expr::string(key));
                arguments.push(lower_expr(bound, value, aliases, physical_columns)?);
            }
            ast::Expr::func("map", arguments)
        }
        Expr::Stringify(value) => {
            if matches!(value.as_ref(), Expr::JsonObject(_)) {
                ast::Expr::func(
                    "toJSONString",
                    vec![lower_expr(bound, value, aliases, physical_columns)?],
                )
            } else {
                ast::Expr::func(
                    "toString",
                    vec![lower_expr(bound, value, aliases, physical_columns)?],
                )
            }
        }
        Expr::ListContains { list, values } => {
            let list = lower_expr(bound, list, aliases, physical_columns)?;
            if values.len() == 1 {
                ast::Expr::func("has", vec![list, literal(&values[0])])
            } else {
                ast::Expr::func(
                    "hasAny",
                    vec![
                        list,
                        ast::Expr::func("array", values.iter().map(literal).collect()),
                    ],
                )
            }
        }
        Expr::TokenMatch { value, token } => ast::Expr::func(
            "hasToken",
            vec![
                lower_expr(bound, value, aliases, physical_columns)?,
                literal(token),
            ],
        ),
        Expr::PathPrefixAny { value, paths } => {
            let path = "_gkg_path";
            ast::Expr::func(
                "arrayExists",
                vec![
                    ast::Expr::lambda(
                        path,
                        ast::Expr::func(
                            "startsWith",
                            vec![
                                lower_expr(bound, value, aliases, physical_columns)?,
                                ast::Expr::ident(path),
                            ],
                        ),
                    ),
                    ast::Expr::param(
                        ast::ChType::String.to_array(),
                        serde_json::Value::Array(
                            paths
                                .iter()
                                .cloned()
                                .map(serde_json::Value::String)
                                .collect(),
                        ),
                    ),
                ],
            )
        }
    })
}

fn table_aliases(table: &TableRef) -> Vec<String> {
    match table {
        TableRef::Scan { alias, .. }
        | TableRef::Union { alias, .. }
        | TableRef::Subquery { alias, .. } => vec![alias.clone()],
        TableRef::Join { left, right, .. } => table_aliases(left)
            .into_iter()
            .chain(table_aliases(right))
            .collect(),
    }
}

fn lowered<M: QueryDataModel, B: Flavor>(
    bound: &BoundCatalog<M>,
    candidate: Candidate<B>,
    aliases: HashMap<RelationId, String>,
    query: Query,
) -> Result<LoweredPlan> {
    let visible = candidate.plan.visible_relations();
    let mut node_sources: HashMap<_, _> = bound
        .relations()
        .filter_map(|(relation, metadata)| {
            let RelationOrigin::Node { input, .. } = metadata.origin else {
                return None;
            };
            visible.contains(&relation).then(|| {
                (
                    bound.input.nodes[input.0].id.clone(),
                    (
                        aliases[&relation].clone(),
                        ontology::constants::DEFAULT_PRIMARY_KEY.into(),
                    ),
                )
            })
        })
        .collect();
    let top_level_aliases: BTreeSet<_> = table_aliases(&query.from).into_iter().collect();
    node_sources.retain(|_, (alias, _)| top_level_aliases.contains(alias));
    for (relation, metadata) in bound.relations() {
        let RelationOrigin::Edge {
            input: Some(input),
            depth: None,
            hop: None,
            ..
        } = metadata.origin
        else {
            continue;
        };
        let relationship = &bound.input.relationships[input.0];
        let (source, target) = relationship.direction.edge_columns();
        for (node, column) in [(&relationship.from, source), (&relationship.to, target)] {
            if node_sources.contains_key(node) {
                continue;
            }
            if bound.column_id(relation, column).is_none() {
                continue;
            }
            let Some(expression) = aliases
                .get(&relation)
                .map(|alias| ast::Expr::col(alias, column))
            else {
                continue;
            };
            let ast::Expr::Column { table, column } = expression else {
                continue;
            };
            node_sources.insert(node.clone(), (table, column));
        }
    }
    let edges = bound
        .input
        .relationships
        .iter()
        .enumerate()
        .map(|(index, relationship)| {
            let column_prefix = if relationship.hops.max > 1 {
                format!("hop_e{index}_")
            } else {
                format!("e{index}_")
            };
            LoweredEdge {
                path_column: (relationship.hops.max > 1)
                    .then(|| format!("{column_prefix}path_nodes")),
                column_prefix,
                rel_types: relationship.types.clone(),
            }
        })
        .collect();
    let stable_order = stable_order(bound, &query, &node_sources);
    let mut alias_manager = crate::aliases::AliasManager::default();
    for alias in aliases.values() {
        alias_manager.reserve(alias);
    }
    Ok(LoweredPlan {
        ast: Node::Query(Box::new(query)),
        metadata: LoweredMetadata {
            node_sources,
            aliases: alias_manager,
            edges,
            stable_order,
        },
    })
}

fn stable_order<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    query: &Query,
    node_sources: &HashMap<String, (String, String)>,
) -> Vec<OrderExpr> {
    match bound.input.query_type {
        crate::input::QueryType::Aggregation => {
            query.group_by.iter().cloned().map(OrderExpr::asc).collect()
        }
        crate::input::QueryType::PathFinding => vec![
            OrderExpr::asc(ast::Expr::col("paths", crate::constants::path_column())),
            OrderExpr::asc(ast::Expr::col(
                "paths",
                crate::constants::edge_kinds_column(),
            )),
        ],
        crate::input::QueryType::Neighbors => bound
            .input
            .neighbors
            .as_ref()
            .filter(|neighbors| neighbors.direction == crate::input::Direction::Both)
            .map_or_else(Vec::new, |_| {
                vec![
                    OrderExpr::asc(ast::Expr::ident(crate::constants::redaction_id_column(
                        &bound.input.nodes[0].id,
                    ))),
                    OrderExpr::asc(ast::Expr::ident(crate::constants::neighbor_id_column())),
                    OrderExpr::asc(ast::Expr::ident(
                        crate::constants::relationship_type_column(),
                    )),
                    OrderExpr::asc(ast::Expr::ident(
                        crate::constants::neighbor_is_outgoing_column(),
                    )),
                ]
            }),
        _ => bound
            .input
            .nodes
            .iter()
            .filter_map(|node| node_sources.get(&node.id))
            .map(|(alias, column)| OrderExpr::asc(ast::Expr::col(alias, column)))
            .collect(),
    }
}

fn union_alias<M: QueryDataModel>(bound: &BoundCatalog<M>) -> &'static str {
    if bound.input.query_type == crate::input::QueryType::PathFinding {
        "paths"
    } else {
        "union"
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

fn relation_alias<M: QueryDataModel>(bound: &BoundCatalog<M>, relation: RelationId) -> String {
    match bound.relation(relation).origin {
        RelationOrigin::Node { input, .. } => bound.input.nodes[input.0].id.clone(),
        RelationOrigin::Edge {
            input: Some(input),
            depth: None,
            hop: None,
            ..
        } if bound.input.relationships[input.0].hops.max > 1 => {
            format!("e{}", input.0)
        }
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

fn scan_sort_key(
    table: &TableRef,
    bound: &BoundCatalog<query_data_model::ClickHouseDataModel>,
    aliases: &HashMap<RelationId, String>,
) -> Vec<ast::Expr> {
    let (name, alias) = match table {
        TableRef::Scan { table, alias, .. } => (table, alias),
        TableRef::Subquery { query, .. } => return scan_sort_key(&query.from, bound, aliases),
        _ => return vec![],
    };
    bound
        .relations()
        .find_map(|(relation, _)| (aliases.get(&relation) == Some(alias)).then(|| relation))
        .and_then(|_| {
            let layout = bound.model.backend().table(name)?;
            Some(
                layout
                    .sort_key
                    .iter()
                    .map(|column| ast::Expr::col(alias, column))
                    .collect(),
            )
        })
        .unwrap_or_default()
}
