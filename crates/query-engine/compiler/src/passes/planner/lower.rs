use super::*;
use crate::ast::{JoinType, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::passes::shared::data_type_to_ch;
use std::collections::HashMap;

pub fn lower_duckdb(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    selected: SelectedPlan<DuckDb>,
) -> Result<LoweredPlan> {
    let aliases = aliases_duckdb(bound, &selected.candidate.plan);
    let query = lower_duck(bound, selected.candidate.plan.clone(), &aliases)?;
    lowered(bound, selected.candidate, aliases, query)
}

fn aliases_duckdb(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    plan: &Plan<DuckDb>,
) -> HashMap<RelationId, String> {
    let mut aliases = HashMap::new();
    let mut manager = crate::aliases::AliasManager::default();
    for node in &bound.input.nodes {
        manager.reserve(&node.id);
    }
    visit_duck(plan, &mut |plan| match &plan.operator {
        Operator::Scan(scan) => {
            let preferred = relation_alias(bound, scan.relation);
            let key = match bound.relation(scan.relation).origin {
                RelationOrigin::Node { input, .. } => bound.input.nodes[input.0].id.clone(),
                RelationOrigin::Edge { .. } => format!("source:{}", scan.relation.0),
            };
            aliases.insert(
                scan.relation,
                manager.generated(&preferred, key),
            );
        }
        Operator::Bind(relation) => {
            let preferred = relation_alias(bound, *relation);
            aliases.insert(
                *relation,
                manager.generated(
                    &preferred,
                    format!("bind:{}", relation.0),
                ),
            );
        }
        _ => {}
    });
    aliases
}

fn visit_duck(plan: &Plan<DuckDb>, visitor: &mut impl FnMut(&Plan<DuckDb>)) {
    visitor(plan);
    plan.inputs
        .iter()
        .for_each(|input| visit_duck(input, visitor));
}

fn lower_duck(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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
                from: TableRef::union_all(queries, union_alias(bound)),
                ..Default::default()
            })
        }
        Operator::Bind(relation) => Ok(Query {
            from: TableRef::subquery(
                select_star(only_duck(bound, plan.inputs, aliases)?),
                &aliases[&relation],
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

fn lower_join_duck(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    inputs: Vec<Plan<DuckDb>>,
    conditions: Vec<Expr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let mut inputs = inputs;
    let first = (!inputs.is_empty())
        .then_some(())
        .ok_or_else(|| QueryError::Lowering("join needs an input".into()))
        .map(|_| inputs.remove(0))?;
    let mut available = first.visible_relations();
    let mut query = lower_duck(bound, first, aliases)?;
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
        )?;
        query.from = TableRef::join(
            JoinType::Inner,
            query.from,
            TableRef::subquery(select_star(lower_duck(bound, input, aliases)?), alias),
            condition,
        );
        available = joined_relations;
    }
    Ok(query)
}

fn lower_semi_duck(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    conditions: &[Expr],
    right_relations: &BTreeSet<RelationId>,
    joined_relations: &BTreeSet<RelationId>,
    aliases: &HashMap<RelationId, String>,
) -> Result<ast::Expr> {
    Ok(conditions
        .iter()
        .filter(|condition| {
            let relations = expression_relations(bound, condition);
            !relations.is_disjoint(right_relations) && relations.is_subset(joined_relations)
        })
        .map(|condition| lower_expr(bound, condition, aliases))
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

fn only_duck(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
    inputs: Vec<Plan<DuckDb>>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let [input]: [Plan<DuckDb>; 1] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("unary operator needs one input".into()))?;
    lower_duck(bound, input, aliases)
}

fn projection(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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

fn lower_aggregate(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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

fn order_by(
    bound: &BoundCatalog<query_data_model::DuckDbDataModel>,
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

fn lower_expr<M: QueryDataModel>(
    bound: &BoundCatalog<M>,
    expression: &Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<ast::Expr> {
    Ok(match expression {
        Expr::Column(column) => {
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
                |right| match right.as_ref() {
                    Expr::Literal(value) => {
                        ast::Expr::param(data_type_to_ch(data_type.as_ref()), json_value(value))
                    }
                    right => lower_expr(bound, right, aliases).unwrap(),
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
        Expr::ListContains { list, values } => {
            let list = lower_expr(bound, list, aliases)?;
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
            vec![lower_expr(bound, value, aliases)?, literal(token)],
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
                            vec![lower_expr(bound, value, aliases)?, ast::Expr::ident(path)],
                        ),
                    ),
                    ast::Expr::param(
                        ast::ChType::String.to_array(),
                        serde_json::Value::Array(
                            paths.iter().cloned().map(serde_json::Value::String).collect(),
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
                    (aliases[&relation].clone(), ontology::constants::DEFAULT_PRIMARY_KEY.into()),
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
