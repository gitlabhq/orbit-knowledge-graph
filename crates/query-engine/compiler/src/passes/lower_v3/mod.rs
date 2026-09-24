use crate::ast::{JoinType, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::passes::logical_v3::{CompareOp, Expr, RelationId, Value};
use crate::passes::physical_v3::{
    Backend, ClickHouse, ClickHouseAccess, ClickHouseDedup, DuckDb, DuckDbAccess, DuckDbDedup,
    PhysicalOp, PhysicalPlan,
};
use crate::passes::shared::data_type_to_ch;
use std::collections::HashMap;

pub fn clickhouse(plan: PhysicalPlan<ClickHouse>) -> Result<Node> {
    let aliases = aliases(&plan);
    Ok(Node::Query(Box::new(lower::<ClickHouseLower>(plan, &aliases)?)))
}

pub fn duckdb(plan: PhysicalPlan<DuckDb>) -> Result<Node> {
    let aliases = aliases(&plan);
    Ok(Node::Query(Box::new(lower::<DuckDbLower>(plan, &aliases)?)))
}

trait LowerBackend {
    type Backend: Backend;

    fn scan(access: <Self::Backend as Backend>::Access, alias: &str) -> Result<TableRef>;
    fn deduplicate(
        query: Query,
        strategy: <Self::Backend as Backend>::Dedup,
        keys: &[Expr],
    ) -> Result<Query>;
    fn fused_neighbors(
        input: Query,
        outgoing: &Expr,
        incoming: &Expr,
        columns: &[crate::passes::logical_v3::NamedExpr],
        aliases: &HashMap<RelationId, String>,
    ) -> Result<Query>;
    fn date_trunc(
        unit: crate::input::TruncateUnit,
        value: crate::ast::Expr,
    ) -> crate::ast::Expr;
    fn array(values: Vec<crate::ast::Expr>) -> crate::ast::Expr;
    fn tuple(values: Vec<crate::ast::Expr>) -> crate::ast::Expr;
    fn json_object(values: Vec<crate::ast::Expr>) -> crate::ast::Expr;
    fn stringify(value: crate::ast::Expr) -> crate::ast::Expr;
    fn starts_with(value: crate::ast::Expr, prefix: crate::ast::Expr) -> crate::ast::Expr;
    fn list_contains(value: crate::ast::Expr, item: crate::ast::Expr) -> crate::ast::Expr;
    fn token_match(value: crate::ast::Expr, token: crate::ast::Expr) -> crate::ast::Expr;
    fn contains(value: crate::ast::Expr, needle: crate::ast::Expr) -> crate::ast::Expr;
    fn ends_with(value: crate::ast::Expr, suffix: crate::ast::Expr) -> crate::ast::Expr;
}

fn aliases<B: Backend>(plan: &PhysicalPlan<B>) -> HashMap<RelationId, String> {
    let mut aliases = HashMap::new();
    plan.visit(&mut |plan| match &plan.op {
        PhysicalOp::Scan {
            relation, alias, ..
        }
        | PhysicalOp::Alias { relation, alias } => {
            aliases.insert(*relation, alias.clone());
        }
        _ => {}
    });
    aliases
}

struct ClickHouseLower;
struct DuckDbLower;

impl LowerBackend for ClickHouseLower {
    type Backend = ClickHouse;

    fn scan(access: ClickHouseAccess, alias: &str) -> Result<TableRef> {
        Ok(match access {
            ClickHouseAccess::Table(table) => TableRef::scan(table, alias),
            ClickHouseAccess::EdgeTables(tables) => edge_tables(tables, alias),
        })
    }

    fn deduplicate(
        mut query: Query,
        strategy: ClickHouseDedup,
        keys: &[Expr],
    ) -> Result<Query> {
        match strategy {
            ClickHouseDedup::Final => set_final(&mut query.from),
            ClickHouseDedup::LimitBy => {
                let keys = keys
                    .iter()
                    .map(|key| lower_expr::<Self>(key, &HashMap::new()))
                    .collect::<Result<Vec<_>>>()?;
                let alias = alias(&query.from).to_string();
                query.order_by = keys.iter().cloned().map(OrderExpr::asc).collect();
                query.order_by.push(OrderExpr::desc(crate::ast::Expr::col(
                    alias,
                    ontology::constants::VERSION_COLUMN,
                )));
                query.limit_by = Some((1, keys));
            }
        }
        Ok(query)
    }

    fn fused_neighbors(
        mut input: Query,
        outgoing: &Expr,
        incoming: &Expr,
        columns: &[crate::passes::logical_v3::NamedExpr],
        aliases: &HashMap<RelationId, String>,
    ) -> Result<Query> {
        let row = "_neighbor_arm";
        let tuple = |predicate: &Expr, outgoing: bool| -> Result<crate::ast::Expr> {
            Ok(crate::ast::Expr::func(
                "tuple",
                vec![
                    lower_expr::<Self>(predicate, aliases)?,
                    crate::ast::Expr::param(crate::ast::ChType::Bool, outgoing),
                ],
            ))
        };
        let matched = crate::ast::Expr::func(
            "arrayFilter",
            vec![
                crate::ast::Expr::lambda(
                    "arm",
                    crate::ast::Expr::func(
                        "tupleElement",
                        vec![crate::ast::Expr::ident("arm"), crate::ast::Expr::lit(1)],
                    ),
                ),
                crate::ast::Expr::func("array", vec![tuple(outgoing, true)?, tuple(incoming, false)?]),
            ],
        );
        input.select = vec![SelectExpr::new(
            crate::ast::Expr::func("arrayJoin", vec![matched]),
            row,
        )];
        input.where_clause = Some(crate::ast::Expr::binary(
            crate::ast::Op::Or,
            lower_expr::<Self>(outgoing, aliases)?,
            lower_expr::<Self>(incoming, aliases)?,
        ));
        Ok(Query {
            select: columns
                .iter()
                .map(|column| {
                    Ok(SelectExpr::new(
                        lower_expr::<Self>(&column.expression, aliases)?,
                        &column.name,
                    ))
                })
                .collect::<Result<_>>()?,
            from: TableRef::subquery(select_star(input), "neighbors"),
            ..Default::default()
        })
    }

    fn date_trunc(unit: crate::input::TruncateUnit, value: crate::ast::Expr) -> crate::ast::Expr {
        crate::ast::Expr::func(unit.ch_function(), vec![value])
    }
    fn array(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("array", values) }
    fn tuple(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("tuple", values) }
    fn json_object(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("map", values) }
    fn stringify(value: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("toString", vec![value]) }
    fn starts_with(value: crate::ast::Expr, prefix: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("startsWith", vec![value, prefix]) }
    fn list_contains(value: crate::ast::Expr, item: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("has", vec![value, item]) }
    fn token_match(value: crate::ast::Expr, token: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("hasToken", vec![value, token]) }
    fn contains(value: crate::ast::Expr, needle: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("positionCaseInsensitive", vec![value, needle]) }
    fn ends_with(value: crate::ast::Expr, suffix: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("endsWith", vec![value, suffix]) }
}

impl LowerBackend for DuckDbLower {
    type Backend = DuckDb;

    fn scan(access: DuckDbAccess, alias: &str) -> Result<TableRef> {
        match access {
            DuckDbAccess::Table(table) => Ok(TableRef::scan(table, alias)),
        }
    }

    fn deduplicate(
        query: Query,
        strategy: DuckDbDedup,
        _keys: &[Expr],
    ) -> Result<Query> {
        match strategy {
            DuckDbDedup::CurrentSnapshot => Ok(query),
        }
    }

    fn fused_neighbors(
        _input: Query,
        _outgoing: &Expr,
        _incoming: &Expr,
        _columns: &[crate::passes::logical_v3::NamedExpr],
        _aliases: &HashMap<RelationId, String>,
    ) -> Result<Query> {
        Err(QueryError::Lowering(
            "DuckDB does not support fused neighbors".into(),
        ))
    }
    fn date_trunc(unit: crate::input::TruncateUnit, value: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("date_trunc", vec![crate::ast::Expr::string(unit.name()), value]) }
    fn array(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("list_value", values) }
    fn tuple(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("row", values) }
    fn json_object(values: Vec<crate::ast::Expr>) -> crate::ast::Expr { crate::ast::Expr::func("json_object", values) }
    fn stringify(value: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("CAST_VARCHAR", vec![value]) }
    fn starts_with(value: crate::ast::Expr, prefix: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("starts_with", vec![value, prefix]) }
    fn list_contains(value: crate::ast::Expr, item: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("list_contains", vec![value, item]) }
    fn token_match(value: crate::ast::Expr, token: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("contains", vec![value, token]) }
    fn contains(value: crate::ast::Expr, needle: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("contains", vec![value, needle]) }
    fn ends_with(value: crate::ast::Expr, suffix: crate::ast::Expr) -> crate::ast::Expr { crate::ast::Expr::func("ends_with", vec![value, suffix]) }
}

fn lower<L: LowerBackend>(
    plan: PhysicalPlan<L::Backend>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    match plan.op {
        PhysicalOp::Scan { alias, access, .. } => Ok(Query {
            from: L::scan(access, &alias)?,
            ..Default::default()
        }),
        PhysicalOp::Filter(predicate) => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            and_where(&mut query, lower_expr::<L>(&predicate, aliases)?);
            Ok(query)
        }
        PhysicalOp::Project(columns) => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            query.select = columns
                .iter()
                .map(|column| {
                    Ok(SelectExpr::new(
                        lower_expr::<L>(&column.expression, aliases)?,
                        &column.name,
                    ))
                })
                .collect::<Result<_>>()?;
            Ok(query)
        }
        PhysicalOp::Join { conditions, .. } => join::<L>(plan.inputs, conditions, aliases),
        PhysicalOp::SemiJoin(condition) => semi_join::<L>(plan.inputs, condition, aliases),
        PhysicalOp::Aggregate { groups, metrics } => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            for group in groups {
                let expression = lower_expr::<L>(&group.expression, aliases)?;
                query
                    .select
                    .push(SelectExpr::new(expression.clone(), group.name));
                query.group_by.push(expression);
            }
            for metric in metrics {
                query.select.push(SelectExpr::new(
                    lower_expr::<L>(&metric.expression, aliases)?,
                    metric.name,
                ));
            }
            Ok(query)
        }
        PhysicalOp::Union => {
            let queries = plan
                .inputs
                .into_iter()
                .map(|input| lower::<L>(input, aliases))
                .collect::<Result<_>>()?;
            Ok(Query {
                from: TableRef::union_all(queries, "union"),
                ..Default::default()
            })
        }
        PhysicalOp::Alias { alias, .. } => {
            let query = only::<L>(plan.inputs, aliases)?;
            Ok(Query {
                from: TableRef::subquery(select_star(query), alias),
                ..Default::default()
            })
        }
        PhysicalOp::Sort(keys) => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            query.order_by = keys
                .iter()
                .map(|key| {
                    let expression = lower_expr::<L>(&key.expression, aliases)?;
                    Ok(if key.descending {
                        OrderExpr::desc(expression)
                    } else {
                        OrderExpr::asc(expression)
                    })
                })
                .collect::<Result<_>>()?;
            Ok(query)
        }
        PhysicalOp::Limit(limit) => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            query.limit = Some(limit);
            Ok(query)
        }
        PhysicalOp::Deduplicate { keys, strategy } => {
            let query = only::<L>(plan.inputs, aliases)?;
            L::deduplicate(query, strategy, &keys)
        }
        PhysicalOp::FusedNeighbors {
            outgoing_predicate,
            incoming_predicate,
            columns,
        } => fused_neighbors::<L>(
            plan.inputs,
            outgoing_predicate,
            incoming_predicate,
            columns,
            aliases,
        ),
        PhysicalOp::ScopeGuard(prefix) => {
            let mut query = only::<L>(plan.inputs, aliases)?;
            and_where(&mut query, prefix.resolved());
            Ok(query)
        }
        PhysicalOp::ReadColumns(columns) => {
            let relation = plan.inputs.first().and_then(PhysicalPlan::relation_id);
            let mut query = only::<L>(plan.inputs, aliases)?;
            if let Some(relation) = relation {
                query.select = columns
                    .into_iter()
                    .map(|name| SelectExpr::col(aliases[&relation].clone(), name))
                    .collect();
            }
            Ok(query)
        }
    }
}

fn fused_neighbors<L: LowerBackend>(
    inputs: Vec<PhysicalPlan<L::Backend>>,
    outgoing: Expr,
    incoming: Expr,
    columns: Vec<crate::passes::logical_v3::NamedExpr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    L::fused_neighbors(
        only::<L>(inputs, aliases)?,
        &outgoing,
        &incoming,
        &columns,
        aliases,
    )
}

fn only<L: LowerBackend>(
    inputs: Vec<PhysicalPlan<L::Backend>>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let [input]: [PhysicalPlan<L::Backend>; 1] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("unary physical operator needs one input".into()))?;
    lower::<L>(input, aliases)
}

fn join<L: LowerBackend>(
    inputs: Vec<PhysicalPlan<L::Backend>>,
    conditions: Vec<Expr>,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let mut inputs = inputs.into_iter();
    let first = inputs
        .next()
        .ok_or_else(|| QueryError::Lowering("join needs an input".into()))?;
    let mut query = lower::<L>(first, aliases)?;
    for input in inputs {
        let right_relations = input.visible_relations();
        let applicable: Vec<_> = conditions
            .iter()
            .filter(|condition| !condition.relations().is_disjoint(&right_relations))
            .map(|condition| lower_expr::<L>(condition, aliases))
            .collect::<Result<_>>()?;
        let condition = applicable
            .into_iter()
            .reduce(crate::ast::Expr::and)
            .unwrap_or_else(|| crate::ast::Expr::lit(1));
        let join_type = if condition == crate::ast::Expr::lit(1) {
            JoinType::Cross
        } else {
            JoinType::Inner
        };
        query.from = TableRef::join(
            join_type,
            query.from,
            TableRef::subquery(select_star(lower::<L>(input, aliases)?), "right"),
            condition,
        );
    }
    Ok(query)
}

fn semi_join<L: LowerBackend>(
    inputs: Vec<PhysicalPlan<L::Backend>>,
    condition: Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<Query> {
    let [left, right]: [PhysicalPlan<L::Backend>; 2] = inputs
        .try_into()
        .map_err(|_| QueryError::Lowering("semi-join needs two inputs".into()))?;
    let Expr::Compare {
        op: CompareOp::Eq,
        left: consumer,
        right: producer,
    } = condition
    else {
        return Err(QueryError::Lowering("semi-join needs equality".into()));
    };
    let mut query = lower::<L>(left, aliases)?;
    let mut lookup = lower::<L>(right, aliases)?;
    lookup.select = vec![SelectExpr {
        expr: lower_expr::<L>(&producer, aliases)?,
        alias: None,
    }];
    and_where(
        &mut query,
        crate::ast::Expr::InSelect {
            expr: Box::new(lower_expr::<L>(&consumer, aliases)?),
            query: Box::new(lookup),
        },
    );
    Ok(query)
}

fn lower_expr<L: LowerBackend>(
    expression: &Expr,
    aliases: &HashMap<RelationId, String>,
) -> Result<crate::ast::Expr> {
    Ok(match expression {
        Expr::Column(column) => crate::ast::Expr::col(&aliases[&column.relation], &column.name),
        Expr::Identifier(identifier) => crate::ast::Expr::ident(identifier),
        Expr::Literal(value) => literal(value),
        Expr::Compare { op, left, right } => crate::ast::Expr::binary(
            compare_op(*op),
            lower_expr::<L>(left, aliases)?,
            lower_expr::<L>(right, aliases)?,
        ),
        Expr::Filter {
            op,
            left,
            right,
            data_type,
        } => lower_filter::<L>(*op, left, right.as_deref(), data_type.as_ref(), aliases)?,
        Expr::And(expressions) => expressions
            .iter()
            .map(|expression| lower_expr::<L>(expression, aliases))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(crate::ast::Expr::and)
            .unwrap_or_else(|| crate::ast::Expr::lit(1)),
        Expr::Or(expressions) => expressions
            .iter()
            .map(|expression| lower_expr::<L>(expression, aliases))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| crate::ast::Expr::binary(crate::ast::Op::Or, left, right))
            .unwrap_or_else(|| crate::ast::Expr::lit(0)),
        Expr::In {
            value,
            values,
            data_type,
        } => crate::ast::Expr::binary(
            crate::ast::Op::In,
            lower_expr::<L>(value, aliases)?,
            crate::ast::Expr::param(
                data_type_to_ch(data_type.as_ref()).to_array(),
                serde_json::Value::Array(values.iter().map(json_value).collect()),
            ),
        ),
        Expr::DateTrunc { unit, value } => {
            L::date_trunc(*unit, lower_expr::<L>(value, aliases)?)
        }
        Expr::Aggregate { function, value } => crate::ast::Expr::func(
            function.as_sql(),
            value
                .iter()
                .map(|value| lower_expr::<L>(value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::Array(values) => L::array(
            values
                .iter()
                .map(|value| lower_expr::<L>(value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::Tuple(values) => L::tuple(
            values
                .iter()
                .map(|value| lower_expr::<L>(value, aliases))
                .collect::<Result<_>>()?,
        ),
        Expr::JsonObject(entries) => {
            let mut arguments = Vec::new();
            for (key, value) in entries {
                arguments.push(crate::ast::Expr::string(key));
                arguments.push(lower_expr::<L>(value, aliases)?);
            }
            L::json_object(arguments)
        }
        Expr::Stringify(value) => L::stringify(lower_expr::<L>(value, aliases)?),
        Expr::PathPrefixAny { value, prefixes } => prefixes
            .iter()
            .map(|prefix| {
                Ok(L::starts_with(
                    lower_expr::<L>(value, aliases)?,
                    crate::ast::Expr::string(prefix),
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .reduce(|left, right| crate::ast::Expr::binary(crate::ast::Op::Or, left, right))
            .unwrap_or_else(|| crate::ast::Expr::lit(1)),
        Expr::ListContains { list, value } => {
            L::list_contains(lower_expr::<L>(list, aliases)?, literal(value))
        }
        Expr::TokenMatch {
            value,
            token,
            tokenizer,
        } => {
            let _ = tokenizer;
            L::token_match(lower_expr::<L>(value, aliases)?, literal(token))
        }
    })
}

fn lower_filter<L: LowerBackend>(
    operator: crate::input::FilterOp,
    left: &Expr,
    right: Option<&Expr>,
    data_type: Option<&ontology::DataType>,
    aliases: &HashMap<RelationId, String>,
) -> Result<crate::ast::Expr> {
    let left = lower_expr::<L>(left, aliases)?;
    let right = right.map_or_else(
        || crate::ast::Expr::param(data_type_to_ch(data_type), serde_json::Value::Null),
        |right| lower_expr::<L>(right, aliases).unwrap(),
    );
    Ok(match operator {
        crate::input::FilterOp::IsNull => crate::ast::Expr::unary(crate::ast::Op::IsNull, left),
        crate::input::FilterOp::IsNotNull => {
            crate::ast::Expr::unary(crate::ast::Op::IsNotNull, left)
        }
        crate::input::FilterOp::Contains => L::contains(left, right),
        crate::input::FilterOp::StartsWith => L::starts_with(left, right),
        crate::input::FilterOp::EndsWith => L::ends_with(left, right),
        operator => crate::ast::Expr::binary(filter_op(operator), left, right),
    })
}

fn compare_op(operator: CompareOp) -> crate::ast::Op {
    match operator {
        CompareOp::Eq => crate::ast::Op::Eq,
        CompareOp::Ne => crate::ast::Op::Ne,
        CompareOp::Lt => crate::ast::Op::Lt,
        CompareOp::Le => crate::ast::Op::Le,
        CompareOp::Gt => crate::ast::Op::Gt,
        CompareOp::Ge => crate::ast::Op::Ge,
    }
}

fn filter_op(operator: crate::input::FilterOp) -> crate::ast::Op {
    match operator {
        crate::input::FilterOp::Eq => crate::ast::Op::Eq,
        crate::input::FilterOp::Ne => crate::ast::Op::Ne,
        crate::input::FilterOp::Lt => crate::ast::Op::Lt,
        crate::input::FilterOp::Lte => crate::ast::Op::Le,
        crate::input::FilterOp::Gt => crate::ast::Op::Gt,
        crate::input::FilterOp::Gte => crate::ast::Op::Ge,
        _ => crate::ast::Op::Eq,
    }
}

fn literal(value: &Value) -> crate::ast::Expr {
    match value {
        Value::Int(value) => crate::ast::Expr::int(*value),
        Value::Float(value) => crate::ast::Expr::param(
            crate::ast::ChType::Float64,
            value.parse::<f64>().unwrap_or_default(),
        ),
        Value::String(value) => crate::ast::Expr::string(value),
        Value::Bool(value) => crate::ast::Expr::param(crate::ast::ChType::Bool, *value),
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

fn edge_tables(tables: Vec<String>, alias: &str) -> TableRef {
    match tables.as_slice() {
        [table] => TableRef::scan(table, alias),
        _ => TableRef::union_all(
            tables
                .into_iter()
                .map(|table| Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(table, alias),
                    ..Default::default()
                })
                .collect(),
            alias,
        ),
    }
}

fn select_star(mut query: Query) -> Query {
    if query.select.is_empty() {
        query.select.push(SelectExpr::star());
    }
    query
}

fn alias(table: &TableRef) -> &str {
    match table {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias,
        TableRef::Join { .. } => "relation",
    }
}

fn set_final(table: &mut TableRef) {
    match table {
        TableRef::Scan { final_, .. } => *final_ = true,
        TableRef::Subquery { query, .. } => set_final(&mut query.from),
        TableRef::Union { queries, .. } => {
            queries.iter_mut().for_each(|query| set_final(&mut query.from));
        }
        TableRef::Join { left, right, .. } => {
            set_final(left);
            set_final(right);
        }
    }
}

fn and_where(query: &mut Query, predicate: crate::ast::Expr) {
    query.where_clause = Some(match query.where_clause.take() {
        Some(existing) => crate::ast::Expr::and(existing, predicate),
        None => predicate,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::logical_v3::{column, Plan};
    use crate::passes::physical_v3::{DuckDbAccess, JoinStrategy};

    #[test]
    fn lowers_physical_join_to_existing_ast() {
        let scan = |alias: &str| {
            Plan::leaf(PhysicalOp::Scan {
                relation: RelationId(if alias == "a" { 1 } else { 2 }),
                alias: alias.into(),
                access: DuckDbAccess::Table(alias.into()),
            })
        };
        let plan = Plan::nary(
            PhysicalOp::Join {
                conditions: vec![column(RelationId(1), "id").eq(column(RelationId(2), "id"))],
                strategy: JoinStrategy::Default,
            },
            [scan("a"), scan("b")],
        );
        let Node::Query(query) = duckdb(plan).unwrap() else {
            unreachable!()
        };
        assert!(matches!(query.from, TableRef::Join { .. }));
    }
}
