//! `PhysOp` → SQL AST. One `Query` block accumulates operators until an
//! operator would conflict with what the block already holds (a second
//! projection, a filter after a projection or dedup, a sort after a limit);
//! then the block is closed as a derived table and a fresh one continues.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan_v2::*;
use crate::passes::shared::filter_to_expr;
use ontology::constants::*;

pub fn lower(op: PhysOp, input: &Input) -> Result<Node> {
    let mut q = emit(op, input);
    if q.select.is_empty() {
        q.select = default_select(&q.from);
    }
    for jp in &input.join_predicates {
        let filter = InputFilter {
            op: Some(jp.op),
            rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
            ..Default::default()
        };
        and_where(&mut q, filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter));
    }
    Ok(Node::Query(Box::new(q)))
}

/// A union's output columns are named after its first arm's aliases; project
/// them explicitly so later passes can see which aliases the query returns.
fn default_select(from: &TableRef) -> Vec<SelectExpr> {
    if let TableRef::Union { queries, alias } = from
        && let Some(first) = queries.first()
        && first.select.iter().all(|s| s.alias.is_some())
    {
        return first
            .select
            .iter()
            .map(|s| SelectExpr::col(alias.as_str(), s.alias.as_deref().unwrap()))
            .collect();
    }
    vec![SelectExpr::star()]
}

fn and_where(q: &mut Query, pred: Expr) {
    q.where_clause = Some(match q.where_clause.take() {
        Some(existing) => Expr::and(existing, pred),
        None => pred,
    });
}

/// The block already projects, groups, sorts, or limits: further filters or
/// projections must apply to its result.
fn is_closed(q: &Query) -> bool {
    !q.select.is_empty()
        || !q.group_by.is_empty()
        || q.limit.is_some()
        || (!q.order_by.is_empty() && q.limit_by.is_none())
}

fn alias_of(q: &Query) -> Option<&str> {
    match &q.from {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => Some(alias),
        TableRef::Join { .. } => None,
    }
}

fn close(q: Query) -> Query {
    let alias = alias_of(&q).unwrap_or("_q").to_string();
    Query {
        from: as_table(q, &alias),
        ..Default::default()
    }
}

fn as_table(mut q: Query, alias: &str) -> TableRef {
    if q.select.is_empty() {
        q.select.push(SelectExpr::star());
    }
    TableRef::subquery(q, alias)
}

/// A plain `FROM table AS alias` block with nothing else set can be used as a
/// join operand directly; anything richer becomes a derived table.
fn join_operand(q: Query) -> (TableRef, Vec<Cte>) {
    let bare = q.select.is_empty()
        && q.where_clause.is_none()
        && q.group_by.is_empty()
        && q.order_by.is_empty()
        && q.limit_by.is_none()
        && q.limit.is_none()
        && matches!(q.from, TableRef::Scan { .. });
    if bare {
        (q.from, q.ctes)
    } else {
        let alias = alias_of(&q).unwrap_or("_q").to_string();
        let mut q = q;
        let ctes = std::mem::take(&mut q.ctes);
        (as_table(q, &alias), ctes)
    }
}

fn emit(op: PhysOp, input: &Input) -> Query {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } => match dedup {
            Dedup::None => Query {
                from: TableRef::scan(&table, &alias),
                ..Default::default()
            },
            Dedup::Final => Query {
                from: TableRef::scan_final(&table, &alias),
                ..Default::default()
            },
            Dedup::LimitBy => {
                let sort_key = input
                    .compiler
                    .table_sort_keys
                    .get(&table)
                    .cloned()
                    .unwrap_or_else(|| vec![DEFAULT_PRIMARY_KEY.to_string()]);
                let mut order_by: Vec<OrderExpr> = sort_key
                    .iter()
                    .map(|c| OrderExpr::asc(Expr::col(&alias, c)))
                    .collect();
                order_by.push(OrderExpr::desc(Expr::col(&alias, VERSION_COLUMN)));
                Query {
                    from: TableRef::scan(&table, &alias),
                    order_by,
                    limit_by: Some((1, sort_key.iter().map(|c| Expr::col(&alias, c)).collect())),
                    ..Default::default()
                }
            }
        },

        PhysOp::Filter {
            input: child,
            predicates,
        } => {
            let mut q = emit(*child, input);
            if is_closed(&q) {
                q = close(q);
            }
            let alias = alias_of(&q).map(str::to_string);
            for p in predicates {
                and_where(&mut q, emit_predicate(alias.as_deref(), &p));
            }
            q
        }

        PhysOp::Project {
            input: child,
            columns,
        } => {
            let mut q = emit(*child, input);
            if !q.select.is_empty() || !q.group_by.is_empty() || q.limit.is_some() {
                q = close(q);
            }
            let alias = alias_of(&q).map(str::to_string);
            q.select = columns
                .into_iter()
                .map(|c| emit_column(alias.as_deref(), c, input))
                .collect();
            q
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Inner,
        } => {
            let (lhs, mut ctes) = join_operand_keep_where(emit(*left, input));
            let (rhs, rctes) = join_operand(emit(*right, input));
            ctes.extend(rctes);
            let (from, where_clause) = lhs;
            let cond = on
                .iter()
                .map(|(x, y)| Expr::eq(Expr::col(&x.0, &x.1), Expr::col(&y.0, &y.1)))
                .reduce(Expr::and);
            let join_type = if cond.is_some() {
                JoinType::Inner
            } else {
                JoinType::Cross
            };
            Query {
                from: TableRef::join(join_type, from, rhs, cond.unwrap_or_else(|| Expr::lit(1))),
                where_clause,
                ctes,
                ..Default::default()
            }
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Semi,
        } => {
            let mut lq = emit(*left, input);
            if is_closed(&lq) {
                lq = close(lq);
            }
            let (consumer, producer) = on.into_iter().next().expect("semi join equality");
            let rq = emit(*right, input);
            let bare_cte_ref = rq.select.is_empty()
                && rq.where_clause.is_none()
                && matches!(rq.from, TableRef::Scan { .. });
            let pred = if let (true, TableRef::Scan { table, .. }) = (bare_cte_ref, &rq.from) {
                Expr::InSubquery {
                    expr: Box::new(Expr::col(&consumer.0, &consumer.1)),
                    cte_name: table.clone(),
                    column: producer.1,
                }
            } else {
                let mut body = rq;
                body.select = vec![SelectExpr::new(
                    Expr::col(&producer.0, &producer.1),
                    producer.1.clone(),
                )];
                Expr::InSelect {
                    expr: Box::new(Expr::col(&consumer.0, &consumer.1)),
                    query: Box::new(body),
                }
            };
            and_where(&mut lq, pred);
            lq
        }

        PhysOp::Aggregate {
            input: child,
            group_by,
            metrics,
        } => {
            let mut q = emit(*child, input);
            if is_closed(&q) {
                q = close(q);
            }
            for gk in &group_by {
                let col = Expr::col(&gk.node, &gk.property);
                let expr = match gk.truncate {
                    Some(unit) => {
                        let tr = Expr::func(unit.ch_function(), vec![col]);
                        match unit {
                            TruncateUnit::Minute | TruncateUnit::Hour => {
                                Expr::func("toDateTime64", vec![tr, Expr::ident("0")])
                            }
                            _ => Expr::func("toDate32", vec![tr]),
                        }
                    }
                    None => col,
                };
                q.select.push(SelectExpr::new(expr.clone(), &gk.alias));
                if !q.group_by.contains(&expr) {
                    q.group_by.push(expr);
                }
            }
            for m in &metrics {
                let expr = match (&m.function, m.property.as_deref()) {
                    (AggFunction::Count, None) | (_, None) => Expr::func("COUNT", vec![]),
                    (AggFunction::Count, Some(p)) => {
                        Expr::func("COUNT", vec![Expr::col(&m.node, p)])
                    }
                    (f, Some(p)) => Expr::func(f.as_sql(), vec![Expr::col(&m.node, p)]),
                };
                q.select.push(SelectExpr::new(expr, &m.alias));
            }
            q
        }

        PhysOp::Union { arms, alias } => {
            let queries: Vec<Query> = arms
                .into_iter()
                .map(|a| {
                    let mut q = emit(a, input);
                    if q.select.is_empty() {
                        q.select.push(SelectExpr::star());
                    }
                    q
                })
                .collect();
            Query {
                from: TableRef::union_all(queries, alias),
                ..Default::default()
            }
        }

        PhysOp::Sort { input: child, keys } => {
            let mut q = emit(*child, input);
            if q.limit.is_some() || q.limit_by.is_some() {
                q = close(q);
            }
            for sk in &keys {
                let expr = sort_expr(&sk.column);
                q.order_by.push(if sk.desc {
                    OrderExpr::desc(expr)
                } else {
                    OrderExpr::asc(expr)
                });
            }
            q
        }

        PhysOp::Limit {
            input: child,
            count,
        } => {
            let mut q = emit(*child, input);
            if q.limit.is_some() {
                q = close(q);
            }
            q.limit = Some(count);
            q
        }

        PhysOp::With { ctes, input: child } => {
            let mut q = emit(*child, input);
            let mut all: Vec<Cte> = ctes
                .into_iter()
                .map(|(name, body)| {
                    let mut bq = emit(body, input);
                    if bq.select.is_empty() {
                        bq.select.push(SelectExpr::star());
                    }
                    Cte::new(name, bq)
                })
                .collect();
            all.append(&mut q.ctes);
            q.ctes = all;
            q
        }
    }
}

/// Left join operand: a block with only a `FROM` and `WHERE` keeps its
/// `WHERE` on the enclosing query (the join is still one SQL block).
fn join_operand_keep_where(q: Query) -> ((TableRef, Option<Expr>), Vec<Cte>) {
    let simple = q.select.is_empty()
        && q.group_by.is_empty()
        && q.order_by.is_empty()
        && q.limit_by.is_none()
        && q.limit.is_none();
    if simple {
        ((q.from, q.where_clause), q.ctes)
    } else {
        let alias = alias_of(&q).unwrap_or("_q").to_string();
        let mut q = q;
        let ctes = std::mem::take(&mut q.ctes);
        ((as_table(q, &alias), None), ctes)
    }
}

/// `a.b`, `b`, or `func(a.b)`.
fn sort_expr(column: &str) -> Expr {
    if let Some((name, rest)) = column.split_once('(')
        && let Some(inner) = rest.strip_suffix(')')
    {
        return Expr::func(name, vec![sort_expr(inner)]);
    }
    match column.split_once('.') {
        Some((a, b)) => Expr::col(a, b),
        None => Expr::ident(column),
    }
}

fn emit_predicate(alias: Option<&str>, pred: &Predicate) -> Expr {
    if let Predicate::Expr(e) = pred {
        return e.clone();
    }
    let alias = alias.expect("alias-relative predicate over an aliased relation");
    match pred {
        Predicate::Eq { column, value } => Expr::eq(Expr::col(alias, column), emit_value(value)),
        Predicate::In { column, values } => Expr::col_in(
            alias,
            column,
            value_ch_type(values),
            values.iter().map(value_to_json).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false)),
        Predicate::Range { column, start, end } => Expr::and(
            Expr::binary(Op::Ge, Expr::col(alias, column), Expr::int(*start)),
            Expr::binary(Op::Le, Expr::col(alias, column), Expr::int(*end)),
        ),
        Predicate::NodeFilter { property, filter } => filter_to_expr(alias, property, filter),
        Predicate::Func {
            name,
            column,
            value,
        } => match value {
            Value::Strs(strs) => Expr::func(
                name,
                vec![
                    Expr::col(alias, column),
                    Expr::func("array", strs.iter().map(Expr::string).collect()),
                ],
            ),
            _ => Expr::func(name, vec![Expr::col(alias, column), emit_value(value)]),
        },
        Predicate::ScopePrefix(sp) => sp.predicate(alias),
        Predicate::Expr(_) => unreachable!(),
    }
}

fn emit_value(v: &Value) -> Expr {
    match v {
        Value::Int(i) => Expr::int(*i),
        Value::Str(s) => Expr::string(s),
        Value::Bool(b) => Expr::param(ChType::Bool, *b),
        Value::Strs(ss) => Expr::func("array", ss.iter().map(Expr::string).collect()),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Strs(ss) => serde_json::Value::Array(
            ss.iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
    }
}

fn value_ch_type(values: &[Value]) -> ChType {
    match values.first() {
        Some(Value::Int(_)) => ChType::Int64,
        Some(Value::Bool(_)) => ChType::Bool,
        _ => ChType::String,
    }
}

fn emit_column(alias: Option<&str>, col: ProjectedColumn, input: &Input) -> SelectExpr {
    match col {
        ProjectedColumn::Ref {
            table,
            column,
            alias: a,
        } => {
            let tbl = if table.is_empty() {
                alias.expect("unqualified column over an aliased relation")
            } else {
                table.as_str()
            };
            SelectExpr::new(Expr::col(tbl, &column), a)
        }
        ProjectedColumn::NodeProperty { node, property } => {
            let n = input.nodes.iter().find(|n| n.id == node);
            let value = Expr::col(&node, &property);
            let expr = match n {
                Some(n) if n.excerpt_columns.contains(&property) && n.excerpt_max_chars > 0 => {
                    let excerpt = Expr::func(
                        "substringUTF8",
                        vec![value.clone(), Expr::lit(1), Expr::lit(n.excerpt_max_chars)],
                    );
                    let shortened = Expr::binary(
                        Op::Gt,
                        Expr::func("length", vec![value]),
                        Expr::func("length", vec![excerpt.clone()]),
                    );
                    Expr::func(
                        "concat",
                        vec![
                            excerpt,
                            Expr::func(
                                "if",
                                vec![shortened, Expr::string(" [truncated]"), Expr::string("")],
                            ),
                        ],
                    )
                }
                _ => value,
            };
            SelectExpr::new(expr, format!("{node}_{property}"))
        }
        ProjectedColumn::Computed { expr, alias: a } => SelectExpr::new(emit_column_expr(&expr), a),
        ProjectedColumn::Expr { expr, alias: a } => SelectExpr::new(expr, a),
    }
}

fn emit_column_expr(ce: &ColumnExpr) -> Expr {
    match ce {
        ColumnExpr::Col(table, col) => Expr::col(table, col),
        ColumnExpr::Lit(v) => emit_value(v),
        ColumnExpr::Array(items) => {
            Expr::func("array", items.iter().map(emit_column_expr).collect())
        }
        ColumnExpr::Tuple(items) => {
            Expr::func("tuple", items.iter().map(emit_column_expr).collect())
        }
    }
}
