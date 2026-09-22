use crate::ast::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan_v2::*;
use crate::passes::shared::filter_to_expr;

pub fn lower(op: PhysOp, input: &Input) -> Result<Node> {
    let mut q = emit(op, input);
    for jp in &input.join_predicates {
        let filter = InputFilter {
            op: Some(jp.op),
            rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
            ..Default::default()
        };
        let pred = filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter);
        q.where_clause = Some(match q.where_clause.take() {
            Some(existing) => Expr::and(existing, pred),
            None => pred,
        });
    }
    Ok(Node::Query(Box::new(q)))
}

fn emit(op: PhysOp, input: &Input) -> Query {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } => Query {
            from: if dedup {
                TableRef::scan_final(&table, &alias)
            } else {
                TableRef::scan(&table, &alias)
            },
            ..Default::default()
        },

        PhysOp::Filter {
            input: child,
            predicates,
        } => {
            let mut q = emit(*child, input);
            let alias = extract_alias(&q.from);
            let exprs: Vec<Expr> = predicates
                .into_iter()
                .map(|p| emit_predicate(&alias, &p))
                .collect();
            for e in exprs {
                q.where_clause = Some(match q.where_clause.take() {
                    Some(existing) => Expr::and(existing, e),
                    None => e,
                });
            }
            q
        }

        PhysOp::Project {
            input: child,
            columns,
        } => {
            let mut q = emit(*child, input);
            let alias = extract_alias(&q.from);
            q.select = columns
                .into_iter()
                .map(|c| emit_column(&alias, c, input))
                .collect();
            q
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Inner,
        } => {
            let lq = emit(*left, input);
            let rq = emit(*right, input);
            let join_expr = Expr::eq(
                Expr::col(&on.left.0, &on.left.1),
                Expr::col(&on.right.0, &on.right.1),
            );
            let rhs = subquery_wrap(rq);
            Query {
                from: TableRef::join(JoinType::Inner, lq.from, rhs, join_expr),
                where_clause: lq.where_clause,
                ctes: lq.ctes,
                ..Default::default()
            }
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Semi { materialize: true },
        } => {
            let mut consumer_q = emit(*left, input);
            let body_q = emit(*right, input);
            let cte_name = on.left.0.clone();
            if !cte_name.is_empty() {
                consumer_q.ctes.insert(0, Cte::new(&cte_name, body_q));
            }
            consumer_q
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Semi { materialize: false },
        } => {
            let mut lq = emit(*left, input);
            let rq = emit(*right, input);
            let in_expr = Expr::InSelect {
                expr: Box::new(Expr::col(&on.left.0, &on.left.1)),
                query: Box::new(rq),
            };
            lq.where_clause = Some(match lq.where_clause.take() {
                Some(existing) => Expr::and(existing, in_expr),
                None => in_expr,
            });
            lq
        }

        PhysOp::Aggregate {
            input: child,
            group_by,
            metrics,
        } => {
            let mut q = emit(*child, input);
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
                    (AggFunction::Count, Some(p)) => {
                        Expr::func("COUNT", vec![Expr::col(&m.node, p)])
                    }
                    (AggFunction::Count, None) => Expr::func("COUNT", vec![]),
                    (f, Some(p)) => Expr::func(f.as_sql(), vec![Expr::col(&m.node, p)]),
                    (_, None) => Expr::func("COUNT", vec![]),
                };
                q.select.push(SelectExpr::new(expr, &m.alias));
            }
            q
        }

        PhysOp::Union { arms } => {
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
            let alias = "_union";
            Query {
                from: TableRef::union_all(queries, alias),
                ..Default::default()
            }
        }

        PhysOp::Sort { input: child, keys } => {
            let mut q = emit(*child, input);
            for sk in &keys {
                let parts: Vec<&str> = sk.column.splitn(2, '.').collect();
                let expr = if parts.len() == 2 {
                    Expr::col(parts[0], parts[1])
                } else {
                    Expr::ident(&sk.column)
                };
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
            q.limit = Some(count);
            q
        }
    }
}

fn emit_predicate(alias: &str, pred: &Predicate) -> Expr {
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
    }
}

fn emit_value(v: &Value) -> Expr {
    match v {
        Value::Int(i) => Expr::int(*i),
        Value::Str(s) => Expr::string(s),
        Value::Bool(b) => Expr::param(ChType::Bool, *b),
        Value::Strs(_) => Expr::string(""),
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
        Some(Value::Str(_)) => ChType::String,
        Some(Value::Bool(_)) => ChType::Bool,
        _ => ChType::String,
    }
}

fn emit_column(alias: &str, col: ProjectedColumn, input: &Input) -> SelectExpr {
    match col {
        ProjectedColumn::Ref { column, alias: a } => SelectExpr::new(Expr::col(alias, &column), a),
        ProjectedColumn::NodeProperty { property } => {
            let node = input.nodes.iter().find(|n| n.id == alias);
            let needs_excerpt = node.is_some_and(|n| n.excerpt_columns.contains(&property));
            let max_chars = node.map(|n| n.excerpt_max_chars).unwrap_or(0);
            let value = Expr::col(alias, &property);
            let expr = if needs_excerpt && max_chars > 0 {
                let excerpt = Expr::func(
                    "substringUTF8",
                    vec![value.clone(), Expr::lit(1), Expr::lit(max_chars)],
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
            } else {
                value
            };
            SelectExpr::new(expr, format!("{alias}_{property}"))
        }
        ProjectedColumn::Computed { expr, alias: a } => SelectExpr::new(emit_column_expr(&expr), a),
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

fn extract_alias(tr: &TableRef) -> String {
    match tr {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias.clone(),
        TableRef::Join { .. } => "_j".to_string(),
    }
}

fn subquery_wrap(q: Query) -> TableRef {
    let alias = extract_alias(&q.from);
    let mut wrapped = q;
    if wrapped.select.is_empty() {
        wrapped.select.push(SelectExpr::star());
    }
    TableRef::subquery(wrapped, &alias)
}
