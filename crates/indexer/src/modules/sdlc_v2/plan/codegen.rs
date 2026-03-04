/// SQL emitter that walks the AST and produces a SQL string.
///
/// No parameterization — ETL uses named ClickHouse params via `Expr::Raw`,
/// and transforms have no external parameters.
use super::ast::{Expr, OrderExpr, Query, SelectExpr, TableRef};

pub fn emit_sql(query: &Query) -> String {
    let mut sql = String::with_capacity(256);

    sql.push_str("SELECT ");
    emit_select_list(&mut sql, &query.select);

    sql.push_str(" FROM ");
    emit_table_ref(&mut sql, &query.from);

    if let Some(where_clause) = &query.where_clause {
        sql.push_str(" WHERE ");
        emit_expr(&mut sql, where_clause);
    }

    if !query.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        emit_order_by(&mut sql, &query.order_by);
    }

    if let Some(limit) = query.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    sql
}

fn emit_select_list(sql: &mut String, select: &[SelectExpr]) {
    for (index, select_expr) in select.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        emit_expr(sql, &select_expr.expr);
        if let Some(alias) = &select_expr.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }
    }
}

fn emit_table_ref(sql: &mut String, table_ref: &TableRef) {
    match table_ref {
        TableRef::Scan { table, alias } => {
            sql.push_str(table);
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
        }
        TableRef::Raw(raw) => sql.push_str(raw),
    }
}

fn emit_expr(sql: &mut String, expr: &Expr) {
    match expr {
        Expr::Raw(raw) => sql.push_str(raw),
        Expr::Column { table, column } => {
            if !table.is_empty() {
                sql.push_str(table);
                sql.push('.');
            }
            sql.push_str(column);
        }
        Expr::BinaryOp { op, left, right } => {
            sql.push('(');
            emit_expr(sql, left);
            sql.push(' ');
            sql.push_str(op.as_sql());
            sql.push(' ');
            emit_expr(sql, right);
            sql.push(')');
        }
        Expr::IsNotNull(inner) => {
            sql.push('(');
            emit_expr(sql, inner);
            sql.push_str(" IS NOT NULL)");
        }
        Expr::FuncCall { name, args } => {
            sql.push_str(name);
            sql.push('(');
            for (index, arg) in args.iter().enumerate() {
                if index > 0 {
                    sql.push_str(", ");
                }
                emit_expr(sql, arg);
            }
            sql.push(')');
        }
        Expr::Cast { expr, data_type } => {
            sql.push_str("CAST(");
            emit_expr(sql, expr);
            sql.push_str(" AS ");
            sql.push_str(data_type);
            sql.push(')');
        }
    }
}

fn emit_order_by(sql: &mut String, order_by: &[OrderExpr]) {
    for (index, order_expr) in order_by.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        emit_expr(sql, &order_expr.expr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc_v2::plan::ast::Op;

    #[test]
    fn simple_select_from() {
        let query = Query {
            select: vec![
                SelectExpr::bare(Expr::col("", "id")),
                SelectExpr::bare(Expr::col("", "name")),
            ],
            from: TableRef::scan("users", None),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(emit_sql(&query), "SELECT id, name FROM users");
    }

    #[test]
    fn select_with_aliases() {
        let query = Query {
            select: vec![
                SelectExpr::new(Expr::col("", "admin"), "is_admin"),
                SelectExpr::new(Expr::raw("'Group'"), "source_kind"),
            ],
            from: TableRef::scan("source_data", None),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(
            emit_sql(&query),
            "SELECT admin AS is_admin, 'Group' AS source_kind FROM source_data"
        );
    }

    #[test]
    fn select_with_where_and_order_and_limit() {
        let query = Query {
            select: vec![SelectExpr::bare(Expr::col("t", "id"))],
            from: TableRef::scan("users", Some("t".to_string())),
            where_clause: Some(Expr::binary(
                Op::Gt,
                Expr::col("t", "id"),
                Expr::raw("0"),
            )),
            order_by: vec![OrderExpr {
                expr: Expr::col("t", "id"),
            }],
            limit: Some(100),
        };

        assert_eq!(
            emit_sql(&query),
            "SELECT t.id FROM users AS t WHERE (t.id > 0) ORDER BY t.id LIMIT 100"
        );
    }

    #[test]
    fn raw_table_ref() {
        let query = Query {
            select: vec![SelectExpr::bare(Expr::col("", "id"))],
            from: TableRef::Raw(
                "siphon_projects p INNER JOIN traversal_paths tp ON p.id = tp.id".to_string(),
            ),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(
            emit_sql(&query),
            "SELECT id FROM siphon_projects p INNER JOIN traversal_paths tp ON p.id = tp.id"
        );
    }

    #[test]
    fn is_not_null() {
        let query = Query {
            select: vec![SelectExpr::bare(Expr::col("", "id"))],
            from: TableRef::scan("t", None),
            where_clause: Some(Expr::is_not_null(Expr::col("", "fk"))),
            order_by: vec![],
            limit: None,
        };

        assert_eq!(emit_sql(&query), "SELECT id FROM t WHERE (fk IS NOT NULL)");
    }

    #[test]
    fn and_all_combines_conditions() {
        let where_clause = Expr::and_all([
            Some(Expr::is_not_null(Expr::col("", "fk"))),
            Some(Expr::raw("type IN ('A', 'B')")),
        ]);

        let query = Query {
            select: vec![SelectExpr::bare(Expr::col("", "id"))],
            from: TableRef::scan("t", None),
            where_clause,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(
            emit_sql(&query),
            "SELECT id FROM t WHERE ((fk IS NOT NULL) AND type IN ('A', 'B'))"
        );
    }

    #[test]
    fn func_call() {
        let query = Query {
            select: vec![SelectExpr::new(
                Expr::func("toString", vec![Expr::col("", "uuid")]),
                "uuid",
            )],
            from: TableRef::scan("t", None),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(emit_sql(&query), "SELECT toString(uuid) AS uuid FROM t");
    }

    #[test]
    fn cast_expression() {
        let expr = Expr::cast(
            Expr::func("NULLIF", vec![
                Expr::func("unnest", vec![
                    Expr::func("string_to_array", vec![
                        Expr::col("", "ids"),
                        Expr::raw("'/'"),
                    ]),
                ]),
                Expr::raw("''"),
            ]),
            "BIGINT",
        );

        let query = Query {
            select: vec![SelectExpr::new(expr, "exploded_id")],
            from: TableRef::scan("t", None),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        assert_eq!(
            emit_sql(&query),
            "SELECT CAST(NULLIF(unnest(string_to_array(ids, '/')), '') AS BIGINT) AS exploded_id FROM t"
        );
    }
}
