//! Codegen: AST → SQL
//!
//! Converts AST nodes to parameterized ClickHouse SQL.

use crate::ast::{Expr, Node, Op, Query, RecursiveCte, TableRef};
use crate::error::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;

/// Parameterized SQL query with bound parameters.
#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, Value>,
}

/// Convert an AST node to parameterized SQL.
#[must_use = "the generated SQL should be used"]
pub fn codegen(ast: &Node) -> Result<ParameterizedQuery> {
    let mut ctx = Context::new();
    let sql = match ast {
        Node::Query(q) => ctx.emit_query(q)?,
        Node::RecursiveCte(cte) => ctx.emit_cte(cte)?,
    };
    Ok(ParameterizedQuery {
        sql,
        params: ctx.params,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Code generation context
// ─────────────────────────────────────────────────────────────────────────────

struct Context {
    params: HashMap<String, Value>,
}

impl Context {
    fn new() -> Self {
        Self {
            params: HashMap::new(),
        }
    }

    fn emit_query(&mut self, q: &Query) -> Result<String> {
        let mut sql = String::new();

        // SELECT
        sql.push_str("SELECT ");
        for (i, sel) in q.select.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.emit_expr(&sel.expr)?);
            if let Some(alias) = &sel.alias {
                write!(sql, " AS {alias}").unwrap();
            }
        }

        // FROM
        let from = self.emit_table_ref(&q.from)?;
        write!(sql, " FROM {}", from.sql).unwrap();

        // WHERE
        let mut where_parts = from.type_conditions;
        if let Some(w) = &q.where_clause {
            where_parts.push(self.emit_expr(w)?);
        }
        if !where_parts.is_empty() {
            write!(sql, " WHERE {}", where_parts.join(" AND ")).unwrap();
        }

        // GROUP BY
        if !q.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            for (i, g) in q.group_by.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&self.emit_expr(g)?);
            }
        }

        // ORDER BY
        if !q.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            for (i, o) in q.order_by.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&self.emit_expr(&o.expr)?);
                sql.push_str(if o.desc { " DESC" } else { " ASC" });
            }
        }

        // LIMIT
        if let Some(limit) = q.limit {
            write!(sql, " LIMIT {limit}").unwrap();
        }

        Ok(sql)
    }

    fn emit_cte(&mut self, cte: &RecursiveCte) -> Result<String> {
        let mut sql = format!("WITH RECURSIVE {} AS (\n  ", cte.name);
        sql.push_str(&self.emit_query(&cte.base)?);
        sql.push_str("\n  UNION ALL\n  ");
        sql.push_str(&self.emit_query(&cte.recursive)?);
        sql.push_str("\n)\n");
        sql.push_str(&self.emit_query(&cte.final_query)?);
        Ok(sql)
    }

    fn emit_expr(&mut self, e: &Expr) -> Result<String> {
        Ok(match e {
            Expr::Column { table, column } => format!("{table}.{column}"),
            Expr::Literal(v) => self.emit_literal(v)?,
            Expr::FuncCall { name, args } => {
                let args: Vec<_> = args
                    .iter()
                    .map(|a| self.emit_expr(a))
                    .collect::<Result<_>>()?;
                format!("{}({})", name, args.join(", "))
            }
            Expr::BinaryOp { op, left, right } => {
                let l = self.emit_expr(left)?;
                let r = self.emit_expr(right)?;
                if *op == Op::In {
                    format!("{l} IN {r}")
                } else {
                    format!("({l} {} {r})", op.as_sql())
                }
            }
            Expr::UnaryOp { op, expr } => {
                let e = self.emit_expr(expr)?;
                if *op == Op::IsNull || *op == Op::IsNotNull {
                    format!("({e} {})", op.as_sql())
                } else {
                    format!("({} {e})", op.as_sql())
                }
            }
        })
    }

    fn emit_literal(&mut self, v: &Value) -> Result<String> {
        match v {
            Value::Null => Ok("NULL".into()),
            Value::Array(arr) => {
                let placeholders: Vec<_> = arr
                    .iter()
                    .map(|item| {
                        let name = format!("p{}", self.params.len());
                        self.params.insert(name.clone(), item.clone());
                        format!("{{{name}:{}}}", ch_type(item))
                    })
                    .collect();
                Ok(format!("({})", placeholders.join(", ")))
            }
            _ => {
                let name = format!("p{}", self.params.len());
                self.params.insert(name.clone(), v.clone());
                Ok(format!("{{{name}:{}}}", ch_type(v)))
            }
        }
    }

    fn emit_table_ref(&mut self, t: &TableRef) -> Result<TableRefResult> {
        match t {
            TableRef::Scan {
                table,
                alias,
                type_filter,
            } => {
                let mut type_conditions = Vec::new();
                if let Some(tf) = type_filter {
                    let param = format!("type_{alias}");
                    self.params.insert(param.clone(), Value::String(tf.clone()));
                    type_conditions.push(format!("({alias}.label = {{{param}:String}})"));
                }
                Ok(TableRefResult {
                    sql: format!("{table} AS {alias}"),
                    type_conditions,
                })
            }
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let left_res = self.emit_table_ref(left)?;
                let right_res = self.emit_table_ref(right)?;
                let on_expr = self.emit_expr(on)?;

                let on_clause = if right_res.type_conditions.is_empty() {
                    on_expr
                } else {
                    format!(
                        "({} AND {})",
                        on_expr,
                        right_res.type_conditions.join(" AND ")
                    )
                };

                Ok(TableRefResult {
                    sql: format!(
                        "{} {} JOIN {} ON {}",
                        left_res.sql,
                        join_type.as_sql(),
                        right_res.sql,
                        on_clause
                    ),
                    type_conditions: left_res.type_conditions,
                })
            }
        }
    }
}

struct TableRefResult {
    sql: String,
    type_conditions: Vec<String>,
}

fn ch_type(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "String",
        Value::Number(n) if n.is_i64() => "Int64",
        Value::Number(_) => "Float64",
        Value::Bool(_) => "Bool",
        _ => "String",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, OrderExpr, SelectExpr};

    #[test]
    fn simple_select() {
        let q = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("n", "id"),
                    alias: Some("node_id".into()),
                },
                SelectExpr {
                    expr: Expr::col("n", "label"),
                    alias: Some("node_type".into()),
                },
            ],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::eq(Expr::col("n", "label"), Expr::lit("User"))),
            group_by: vec![],
            order_by: vec![],
            limit: Some(10),
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id AS node_id, n.label AS node_type FROM nodes AS n WHERE (n.label = {p0:String}) LIMIT 10"
        );
        assert_eq!(result.params.get("p0"), Some(&Value::from("User")));
    }

    #[test]
    fn with_join() {
        let q = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("n", "id"),
                    alias: Some("node_id".into()),
                },
                SelectExpr {
                    expr: Expr::col("e", "label"),
                    alias: Some("rel_type".into()),
                },
            ],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("nodes", "n"),
                TableRef::scan("edges", "e"),
                Expr::eq(Expr::col("n", "id"), Expr::col("e", "source_id")),
            ),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id AS node_id, e.label AS rel_type FROM nodes AS n INNER JOIN edges AS e ON (n.id = e.source_id)"
        );
    }

    #[test]
    fn aggregation() {
        let q = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("n", "label"),
                    alias: Some("type".into()),
                },
                SelectExpr {
                    expr: Expr::func("COUNT", vec![Expr::col("n", "id")]),
                    alias: Some("count".into()),
                },
            ],
            from: TableRef::scan("nodes", "n"),
            where_clause: None,
            group_by: vec![Expr::col("n", "label")],
            order_by: vec![OrderExpr {
                expr: Expr::func("COUNT", vec![Expr::col("n", "id")]),
                desc: true,
            }],
            limit: None,
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.label AS type, COUNT(n.id) AS count FROM nodes AS n GROUP BY n.label ORDER BY COUNT(n.id) DESC"
        );
    }

    #[test]
    fn in_operator() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: None,
            }],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::binary(
                Op::In,
                Expr::col("n", "label"),
                Expr::lit(Value::Array(vec![
                    Value::from("User"),
                    Value::from("Project"),
                    Value::from("Group"),
                ])),
            )),
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id FROM nodes AS n WHERE n.label IN ({p0:String}, {p1:String}, {p2:String})"
        );
    }

    #[test]
    fn and_or_conditions() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: None,
            }],
            from: TableRef::scan("nodes", "n"),
            where_clause: Expr::and_all([
                Some(Expr::eq(Expr::col("n", "label"), Expr::lit("User"))),
                Expr::or_all([
                    Some(Expr::binary(
                        Op::Gt,
                        Expr::col("n", "created_at"),
                        Expr::lit("2024-01-01"),
                    )),
                    Some(Expr::unary(Op::IsNull, Expr::col("n", "deleted_at"))),
                ]),
            ]),
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id FROM nodes AS n WHERE ((n.label = {p0:String}) AND ((n.created_at > {p1:String}) OR (n.deleted_at IS NULL)))"
        );
    }

    #[test]
    fn literals() {
        let mut ctx = Context::new();

        assert_eq!(
            ctx.emit_literal(&Value::from("hello")).unwrap(),
            "{p0:String}"
        );
        assert_eq!(ctx.emit_literal(&Value::from(42)).unwrap(), "{p1:Int64}");
        assert_eq!(ctx.emit_literal(&Value::from(true)).unwrap(), "{p2:Bool}");
        assert_eq!(ctx.emit_literal(&Value::Null).unwrap(), "NULL");
        assert_eq!(
            ctx.emit_literal(&Value::Array(vec![Value::from(1), Value::from(2)]))
                .unwrap(),
            "({p3:Int64}, {p4:Int64})"
        );
    }

    #[test]
    fn unary_ops() {
        let mut ctx = Context::new();

        assert_eq!(
            ctx.emit_expr(&Expr::unary(Op::IsNull, Expr::col("t", "deleted_at")))
                .unwrap(),
            "(t.deleted_at IS NULL)"
        );
        assert_eq!(
            ctx.emit_expr(&Expr::unary(Op::Not, Expr::col("t", "active")))
                .unwrap(),
            "(NOT t.active)"
        );
    }

    #[test]
    fn type_filter_in_where() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: Some("user_id".into()),
            }],
            from: TableRef::scan_with_filter("nodes", "u", "User"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(10),
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert!(result.sql.contains("WHERE (u.label = {type_u:String})"));
        assert_eq!(
            result.params.get("type_u"),
            Some(&Value::String("User".into()))
        );
    }

    #[test]
    fn type_filter_in_join() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan_with_filter("nodes", "u", "User"),
                TableRef::scan_with_filter("edges", "e", "AUTHORED"),
                Expr::eq(Expr::col("u", "id"), Expr::col("e", "from_id")),
            ),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = codegen(&Node::Query(Box::new(q))).unwrap();
        assert!(result
            .sql
            .contains("ON ((u.id = e.from_id) AND (e.label = {type_e:String}))"));
        assert!(result.sql.contains("WHERE (u.label = {type_u:String})"));
    }
}
