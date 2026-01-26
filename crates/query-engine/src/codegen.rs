//! Codegen: AST → SQL
//!
//! Converts AST nodes to parameterized ClickHouse SQL.
//!
//! Note: All validation (identifier safety, ontology checks) is performed at
//! earlier stages (input parsing in `input.rs`, lowering in `lower.rs`).
//! By the time data reaches codegen, the AST is fully validated.

use crate::ast::{Expr, Node, Op, Query, RecursiveCte, TableRef};
use crate::error::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;

/// Parameterized SQL query with bound parameters
#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, Value>,
}

/// Convert an AST node to parameterized SQL
#[must_use = "the generated SQL should be used"]
pub fn codegen(ast: &Node) -> Result<ParameterizedQuery> {
    let mut params = HashMap::new();

    let sql = match ast {
        Node::Query(q) => emit_query(q.as_ref(), &mut params)?,
        Node::RecursiveCte(cte) => emit_recursive_cte(cte.as_ref(), &mut params)?,
    };

    Ok(ParameterizedQuery { sql, params })
}

fn emit_query(q: &Query, params: &mut HashMap<String, Value>) -> Result<String> {
    let mut sql = String::new();

    // SELECT clause
    sql.push_str("SELECT ");
    for (i, sel) in q.select.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&emit_expr(&sel.expr, params)?);
        if let Some(ref alias) = sel.alias {
            write!(sql, " AS {alias}").unwrap();
        }
    }

    // FROM clause
    sql.push_str(" FROM ");
    let from_result = emit_table_ref(&q.from, params)?;
    sql.push_str(&from_result.sql);

    // WHERE clause - combine original WHERE with any base table type conditions
    let has_where = q.where_clause.is_some();
    let has_type_conds = !from_result.type_conditions.is_empty();

    if has_where || has_type_conds {
        sql.push_str(" WHERE ");

        let mut conditions = Vec::new();

        // Add base table type conditions
        conditions.extend(from_result.type_conditions);

        // Add original WHERE clause
        if let Some(ref w) = q.where_clause {
            conditions.push(emit_expr(w, params)?);
        }

        sql.push_str(&conditions.join(" AND "));
    }

    // GROUP BY clause
    if !q.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        for (i, g) in q.group_by.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&emit_expr(g, params)?);
        }
    }

    // ORDER BY clause
    if !q.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        for (i, o) in q.order_by.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&emit_expr(&o.expr, params)?);
            sql.push_str(if o.desc { " DESC" } else { " ASC" });
        }
    }

    // LIMIT clause
    if let Some(limit) = q.limit {
        write!(sql, " LIMIT {limit}").unwrap();
    }

    Ok(sql)
}

fn emit_recursive_cte(cte: &RecursiveCte, params: &mut HashMap<String, Value>) -> Result<String> {
    let mut sql = String::new();

    sql.push_str("WITH RECURSIVE ");
    sql.push_str(&cte.name);
    sql.push_str(" AS (\n  ");

    // Base case
    sql.push_str(&emit_query(&cte.base, params)?);

    sql.push_str("\n  UNION ALL\n  ");

    // Recursive case
    sql.push_str(&emit_query(&cte.recursive, params)?);

    sql.push_str("\n)\n");

    // Final query
    sql.push_str(&emit_query(&cte.final_query, params)?);

    Ok(sql)
}

fn emit_expr(e: &Expr, params: &mut HashMap<String, Value>) -> Result<String> {
    match e {
        Expr::Column { table, column } => Ok(format!("{table}.{column}")),

        Expr::Literal(value) => emit_literal(value, params),

        Expr::FuncCall { name, args } => {
            let mut sql = String::new();
            sql.push_str(name);
            sql.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&emit_expr(arg, params)?);
            }
            sql.push(')');
            Ok(sql)
        }

        Expr::BinaryOp { op, left, right } => {
            let left_sql = emit_expr(left, params)?;
            let right_sql = emit_expr(right, params)?;

            // Handle IN operator specially
            if *op == Op::In {
                Ok(format!("{left_sql} IN {right_sql}"))
            } else {
                Ok(format!("({left_sql} {} {right_sql})", op.as_sql()))
            }
        }

        Expr::UnaryOp { op, expr } => {
            let expr_sql = emit_expr(expr, params)?;

            // Handle postfix operators (IS NULL, IS NOT NULL)
            if *op == Op::IsNull || *op == Op::IsNotNull {
                Ok(format!("({expr_sql} {})", op.as_sql()))
            } else {
                // Prefix operators
                Ok(format!("({} {expr_sql})", op.as_sql()))
            }
        }
    }
}

/// Emit a literal value as a ClickHouse native placeholder {name:Type}
fn emit_literal(v: &Value, params: &mut HashMap<String, Value>) -> Result<String> {
    match v {
        Value::Null => Ok("NULL".to_string()),

        Value::Array(arr) => {
            // Array literal for IN clause - store each item separately
            let mut placeholders = Vec::with_capacity(arr.len());
            for item in arr {
                let name = format!("p{}", params.len());
                let ch_type = ch_type_of(item);
                params.insert(name.clone(), item.clone());
                placeholders.push(format!("{{{name}:{ch_type}}}"));
            }
            Ok(format!("({})", placeholders.join(", ")))
        }

        _ => {
            // Normal scalar type
            let name = format!("p{}", params.len());
            let ch_type = ch_type_of(v);
            params.insert(name.clone(), v.clone());
            Ok(format!("{{{name}:{ch_type}}}"))
        }
    }
}

fn ch_type_of(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "String",
        Value::Number(n) => {
            if n.is_i64() {
                "Int64"
            } else if n.is_f64() {
                "Float64"
            } else {
                "Int64"
            }
        }
        Value::Bool(_) => "Bool",
        _ => "String",
    }
}

/// Result of emitting a table reference, including any type filter conditions
struct TableRefResult {
    sql: String,
    /// Type filter conditions that need to be added to WHERE/ON clauses
    type_conditions: Vec<String>,
}

fn emit_table_ref(t: &TableRef, params: &mut HashMap<String, Value>) -> Result<TableRefResult> {
    match t {
        TableRef::Scan {
            table,
            alias,
            type_filter,
        } => {
            let mut type_conditions = Vec::new();

            // Type filters are validated in lower.rs when building the AST
            if let Some(tf) = type_filter {
                // Both nodes and edges use "label" column for type filtering
                let label_column = "label";

                // Add parameterized type filter condition
                let param_name = format!("type_{alias}");
                params.insert(param_name.clone(), Value::String(tf.clone()));
                type_conditions.push(format!(
                    "({alias}.{label_column} = {{{param_name}:String}})"
                ));
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
            let mut sql = String::new();

            // Left side
            let left_result = emit_table_ref(left, params)?;
            sql.push_str(&left_result.sql);

            // Join type and right side
            write!(sql, " {} JOIN ", join_type.as_sql()).unwrap();
            let right_result = emit_table_ref(right, params)?;
            sql.push_str(&right_result.sql);

            // ON clause - combine original condition with any type filters from right side
            sql.push_str(" ON ");
            let on_expr = emit_expr(on, params)?;

            // Add right-side type conditions to the ON clause
            if right_result.type_conditions.is_empty() {
                sql.push_str(&on_expr);
            } else {
                sql.push_str(&format!(
                    "({} AND {})",
                    on_expr,
                    right_result.type_conditions.join(" AND ")
                ));
            }

            // Propagate left-side type conditions up (right-side are in ON clause)
            Ok(TableRefResult {
                sql,
                type_conditions: left_result.type_conditions,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, OrderExpr, SelectExpr};
    use std::collections::HashMap;

    #[test]
    fn test_emit_simple_select() {
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
    fn test_emit_with_join() {
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
    fn test_emit_aggregation() {
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
    fn test_emit_in_operator() {
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
        assert_eq!(result.params.get("p0"), Some(&Value::from("User")));
        assert_eq!(result.params.get("p1"), Some(&Value::from("Project")));
        assert_eq!(result.params.get("p2"), Some(&Value::from("Group")));
    }

    #[test]
    fn test_emit_and_or_conditions() {
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
    fn test_emit_literals() {
        let mut params = HashMap::new();

        // String
        let sql = emit_literal(&Value::from("hello"), &mut params).unwrap();
        assert_eq!(sql, "{p0:String}");
        assert_eq!(params.get("p0"), Some(&Value::from("hello")));

        // Int
        let sql = emit_literal(&Value::from(42), &mut params).unwrap();
        assert_eq!(sql, "{p1:Int64}");

        // Bool
        let sql = emit_literal(&Value::from(true), &mut params).unwrap();
        assert_eq!(sql, "{p2:Bool}");

        // Null
        let sql = emit_literal(&Value::Null, &mut params).unwrap();
        assert_eq!(sql, "NULL");

        // Array
        let sql = emit_literal(
            &Value::Array(vec![Value::from(1), Value::from(2)]),
            &mut params,
        )
        .unwrap();
        assert_eq!(sql, "({p3:Int64}, {p4:Int64})");
    }

    #[test]
    fn test_emit_unary_ops() {
        let mut params = HashMap::new();

        // IS NULL
        let sql = emit_expr(
            &Expr::unary(Op::IsNull, Expr::col("t", "deleted_at")),
            &mut params,
        )
        .unwrap();
        assert_eq!(sql, "(t.deleted_at IS NULL)");

        // NOT
        let sql = emit_expr(&Expr::unary(Op::Not, Expr::col("t", "active")), &mut params).unwrap();
        assert_eq!(sql, "(NOT t.active)");
    }

    #[test]
    fn test_type_filter_emitted_in_sql() {
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

        // Type filter validation now happens in lower.rs, codegen just emits the filter
        let result = codegen(&Node::Query(Box::new(q))).unwrap();

        // Type filter should be in WHERE clause
        assert!(
            result.sql.contains("WHERE (u.label = {type_u:String})"),
            "type filter should be in WHERE: {}",
            result.sql
        );
        assert_eq!(
            result.params.get("type_u"),
            Some(&Value::String("User".into()))
        );
    }

    #[test]
    fn test_type_filter_in_join() {
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

        // Type filter validation now happens in lower.rs, codegen just emits the filter
        let result = codegen(&Node::Query(Box::new(q))).unwrap();

        // Edge type filter should be in ON clause
        assert!(
            result
                .sql
                .contains("ON ((u.id = e.from_id) AND (e.label = {type_e:String}))"),
            "edge type filter should be in ON clause: {}",
            result.sql
        );
        // Base table type filter should be in WHERE
        assert!(
            result.sql.contains("WHERE (u.label = {type_u:String})"),
            "base type filter should be in WHERE: {}",
            result.sql
        );
    }
}
