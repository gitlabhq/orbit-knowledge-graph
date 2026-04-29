//! DuckDB SQL code generation.
//!
//! Emits parameterized SQL using DuckDB's positional `$N` bind syntax and
//! DuckDB-native functions (`starts_with`, `list_contains`, `list_value`, etc.).
//!
//! Key differences from the ClickHouse backend:
//! - Parameters use positional `$1`, `$2`, ... instead of `{name:Type}`.
//! - Array IN clauses expand element-by-element (no native array bind).
//! - Functions are remapped to DuckDB equivalents.
//! - SET statements are skipped (ClickHouse-specific knobs).
//! - `if(cond, then, else)` is rewritten to `CASE WHEN ... THEN ... ELSE ... END`.
//! - Recursive CTE bodies have LIMIT/OFFSET stripped (DuckDB restriction).

use gkg_server_config::QueryConfig;

use crate::ast::{ChType, Cte, Expr, Insert, JoinType, Node, Op, Query, TableRef};
use crate::error::Result;
use crate::passes::enforce::ResultContext;
use serde_json::Value;
use std::collections::HashMap;

use super::{ParamValue, ParameterizedQuery, SqlDialect};

pub fn codegen(ast: &Node, result_context: ResultContext) -> Result<ParameterizedQuery> {
    let mut ctx = Context::new();
    let sql = match ast {
        Node::Query(q) => ctx.emit_query(q)?,
        Node::Insert(ins) => ctx.emit_insert(ins),
    };
    Ok(ParameterizedQuery {
        sql,
        params: ctx.params,
        result_context,
        query_config: QueryConfig::default(),
        dialect: SqlDialect::DuckDb,
    })
}

struct Context {
    params: HashMap<String, ParamValue>,
    param_counter: usize,
}

impl Context {
    fn new() -> Self {
        Self {
            params: HashMap::new(),
            param_counter: 0,
        }
    }

    fn emit_insert(&mut self, ins: &Insert) -> String {
        let cols = ins.columns().join(", ");
        let rows: Vec<String> = ins
            .values()
            .iter()
            .map(|row| {
                let exprs: Vec<String> = row.iter().map(|e| self.emit_expr(e)).collect();
                format!("({})", exprs.join(", "))
            })
            .collect();
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            ins.table(),
            cols,
            rows.join(", ")
        )
    }

    fn emit_query(&mut self, q: &Query) -> Result<String> {
        let mut parts = Vec::new();

        if !q.ctes.is_empty() {
            parts.push(self.emit_ctes(&q.ctes)?);
        }

        parts.push(self.emit_query_body(q)?);

        Ok(parts.join(" "))
    }

    fn emit_ctes(&mut self, ctes: &[Cte]) -> Result<String> {
        let has_recursive = ctes.iter().any(|c| c.recursive);
        let keyword = if has_recursive {
            "WITH RECURSIVE"
        } else {
            "WITH"
        };

        let cte_parts: Vec<String> = ctes
            .iter()
            .map(|cte| {
                if cte.recursive {
                    // DuckDB: recursive CTE bodies must not have LIMIT/OFFSET.
                    let inner = self.emit_query_body_without_limit(&cte.query)?;
                    Ok(format!("{} AS ({})", cte.name, inner))
                } else {
                    let inner = self.emit_query_body(&cte.query)?;
                    Ok(format!("{} AS ({})", cte.name, inner))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(format!("{} {}", keyword, cte_parts.join(", ")))
    }

    fn emit_query_body(&mut self, q: &Query) -> Result<String> {
        self.emit_query_body_inner(q, true)
    }

    fn emit_query_body_without_limit(&mut self, q: &Query) -> Result<String> {
        self.emit_query_body_inner(q, false)
    }

    fn emit_query_body_inner(&mut self, q: &Query, include_limit: bool) -> Result<String> {
        let mut parts = Vec::new();

        let select_items: Vec<_> = q
            .select
            .iter()
            .map(|sel| {
                let expr = self.emit_expr(&sel.expr);
                match &sel.alias {
                    Some(alias) => format!("{expr} AS {alias}"),
                    None => expr,
                }
            })
            .collect();
        parts.push(format!("SELECT {}", select_items.join(", ")));

        let from = self.emit_table_ref(&q.from)?;
        parts.push(format!("FROM {from}"));

        if let Some(w) = &q.where_clause {
            parts.push(format!("WHERE {}", self.emit_expr(w)));
        }

        if !q.group_by.is_empty() {
            let groups: Vec<_> = q.group_by.iter().map(|g| self.emit_expr(g)).collect();
            parts.push(format!("GROUP BY {}", groups.join(", ")));
        }

        if let Some(h) = &q.having {
            parts.push(format!("HAVING {}", self.emit_expr(h)));
        }

        for union_q in &q.union_all {
            parts.push(format!("UNION ALL {}", self.emit_query_body(union_q)?));
        }

        if !q.order_by.is_empty() {
            let orders: Vec<_> = q
                .order_by
                .iter()
                .map(|o| {
                    let dir = if o.desc { "DESC" } else { "ASC" };
                    format!("{} {dir}", self.emit_expr(&o.expr))
                })
                .collect();
            parts.push(format!("ORDER BY {}", orders.join(", ")));
        }

        if include_limit && let Some(limit) = q.limit {
            parts.push(format!("LIMIT {limit}"));
        }

        Ok(parts.join(" "))
    }

    fn emit_expr(&mut self, e: &Expr) -> String {
        match e {
            Expr::Column { table, column } => format!("{table}.{column}"),
            Expr::Identifier(name) => name.clone(),
            Expr::Literal(v) => self.emit_literal(v),
            Expr::Param { data_type, value } => self.emit_param(*data_type, value),
            Expr::FuncCall { name, args } => self.emit_func_call(name, args),
            Expr::Lambda { param, body } => {
                let body = self.emit_expr(body);
                format!("{param} -> {body}")
            }
            Expr::BinaryOp { op, left, right } => {
                let l = self.emit_expr(left);
                let r = self.emit_expr(right);
                if *op == Op::In {
                    format!("{l} IN {r}")
                } else if *op == Op::Like {
                    // DuckDB has no default LIKE escape character, so the
                    // `\_` / `\%` produced by the compiler's escape_like would
                    // match a literal backslash. Request `\` explicitly.
                    format!("({l} LIKE {r} ESCAPE '\\')")
                } else {
                    format!("({l} {op} {r})")
                }
            }
            Expr::UnaryOp { op, expr } => {
                let e = self.emit_expr(expr);
                if *op == Op::IsNull || *op == Op::IsNotNull {
                    format!("({e} {op})")
                } else {
                    format!("({op} {e})")
                }
            }
            Expr::InSubquery {
                expr,
                cte_name,
                column,
            } => {
                let e = self.emit_expr(expr);
                format!("{e} IN (SELECT {column} FROM {cte_name})")
            }
            Expr::Star => "*".to_string(),
        }
    }

    fn emit_func_call(&mut self, name: &str, args: &[Expr]) -> String {
        // Rewrite `if(cond, then, else)` → `CASE WHEN cond THEN then ELSE else END`
        if name == "if" && args.len() == 3 {
            let cond = self.emit_expr(&args[0]);
            let then = self.emit_expr(&args[1]);
            let else_ = self.emit_expr(&args[2]);
            return format!("CASE WHEN {cond} THEN {then} ELSE {else_} END");
        }

        // toString(x) → CAST(x AS VARCHAR)
        if name == "toString" && args.len() == 1 {
            let inner = self.emit_expr(&args[0]);
            return format!("CAST({inner} AS VARCHAR)");
        }

        // toJSONString(x) → just emit x (the inner map() is already
        // rewritten to json_object() which returns a JSON string).
        if name == "toJSONString" && args.len() == 1 {
            return self.emit_expr(&args[0]);
        }

        let duckdb_name = match name {
            "startsWith" => "starts_with",
            "has" => "list_contains",
            "array" => "list_value",
            "arrayConcat" => "list_concat",
            "arrayReverse" => "list_reverse",
            "arrayResize" => "list_resize",
            // ClickHouse map(k1,v1,k2,v2) → DuckDB json_object(k1,v1,k2,v2)
            "map" => "json_object",
            "tuple" => "row",
            other => other,
        };

        let args: Vec<_> = args.iter().map(|a| self.emit_expr(a)).collect();
        format!("{}({})", duckdb_name, args.join(", "))
    }

    fn emit_param(&mut self, data_type: ChType, v: &Value) -> String {
        match v {
            Value::Null => "NULL".into(),
            // DuckDB: no native array bind — always expand element-by-element.
            Value::Array(arr) => {
                let placeholders: Vec<_> = arr
                    .iter()
                    .map(|item| {
                        self.param_counter += 1;
                        let name = format!("p{}", self.param_counter);
                        let placeholder = format!("${}", self.param_counter);
                        // TODO: This will be abstracted away with LLQM to generic scalar types
                        let scalar_type = match data_type {
                            ChType::Array(inner) => ChType::from(inner),
                            _ => data_type,
                        };
                        self.params.insert(
                            name,
                            ParamValue {
                                ch_type: scalar_type,
                                value: item.clone(),
                            },
                        );
                        placeholder
                    })
                    .collect();
                format!("({})", placeholders.join(", "))
            }
            _ => {
                self.param_counter += 1;
                let name = format!("p{}", self.param_counter);
                let placeholder = format!("${}", self.param_counter);
                self.params.insert(
                    name,
                    ParamValue {
                        ch_type: data_type,
                        value: v.clone(),
                    },
                );
                placeholder
            }
        }
    }

    fn emit_literal(&mut self, v: &Value) -> String {
        match v {
            Value::Null => "NULL".into(),
            Value::Array(arr) => {
                let placeholders: Vec<_> = arr
                    .iter()
                    .map(|item| self.emit_param(ChType::from_value(item), item))
                    .collect();
                format!("({})", placeholders.join(", "))
            }
            _ => self.emit_param(ChType::from_value(v), v),
        }
    }

    fn emit_table_ref(&mut self, t: &TableRef) -> Result<String> {
        match t {
            TableRef::Scan { table, alias, .. } => Ok(format!("{table} AS {alias}")),
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let left_sql = self.emit_table_ref(left)?;
                let right_sql = self.emit_table_ref(right)?;
                if *join_type == JoinType::Cross {
                    Ok(format!("{left_sql} CROSS JOIN {right_sql}"))
                } else {
                    let on_expr = self.emit_expr(on);
                    Ok(format!(
                        "{left_sql} {join_type} JOIN {right_sql} ON {on_expr}"
                    ))
                }
            }
            TableRef::Union { queries, alias } => {
                let union_parts: Vec<String> = queries
                    .iter()
                    .map(|q| self.emit_query(q))
                    .collect::<Result<_>>()?;
                Ok(format!("({}) AS {alias}", union_parts.join(" UNION ALL ")))
            }
            TableRef::Subquery { query, alias } => {
                let inner_sql = self.emit_query(query)?;
                Ok(format!("({inner_sql}) AS {alias}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cte, JoinType, SelectExpr};

    fn empty_ctx() -> ResultContext {
        ResultContext::new()
    }

    #[test]
    fn positional_param_placeholders() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::eq(Expr::col("n", "label"), Expr::lit("User"))),
            limit: Some(10),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("$1"),
            "expected $1 placeholder: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("{p"),
            "should not contain CH-style params: {}",
            result.sql
        );
    }

    #[test]
    fn like_emits_backslash_escape() {
        // DuckDB's default LIKE has no escape character. The compiler emits
        // `\_` / `\%` for literal matches (see escape_like); without ESCAPE
        // '\', those collapse to literal backslash-underscore in DuckDB.
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::binary(
                Op::Like,
                Expr::col("n", "name"),
                Expr::lit("apply\\_%"),
            )),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("LIKE") && result.sql.contains("ESCAPE '\\'"),
            "expected LIKE ... ESCAPE '\\': {}",
            result.sql
        );
    }

    #[test]
    fn function_remapping() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::func(
                "startsWith",
                vec![Expr::col("n", "path"), Expr::lit("src/")],
            )),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("starts_with("),
            "expected starts_with: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("startsWith("),
            "should not contain CH function: {}",
            result.sql
        );
    }

    #[test]
    fn array_in_expanded() {
        let type_filter = Expr::col_in(
            "e",
            "kind",
            ChType::String,
            vec![Value::from("A"), Value::from("B"), Value::from("C")],
        )
        .unwrap();

        let q = Query {
            select: vec![SelectExpr::new(Expr::col("e", "id"), "id")],
            from: TableRef::scan("edges", "e"),
            where_clause: Some(type_filter),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("IN ($1, $2, $3)"),
            "expected expanded positional params: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("Array("),
            "should not contain Array() type: {}",
            result.sql
        );
    }

    #[test]
    fn if_to_case_when() {
        let q = Query {
            select: vec![SelectExpr::new(
                Expr::func(
                    "if",
                    vec![
                        Expr::binary(Op::Gt, Expr::col("n", "age"), Expr::lit(18)),
                        Expr::lit("adult"),
                        Expr::lit("minor"),
                    ],
                ),
                "category",
            )],
            from: TableRef::scan("nodes", "n"),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("CASE WHEN"),
            "expected CASE WHEN: {}",
            result.sql
        );
        assert!(result.sql.contains("THEN"), "expected THEN: {}", result.sql);
        assert!(result.sql.contains("ELSE"), "expected ELSE: {}", result.sql);
        assert!(result.sql.contains("END"), "expected END: {}", result.sql);
        assert!(
            !result.sql.contains("if("),
            "should not contain if(): {}",
            result.sql
        );
    }

    #[test]
    fn recursive_cte_strips_limit() {
        let q = Query {
            ctes: vec![Cte {
                name: "path_cte".into(),
                query: Box::new(Query {
                    select: vec![SelectExpr::new(Expr::col("p", "id"), "node_id")],
                    from: TableRef::scan("gl_project", "p"),
                    union_all: vec![Query {
                        select: vec![SelectExpr::new(Expr::col("c", "node_id"), "node_id")],
                        from: TableRef::scan("path_cte", "c"),
                        ..Default::default()
                    }],
                    limit: Some(1000),
                    ..Default::default()
                }),
                recursive: true,
            }],
            select: vec![SelectExpr::new(Expr::col("r", "node_id"), "id")],
            from: TableRef::scan("path_cte", "r"),
            limit: Some(10),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        // The outer LIMIT 10 should be present
        assert!(result.sql.contains("LIMIT 10"), "{}", result.sql);
        // The CTE body should NOT have LIMIT 1000
        let cte_body_end = result.sql.find(") SELECT").unwrap();
        let cte_body = &result.sql[..cte_body_end];
        assert!(
            !cte_body.contains("LIMIT 1000"),
            "recursive CTE body should not have LIMIT: {}",
            cte_body
        );
    }

    #[test]
    fn cross_join_emits_cross_join() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("a", "id"), "id")],
            from: TableRef::join(
                JoinType::Cross,
                TableRef::scan("t1", "a"),
                TableRef::scan("t2", "b"),
                Expr::lit(true),
            ),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(
            result.sql.contains("CROSS JOIN"),
            "expected CROSS JOIN: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("ON 1"),
            "DuckDB should not use ON 1 hack: {}",
            result.sql
        );
    }

    #[test]
    fn render_inlines_positional_params() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("n", "id"), "id")],
            from: TableRef::scan("nodes", "n"),
            where_clause: Some(Expr::eq(Expr::col("n", "name"), Expr::lit("alice"))),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx()).unwrap();
        assert!(result.sql.contains("$1"), "{}", result.sql);

        let rendered = result.render();
        assert!(
            rendered.contains("'alice'"),
            "expected inlined value: {}",
            rendered
        );
        assert!(
            !rendered.contains('$'),
            "should not contain $N after render: {}",
            rendered
        );
    }
}
