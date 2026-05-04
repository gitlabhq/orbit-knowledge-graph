//! ClickHouse SQL code generation.
//!
//! Emits parameterized SQL using ClickHouse's `{name:Type}` bind syntax and
//! ClickHouse-specific functions (`startsWith`, `has`, `array`, etc.).

use gkg_server_config::QueryConfig;

use crate::ast::{ChType, Cte, Expr, Insert, JoinType, Node, Op, Query, TableRef};
use crate::error::Result;
use crate::passes::enforce::ResultContext;
use serde_json::Value;
use std::collections::HashMap;

use super::{ParamValue, ParameterizedQuery, SqlDialect};

pub fn codegen(
    ast: &Node,
    result_context: ResultContext,
    query_config: QueryConfig,
) -> Result<ParameterizedQuery> {
    let mut ctx = Context::new();
    let mut sql = match ast {
        Node::Query(q) => ctx.emit_query(q)?,
        Node::Insert(ins) => ctx.emit_insert(ins),
    };

    // SETTINGS — only on SELECT queries, not INSERT or subqueries/UNION arms.
    // Values are pre-formatted as SQL-safe literals by to_clickhouse_settings()
    // (bare integers, 0/1 bools, escaped quoted strings).
    if matches!(ast, Node::Query(_)) {
        let mut settings = query_config
            .to_clickhouse_settings()
            .map_err(crate::error::QueryError::Codegen)?;

        settings.extend(query_config.compiler_derived.to_clickhouse_settings());

        if !settings.is_empty() {
            let clause: Vec<String> = settings.iter().map(|(k, v)| format!("{k} = {v}")).collect();
            sql.push_str(&format!(" SETTINGS {}", clause.join(", ")));
        }
    }

    Ok(ParameterizedQuery {
        sql,
        params: ctx.params,
        result_context,
        query_config,
        dialect: SqlDialect::ClickHouse,
    })
}

/// Emit a `Query` (or `Insert`) AST as parameterized ClickHouse SQL without
/// requiring `ResultContext` or `QueryConfig`.
///
/// # Trust boundary
///
/// This function bypasses the compiler security pipeline (`apply_security_context`,
/// `check_ast`, `enforce_return`). It must only be used for trusted, internally
/// constructed ASTs (e.g. schema version management DDL/DML), never for
/// user-supplied query input.
pub fn emit_simple_query(node: &Node) -> Result<(String, HashMap<String, ParamValue>)> {
    let mut ctx = Context::new();
    let sql = match node {
        Node::Query(q) => ctx.emit_query(q)?,
        Node::Insert(ins) => ctx.emit_insert(ins),
    };
    Ok((sql, ctx.params))
}

struct Context {
    params: HashMap<String, ParamValue>,
}

impl Context {
    fn new() -> Self {
        Self {
            params: HashMap::new(),
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

        // WITH clause (CTEs)
        if !q.ctes.is_empty() {
            parts.push(self.emit_ctes(&q.ctes)?);
        }

        // SELECT, FROM, WHERE, GROUP BY, HAVING, UNION ALL, ORDER BY, LIMIT
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
                let inner = self.emit_query_body(&cte.query)?;
                if cte.materialized {
                    Ok(format!("{} AS MATERIALIZED ({})", cte.name, inner))
                } else {
                    Ok(format!("{} AS ({})", cte.name, inner))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(format!("{} {}", keyword, cte_parts.join(", ")))
    }

    fn emit_query_body(&mut self, q: &Query) -> Result<String> {
        let mut parts = Vec::new();

        // SELECT
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
        let keyword = if q.distinct {
            "SELECT DISTINCT"
        } else {
            "SELECT"
        };
        parts.push(format!("{keyword} {}", select_items.join(", ")));

        // FROM
        let from = self.emit_table_ref(&q.from)?;
        parts.push(format!("FROM {from}"));

        // WHERE
        if let Some(w) = &q.where_clause {
            parts.push(format!("WHERE {}", self.emit_expr(w)));
        }

        // GROUP BY
        if !q.group_by.is_empty() {
            let groups: Vec<_> = q.group_by.iter().map(|g| self.emit_expr(g)).collect();
            parts.push(format!("GROUP BY {}", groups.join(", ")));
        }

        // HAVING
        if let Some(h) = &q.having {
            parts.push(format!("HAVING {}", self.emit_expr(h)));
        }

        // UNION ALL
        for union_q in &q.union_all {
            parts.push(format!("UNION ALL {}", self.emit_query_body(union_q)?));
        }

        // ClickHouse binds a trailing ORDER BY / LIMIT to the last branch of an
        // unparenthesized UNION ALL, not the whole union. Wrap so they apply
        // to the combined result.
        if !q.union_all.is_empty()
            && (q.limit.is_some() || q.limit_by.is_some() || !q.order_by.is_empty())
        {
            let body = std::mem::take(&mut parts).join(" ");
            parts.push(format!("SELECT * FROM ({body})"));
        }

        // ORDER BY
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

        if let Some((n, ref cols)) = q.limit_by {
            let cols: Vec<_> = cols.iter().map(|c| self.emit_expr(c)).collect();
            parts.push(format!("LIMIT {n} BY {}", cols.join(", ")));
        }

        if let Some(limit) = q.limit {
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
            Expr::FuncCall { name, args } => {
                let args: Vec<_> = args.iter().map(|a| self.emit_expr(a)).collect();
                format!("{}({})", name, args.join(", "))
            }
            Expr::Lambda { param, body } => {
                let body = self.emit_expr(body);
                format!("{param} -> {body}")
            }
            Expr::BinaryOp { op, left, right } => {
                let l = self.emit_expr(left);
                let r = self.emit_expr(right);
                if *op == Op::In {
                    format!("{l} IN {r}")
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

    fn emit_param(&mut self, data_type: ChType, v: &Value) -> String {
        match v {
            Value::Null => "NULL".into(),
            // Array ChType: bind the whole array as a single ClickHouse Array(T) param.
            Value::Array(_) if matches!(data_type, ChType::Array(_)) => {
                let name = format!("p{}", self.params.len());
                let placeholder = format!("{{{name}:{data_type}}}");
                self.params.insert(
                    name,
                    ParamValue {
                        ch_type: data_type,
                        value: v.clone(),
                    },
                );
                placeholder
            }
            // Scalar ChType with array value: expand element-by-element.
            Value::Array(arr) => {
                let placeholders: Vec<_> = arr
                    .iter()
                    .map(|item| {
                        let name = format!("p{}", self.params.len());
                        let placeholder = format!("{{{name}:{data_type}}}");
                        self.params.insert(
                            name,
                            ParamValue {
                                ch_type: data_type,
                                value: item.clone(),
                            },
                        );
                        placeholder
                    })
                    .collect();
                format!("({})", placeholders.join(", "))
            }
            _ => {
                let name = format!("p{}", self.params.len());
                let placeholder = format!("{{{name}:{data_type}}}");
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
            TableRef::Scan {
                table,
                alias,
                final_,
            } => {
                if *final_ {
                    Ok(format!("{table} AS {alias} FINAL"))
                } else {
                    Ok(format!("{table} AS {alias}"))
                }
            }
            TableRef::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let left_sql = self.emit_table_ref(left)?;
                let right_sql = self.emit_table_ref(right)?;
                if *join_type == JoinType::Cross {
                    Ok(format!("{left_sql} INNER JOIN {right_sql} ON 1"))
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
    use crate::ast::{JoinType, OrderExpr, SelectExpr};

    fn empty_ctx() -> ResultContext {
        ResultContext::new()
    }

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
            limit: Some(10),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id AS node_id, n.label AS node_type FROM nodes AS n WHERE (n.label = {p0:String}) LIMIT 10"
        );
        assert_eq!(
            result.params.get("p0").map(|p| &p.value),
            Some(&Value::from("User"))
        );
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
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
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
            group_by: vec![Expr::col("n", "label")],
            order_by: vec![OrderExpr {
                expr: Expr::func("COUNT", vec![Expr::col("n", "id")]),
                desc: true,
            }],
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
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
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
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
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.id FROM nodes AS n WHERE ((n.label = {p0:String}) AND ((n.created_at > {p1:String}) OR (n.deleted_at IS NULL)))"
        );
    }

    #[test]
    fn literals() {
        let mut ctx = Context::new();

        assert_eq!(ctx.emit_literal(&Value::from("hello")), "{p0:String}");
        assert_eq!(ctx.emit_literal(&Value::from(42)), "{p1:Int64}");
        assert_eq!(ctx.emit_literal(&Value::from(true)), "{p2:Bool}");
        assert_eq!(ctx.emit_literal(&Value::Null), "NULL");
        assert_eq!(
            ctx.emit_literal(&Value::Array(vec![Value::from(1), Value::from(2)])),
            "({p3:Int64}, {p4:Int64})"
        );
    }

    #[test]
    fn unary_ops() {
        let mut ctx = Context::new();

        assert_eq!(
            ctx.emit_expr(&Expr::unary(Op::IsNull, Expr::col("t", "deleted_at"))),
            "(t.deleted_at IS NULL)"
        );
        assert_eq!(
            ctx.emit_expr(&Expr::unary(Op::Not, Expr::col("t", "active"))),
            "(NOT t.active)"
        );
    }

    #[test]
    fn edge_type_filter() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::scan("gl_edge", "e"),
                Expr::and(
                    Expr::eq(Expr::col("u", "id"), Expr::col("e", "source")),
                    Expr::eq(
                        Expr::col("e", "relationship_kind"),
                        Expr::string("AUTHORED"),
                    ),
                ),
            ),
            ..Default::default()
        };
        let r = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(
            r.sql.contains("e.relationship_kind = {p0:String}"),
            "{}",
            r.sql
        );
        assert_eq!(
            r.params.get("p0").map(|p| &p.value),
            Some(&Value::String("AUTHORED".into()))
        );

        let type_filter = Expr::col_in(
            "e",
            "relationship_kind",
            ChType::String,
            vec![
                Value::String("AUTHORED".into()),
                Value::String("CONTAINS".into()),
            ],
        )
        .unwrap();
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::scan("gl_edge", "e"),
                Expr::and(
                    Expr::eq(Expr::col("u", "id"), Expr::col("e", "source")),
                    type_filter,
                ),
            ),
            ..Default::default()
        };
        let r = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(
            r.sql.contains("e.relationship_kind IN {p0:Array(String)}"),
            "{}",
            r.sql
        );
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn result_context_preserved() {
        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");

        let q = Query {
            select: vec![],
            from: TableRef::scan("nodes", "n"),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), ctx, QueryConfig::empty()).unwrap();
        assert_eq!(result.result_context.len(), 1);
        assert_eq!(result.result_context.get("u").unwrap().entity_type, "User");
    }

    #[test]
    fn having_clause() {
        let q = Query {
            select: vec![
                SelectExpr::new(Expr::col("n", "label"), "type"),
                SelectExpr::new(Expr::func("COUNT", vec![Expr::col("n", "id")]), "count"),
            ],
            from: TableRef::scan("nodes", "n"),
            group_by: vec![Expr::col("n", "label")],
            having: Some(Expr::binary(
                Op::Gt,
                Expr::func("COUNT", vec![Expr::col("n", "id")]),
                Expr::lit(5),
            )),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert_eq!(
            result.sql,
            "SELECT n.label AS type, COUNT(n.id) AS count FROM nodes AS n GROUP BY n.label HAVING (COUNT(n.id) > {p0:Int64})"
        );
    }

    #[test]
    fn having_without_group_by() {
        let q = Query {
            select: vec![SelectExpr::new(
                Expr::func("COUNT", vec![Expr::col("n", "id")]),
                "total",
            )],
            from: TableRef::scan("nodes", "n"),
            having: Some(Expr::binary(
                Op::Gt,
                Expr::func("COUNT", vec![Expr::col("n", "id")]),
                Expr::lit(0),
            )),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(result.sql.contains("HAVING"));
        assert!(!result.sql.contains("GROUP BY"));
    }

    #[test]
    fn subquery_in_from() {
        let inner = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "id"), "id"),
                SelectExpr::new(Expr::col("p", "name"), "name"),
            ],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(Expr::eq(Expr::col("p", "name"), Expr::lit("test"))),
            ..Default::default()
        };

        let outer = Query {
            select: vec![SelectExpr::new(Expr::col("sub", "id"), "id")],
            from: TableRef::subquery(inner, "sub"),
            ..Default::default()
        };

        let result = codegen(
            &Node::Query(Box::new(outer)),
            empty_ctx(),
            QueryConfig::empty(),
        )
        .unwrap();
        assert!(result.sql.contains("(SELECT"));
        assert!(result.sql.contains(") AS sub"));
        assert!(result.sql.contains("gl_project AS p"));
    }

    #[test]
    fn subquery_in_join() {
        let inner = Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "source_id")],
            from: TableRef::scan("gl_edge", "e"),
            group_by: vec![Expr::col("e", "source_id")],
            having: Some(Expr::eq(
                Expr::func(
                    "argMax",
                    vec![Expr::col("e", "_deleted"), Expr::col("e", "_version")],
                ),
                Expr::lit(false),
            )),
            ..Default::default()
        };

        let outer = Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::subquery(inner, "deduped_e"),
                Expr::eq(Expr::col("u", "id"), Expr::col("deduped_e", "source_id")),
            ),
            ..Default::default()
        };

        let result = codegen(
            &Node::Query(Box::new(outer)),
            empty_ctx(),
            QueryConfig::empty(),
        )
        .unwrap();
        assert!(result.sql.contains("INNER JOIN (SELECT"));
        assert!(result.sql.contains("HAVING"));
        assert!(result.sql.contains(") AS deduped_e ON"));
    }

    #[test]
    fn union_all_in_cte_body() {
        use crate::ast::Cte;

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
                    ..Default::default()
                }),
                recursive: true,
                materialized: false,
            }],
            select: vec![SelectExpr::new(Expr::col("r", "node_id"), "id")],
            from: TableRef::scan("path_cte", "r"),
            limit: Some(10),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(result.sql.contains("WITH RECURSIVE"));
        assert!(result.sql.contains("UNION ALL"));
    }

    #[test]
    fn union_all_in_top_level_query() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::scan("gl_user", "u"),
            union_all: vec![Query {
                select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
                from: TableRef::scan("gl_project", "p"),
                ..Default::default()
            }],
            limit: Some(10),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        // Outer LIMIT must apply to the whole union, not just the last branch.
        // ClickHouse binds a trailing LIMIT after a bare UNION ALL to the last
        // SELECT only, so the union is wrapped in a subquery.
        assert_eq!(
            result.sql,
            "SELECT * FROM (SELECT u.id AS id FROM gl_user AS u UNION ALL \
             SELECT p.id AS id FROM gl_project AS p) LIMIT 10"
        );
    }

    #[test]
    fn table_ref_union_emits_derived_table() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("all_edges", "id"), "id")],
            from: TableRef::Union {
                queries: vec![
                    Query {
                        select: vec![SelectExpr::new(Expr::col("e1", "source"), "id")],
                        from: TableRef::scan("gl_edge", "e1"),
                        ..Default::default()
                    },
                    Query {
                        select: vec![SelectExpr::new(Expr::col("e2", "source"), "id")],
                        from: TableRef::scan("gl_edge", "e2"),
                        ..Default::default()
                    },
                ],
                alias: "all_edges".into(),
            },
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(result.sql.contains("UNION ALL"));
        assert!(result.sql.contains(") AS all_edges"));
    }

    #[test]
    fn insert_values() {
        let ins = Insert::new(
            "gl_schema_versions",
            vec!["key".into(), "version".into()],
            vec![
                vec![Expr::string("graph"), Expr::int(3)],
                vec![Expr::string("datalake"), Expr::int(1)],
            ],
        );

        let result = codegen(
            &Node::Insert(Box::new(ins)),
            empty_ctx(),
            QueryConfig::empty(),
        )
        .unwrap();
        assert_eq!(
            result.sql,
            "INSERT INTO gl_schema_versions (key, version) VALUES ({p0:String}, {p1:Int64}), ({p2:String}, {p3:Int64})"
        );
        assert_eq!(
            result.params.get("p0").map(|p| &p.value),
            Some(&Value::String("graph".into()))
        );
        assert_eq!(
            result.params.get("p1").map(|p| &p.value),
            Some(&Value::Number(3.into()))
        );
    }

    #[test]
    fn insert_skips_settings() {
        let ins = Insert::new("t", vec!["a".into()], vec![vec![Expr::int(1)]]);

        let cfg = QueryConfig {
            max_execution_time: None,
            use_query_cache: Some(true),
            query_cache_ttl: Some(60),
            ..Default::default()
        };
        let result = codegen(&Node::Insert(Box::new(ins)), empty_ctx(), cfg).unwrap();
        assert!(
            !result.sql.contains("SETTINGS"),
            "INSERT should not have SETTINGS: {}",
            result.sql,
        );
    }

    #[test]
    fn scan_final() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("t", "id"), "id")],
            from: TableRef::scan_final("gl_schema_versions", "t"),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert_eq!(
            result.sql,
            "SELECT t.id AS id FROM gl_schema_versions AS t FINAL"
        );
    }

    #[test]
    fn emit_simple_query_with_final_and_params() {
        let q = Query {
            select: vec![
                SelectExpr::new(Expr::col("t", "key"), "key"),
                SelectExpr::new(Expr::col("t", "version"), "version"),
            ],
            from: TableRef::scan_final("gl_schema_versions", "t"),
            where_clause: Some(Expr::eq(Expr::col("t", "key"), Expr::string("graph"))),
            ..Default::default()
        };

        let (sql, params) = emit_simple_query(&Node::Query(Box::new(q))).unwrap();
        assert_eq!(
            sql,
            "SELECT t.key AS key, t.version AS version FROM gl_schema_versions AS t FINAL WHERE (t.key = {p0:String})"
        );
        assert_eq!(params.len(), 1);
        assert_eq!(
            params.get("p0").map(|p| &p.value),
            Some(&Value::String("graph".into()))
        );
    }

    #[test]
    fn render_replaces_scalar_params() {
        let mut params = HashMap::new();
        params.insert(
            "p0".into(),
            ParamValue {
                ch_type: ChType::String,
                value: Value::from("User"),
            },
        );
        params.insert(
            "p1".into(),
            ParamValue {
                ch_type: ChType::String,
                value: Value::from("active"),
            },
        );

        let pq = ParameterizedQuery {
            sql: "SELECT * FROM t WHERE kind = {p0:String} AND state = {p1:String}".into(),
            params,
            result_context: empty_ctx(),
            query_config: QueryConfig::empty(),
            dialect: SqlDialect::ClickHouse,
        };

        assert_eq!(
            pq.render(),
            "SELECT * FROM t WHERE kind = 'User' AND state = 'active'"
        );
    }

    #[test]
    fn render_replaces_array_params() {
        let mut params = HashMap::new();
        params.insert(
            "p0".into(),
            ParamValue {
                ch_type: ChType::Array(gkg_utils::clickhouse::ChScalar::String),
                value: serde_json::json!(["a", "b"]),
            },
        );
        params.insert(
            "p1".into(),
            ParamValue {
                ch_type: ChType::Array(gkg_utils::clickhouse::ChScalar::Int64),
                value: serde_json::json!([10, 20]),
            },
        );

        let pq = ParameterizedQuery {
            sql: "SELECT * FROM t WHERE x IN {p0:Array(String)} AND y IN {p1:Array(Int64)}".into(),
            params,
            result_context: empty_ctx(),
            query_config: QueryConfig::empty(),
            dialect: SqlDialect::ClickHouse,
        };

        assert_eq!(
            pq.render(),
            "SELECT * FROM t WHERE x IN ['a', 'b'] AND y IN [10, 20]"
        );
    }

    #[test]
    fn query_settings_emitted_after_limit() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: None,
            }],
            from: TableRef::scan("nodes", "n"),
            limit: Some(100),
            ..Default::default()
        };

        let cfg = QueryConfig {
            use_query_cache: Some(true),
            query_cache_ttl: Some(60),
            ..QueryConfig::empty()
        };
        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), cfg).unwrap();
        assert!(
            result.sql.contains("LIMIT 100 SETTINGS"),
            "SETTINGS should come after LIMIT: {}",
            result.sql,
        );
        assert!(result.sql.contains("use_query_cache = 1"), "{}", result.sql);
        assert!(
            result.sql.contains("query_cache_ttl = 60"),
            "{}",
            result.sql
        );
    }

    #[test]
    fn no_query_settings_when_empty() {
        let q = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: None,
            }],
            from: TableRef::scan("nodes", "n"),
            ..Default::default()
        };

        let result = codegen(&Node::Query(Box::new(q)), empty_ctx(), QueryConfig::empty()).unwrap();
        assert!(
            !result.sql.contains("SETTINGS"),
            "no SETTINGS with default config: {}",
            result.sql
        );
    }

    #[test]
    fn render_leaves_unknown_params() {
        let pq = ParameterizedQuery {
            sql: "SELECT {p0:String} AND {p1:Int64}".into(),
            params: HashMap::new(),
            result_context: empty_ctx(),
            query_config: QueryConfig::empty(),
            dialect: SqlDialect::ClickHouse,
        };

        assert_eq!(pq.render(), "SELECT {p0:String} AND {p1:Int64}");
    }
}
