//! ClickHouse SQL code generation from the relation tree.
//!
//! Walks [`Rel`] / [`Expr`] directly — no Substrait, no positional field
//! references. Column names stay symbolic throughout.

use std::collections::HashMap;

use serde_json::Value;
use thiserror::Error;

use crate::ir::expr::{BinaryOp, Expr, JoinType, LiteralValue, SortDir, UnaryOp};
use crate::ir::plan::{
    AggregateRel, FetchRel, FilterRel, JoinRel, Plan, ProjectRel, RawFrom, ReadRel, Rel, SortRel,
    SubqueryRel, UnionAllRel,
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A parameterized ClickHouse SQL query with extracted parameter values.
#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, ParamValue>,
}

/// A query parameter with its ClickHouse type and JSON value.
#[derive(Debug, Clone)]
pub struct ParamValue {
    pub ch_type: String,
    pub value: Value,
}

#[derive(Debug, Error)]
pub enum CodegenError {
    #[error("unsupported relation type: {0}")]
    UnsupportedRelation(String),
    #[error("unsupported expression type: {0}")]
    UnsupportedExpression(String),
    #[error("missing required field: {0}")]
    MissingField(String),
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate a parameterized ClickHouse SQL query from a plan.
pub fn emit_clickhouse_sql(plan: &Plan) -> Result<ParameterizedQuery, CodegenError> {
    let mut ctx = CodegenContext::new();

    let cte_sql = if !plan.ctes.is_empty() {
        let has_recursive = plan.ctes.iter().any(|c| c.recursive);
        let keyword = if has_recursive {
            "WITH RECURSIVE"
        } else {
            "WITH"
        };

        let cte_parts: Vec<String> = plan
            .ctes
            .iter()
            .map(|cte| {
                let (cte_parts, _) =
                    ctx.emit_query(&cte.plan.root, Some(&cte.plan.output_names))?;
                Ok(format!("{} AS ({})", cte.name, cte_parts.to_sql()))
            })
            .collect::<Result<Vec<_>, CodegenError>>()?;

        Some(format!("{keyword} {}", cte_parts.join(", ")))
    } else {
        None
    };

    let (mut parts, _) = ctx.emit_query(&plan.root, Some(&plan.output_names))?;

    if let Some(cte_prefix) = cte_sql {
        parts.ctes.insert(0, cte_prefix);
    }

    Ok(ParameterizedQuery {
        sql: parts.to_sql(),
        params: ctx.params,
    })
}

// ---------------------------------------------------------------------------
// Internal: CodegenContext
// ---------------------------------------------------------------------------

struct CodegenContext {
    params: HashMap<String, ParamValue>,
    param_counter: usize,
}

impl CodegenContext {
    fn new() -> Self {
        Self {
            params: HashMap::new(),
            param_counter: 0,
        }
    }

    fn next_param_name(&mut self) -> String {
        let name = format!("p{}", self.param_counter);
        self.param_counter += 1;
        name
    }
}

// ---------------------------------------------------------------------------
// Internal: SQL clause collector
// ---------------------------------------------------------------------------

#[derive(Default)]
struct QueryParts {
    ctes: Vec<String>,
    select: Vec<String>,
    from: String,
    where_clauses: Vec<String>,
    group_by: Vec<String>,
    having: Option<String>,
    order_by: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    union_all: Vec<String>,
}

impl QueryParts {
    fn to_sql(&self) -> String {
        let mut sql = String::new();

        if !self.ctes.is_empty() {
            sql.push_str(&self.ctes.join(" "));
            sql.push(' ');
        }

        let select = if self.select.is_empty() {
            "*".to_string()
        } else {
            self.select.join(", ")
        };

        sql.push_str(&format!("SELECT {select} FROM {}", self.from));

        if !self.where_clauses.is_empty() {
            let combined = if self.where_clauses.len() == 1 {
                self.where_clauses[0].clone()
            } else {
                self.where_clauses
                    .iter()
                    .map(|w| format!("({w})"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            sql.push_str(&format!(" WHERE {combined}"));
        }

        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }

        if let Some(having) = &self.having {
            sql.push_str(&format!(" HAVING {having}"));
        }

        for union_sql in &self.union_all {
            sql.push_str(&format!(" UNION ALL {union_sql}"));
        }

        if !self.order_by.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", self.order_by.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql
    }
}

// ---------------------------------------------------------------------------
// Relation handlers
// ---------------------------------------------------------------------------

impl CodegenContext {
    fn emit_query(
        &mut self,
        rel: &Rel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        match rel {
            Rel::Read(r) => self.emit_read(r),
            Rel::Filter(f) => self.emit_filter(f, output_names),
            Rel::Project(p) => self.emit_project(p, output_names),
            Rel::Join(j) => self.emit_join(j),
            Rel::Sort(s) => self.emit_sort(s, output_names),
            Rel::Fetch(f) => self.emit_fetch(f, output_names),
            Rel::Aggregate(a) => self.emit_aggregate(a, output_names),
            Rel::UnionAll(u) => self.emit_union_all(u, output_names),
            Rel::Subquery(s) => self.emit_subquery(s, output_names),
        }
    }

    fn emit_read(&mut self, read: &ReadRel) -> Result<(QueryParts, Vec<String>), CodegenError> {
        // Raw FROM clause
        if read.table == RawFrom::TAG {
            let col_names = read.columns.iter().map(|c| c.name.clone()).collect();
            return Ok((
                QueryParts {
                    from: read.alias.clone(),
                    ..Default::default()
                },
                col_names,
            ));
        }

        let from = if read.alias.is_empty() || read.alias == read.table {
            read.table.clone()
        } else {
            format!("{} AS {}", read.table, read.alias)
        };

        let col_names = read.columns.iter().map(|c| c.name.clone()).collect();

        Ok((
            QueryParts {
                from,
                ..Default::default()
            },
            col_names,
        ))
    }

    fn emit_filter(
        &mut self,
        filter: &FilterRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        // Detect HAVING: filter on top of aggregate
        let is_having = matches!(&*filter.input, Rel::Aggregate(_));

        let (mut parts, col_names) = self.emit_query(&filter.input, output_names)?;
        let condition_sql = self.emit_expr(&filter.condition)?;

        if is_having {
            parts.having = Some(condition_sql);
        } else {
            parts.where_clauses.push(condition_sql);
        }

        Ok((parts, col_names))
    }

    fn emit_project(
        &mut self,
        project: &ProjectRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (mut parts, _) = self.emit_query(&project.input, None)?;

        let names = output_names.cloned().unwrap_or_else(|| {
            project
                .expressions
                .iter()
                .map(|(_, alias)| alias.clone())
                .collect()
        });

        let mut select_items = Vec::new();
        let mut col_names = Vec::new();

        for (i, (expr, default_alias)) in project.expressions.iter().enumerate() {
            let expr_sql = self.emit_expr(expr)?;
            let alias = names
                .get(i)
                .cloned()
                .unwrap_or_else(|| default_alias.clone());

            // Skip redundant alias for simple column refs
            if is_simple_column_ref(expr, &alias) {
                select_items.push(expr_sql);
            } else {
                select_items.push(format!("{expr_sql} AS {alias}"));
            }

            col_names.push(alias);
        }

        parts.select = select_items;
        Ok((parts, col_names))
    }

    fn emit_join(&mut self, join: &JoinRel) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (left_parts, left_cols) = self.emit_from_item(&join.left)?;
        let (right_parts, right_cols) = self.emit_from_item(&join.right)?;

        let join_type_str = match join.join_type {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL JOIN",
            JoinType::Cross => "CROSS JOIN",
        };

        let on_sql = self.emit_expr(&join.condition)?;

        let from = format!(
            "{} {join_type_str} {} ON {on_sql}",
            left_parts.from, right_parts.from
        );

        let mut parts = QueryParts {
            from,
            ..Default::default()
        };
        parts.where_clauses.extend(left_parts.where_clauses);
        parts.where_clauses.extend(right_parts.where_clauses);

        let mut col_names = left_cols;
        col_names.extend(right_cols);

        Ok((parts, col_names))
    }

    fn emit_sort(
        &mut self,
        sort: &SortRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (mut parts, col_names) = self.emit_query(&sort.input, output_names)?;

        for spec in &sort.sorts {
            let expr_sql = self.emit_expr(&spec.expr)?;
            let dir = match spec.direction {
                SortDir::Asc => "ASC",
                SortDir::Desc => "DESC",
            };
            parts.order_by.push(format!("{expr_sql} {dir}"));
        }

        Ok((parts, col_names))
    }

    fn emit_fetch(
        &mut self,
        fetch: &FetchRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (mut parts, col_names) = self.emit_query(&fetch.input, output_names)?;
        parts.limit = Some(fetch.limit);
        parts.offset = fetch.offset;
        Ok((parts, col_names))
    }

    fn emit_aggregate(
        &mut self,
        agg: &AggregateRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (mut parts, _) = self.emit_query(&agg.input, None)?;

        let names = output_names.cloned().unwrap_or_default();
        let mut select_items = Vec::new();
        let mut group_by_items = Vec::new();
        let mut col_names = Vec::new();
        let mut col_idx = 0;

        for group_expr in &agg.group_by {
            let expr_sql = self.emit_expr(group_expr)?;
            let alias = names
                .get(col_idx)
                .cloned()
                .unwrap_or_else(|| format!("_col{col_idx}"));

            if is_simple_column_ref(group_expr, &alias) {
                select_items.push(expr_sql.clone());
            } else {
                select_items.push(format!("{expr_sql} AS {alias}"));
            }
            group_by_items.push(expr_sql);
            col_names.push(alias);
            col_idx += 1;
        }

        for measure in &agg.measures {
            let alias = names
                .get(col_idx)
                .cloned()
                .unwrap_or_else(|| measure.alias.clone());

            let args: Vec<String> = measure
                .args
                .iter()
                .map(|a| self.emit_expr(a))
                .collect::<Result<_, _>>()?;
            let measure_sql = format!("{}({})", measure.function, args.join(", "));

            select_items.push(format!("{measure_sql} AS {alias}"));
            col_names.push(alias);
            col_idx += 1;
        }

        parts.select = select_items;
        parts.group_by = group_by_items;

        Ok((parts, col_names))
    }

    fn emit_union_all(
        &mut self,
        union: &UnionAllRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        if union.inputs.is_empty() {
            return Err(CodegenError::MissingField("UnionAll.inputs".into()));
        }

        let (first_parts, first_cols) = self.emit_query(&union.inputs[0], output_names)?;
        let mut union_sql = first_parts.to_sql();

        for input in &union.inputs[1..] {
            let (input_parts, _) = self.emit_query(input, output_names)?;
            union_sql.push_str(&format!(" UNION ALL {}", input_parts.to_sql()));
        }

        let col_names = first_cols.to_vec();

        Ok((
            QueryParts {
                from: format!("({union_sql}) AS {}", union.alias),
                ..Default::default()
            },
            col_names,
        ))
    }

    fn emit_subquery(
        &mut self,
        subquery: &SubqueryRel,
        output_names: Option<&Vec<String>>,
    ) -> Result<(QueryParts, Vec<String>), CodegenError> {
        let (inner_parts, inner_cols) = self.emit_query(&subquery.input, output_names)?;
        let inner_sql = inner_parts.to_sql();

        Ok((
            QueryParts {
                from: format!("({inner_sql}) AS {}", subquery.alias),
                ..Default::default()
            },
            inner_cols,
        ))
    }

    /// Emit a relation as a FROM-clause item. Simple reads, joins, unions, and
    /// subqueries are inlined. Everything else gets wrapped in a subquery.
    fn emit_from_item(&mut self, rel: &Rel) -> Result<(QueryParts, Vec<String>), CodegenError> {
        match rel {
            Rel::Read(_) | Rel::Join(_) | Rel::UnionAll(_) | Rel::Subquery(_) => {
                self.emit_query(rel, None)
            }
            _ => {
                let (inner_parts, cols) = self.emit_query(rel, None)?;
                let sql = inner_parts.to_sql();
                Ok((
                    QueryParts {
                        from: format!("({sql}) AS _sub"),
                        ..Default::default()
                    },
                    cols,
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Expression emitter
// ---------------------------------------------------------------------------

impl CodegenContext {
    fn emit_expr(&mut self, expr: &Expr) -> Result<String, CodegenError> {
        match expr {
            Expr::Column { table, name } => {
                if table.is_empty() {
                    Ok(name.clone())
                } else {
                    Ok(format!("{table}.{name}"))
                }
            }
            Expr::Literal(lit) => self.emit_literal(lit),
            Expr::Param { name, data_type } => Ok(format!("{{{name}:{data_type}}}")),
            Expr::BinaryOp { op, left, right } => {
                let l = self.emit_expr(left)?;
                let r = self.emit_expr(right)?;
                if *op == BinaryOp::In {
                    Ok(format!("{l} IN {r}"))
                } else {
                    Ok(format!("({l} {} {r})", op.as_sql()))
                }
            }
            Expr::UnaryOp { op, operand } => {
                let inner = self.emit_expr(operand)?;
                match op {
                    UnaryOp::Not => Ok(format!("(NOT {inner})")),
                    UnaryOp::IsNull => Ok(format!("({inner} IS NULL)")),
                    UnaryOp::IsNotNull => Ok(format!("({inner} IS NOT NULL)")),
                }
            }
            Expr::FuncCall { name, args } => {
                let arg_strs: Vec<String> = args
                    .iter()
                    .map(|a| self.emit_expr(a))
                    .collect::<Result<_, _>>()?;
                Ok(format!("{name}({})", arg_strs.join(", ")))
            }
            Expr::Cast { expr, target_type } => {
                let inner = self.emit_expr(expr)?;
                Ok(format!("CAST({inner} AS {target_type})"))
            }
            Expr::IfThen { ifs, else_expr } => {
                let mut sql = "CASE".to_string();
                for (cond, then) in ifs {
                    let cond_sql = self.emit_expr(cond)?;
                    let then_sql = self.emit_expr(then)?;
                    sql.push_str(&format!(" WHEN {cond_sql} THEN {then_sql}"));
                }
                if let Some(else_expr) = else_expr {
                    let else_sql = self.emit_expr(else_expr)?;
                    sql.push_str(&format!(" ELSE {else_sql}"));
                }
                sql.push_str(" END");
                Ok(sql)
            }
            Expr::InList { expr, list } => {
                let value = self.emit_expr(expr)?;
                let options: Vec<String> = list
                    .iter()
                    .map(|o| self.emit_expr(o))
                    .collect::<Result<_, _>>()?;
                Ok(format!("{value} IN ({})", options.join(", ")))
            }
            Expr::Raw(sql) => Ok(sql.clone()),
        }
    }

    fn emit_literal(&mut self, lit: &LiteralValue) -> Result<String, CodegenError> {
        let (ch_type, value) = match lit {
            LiteralValue::String(s) => ("String", Value::String(s.clone())),
            LiteralValue::Int64(n) => ("Int64", Value::Number((*n).into())),
            LiteralValue::Float64(f) => {
                let n = serde_json::Number::from_f64(*f)
                    .ok_or_else(|| CodegenError::UnsupportedExpression("NaN/Inf float".into()))?;
                ("Float64", Value::Number(n))
            }
            LiteralValue::Bool(b) => ("Bool", Value::Bool(*b)),
            LiteralValue::Null => return Ok("NULL".into()),
        };

        let name = self.next_param_name();
        let placeholder = format!("{{{name}:{ch_type}}}");
        self.params.insert(
            name,
            ParamValue {
                ch_type: ch_type.into(),
                value,
            },
        );
        Ok(placeholder)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if an expression is a simple column ref whose name matches the alias,
/// so we can skip the redundant `AS` clause.
fn is_simple_column_ref(expr: &Expr, alias: &str) -> bool {
    match expr {
        Expr::Column { name, .. } => name == alias,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Backend trait impl
// ---------------------------------------------------------------------------

pub struct ClickHouseBackend;

impl crate::pipeline::Backend for ClickHouseBackend {
    type Output = ParameterizedQuery;
    type Error = CodegenError;

    fn emit(&self, plan: &crate::ir::plan::Plan) -> Result<Self::Output, Self::Error> {
        emit_clickhouse_sql(plan)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::*;
    use crate::ir::plan::{CteDef, Measure};

    fn emit(plan: &Plan) -> ParameterizedQuery {
        emit_clickhouse_sql(plan).unwrap()
    }

    #[test]
    fn simple_select_where_order_limit() {
        let plan = Rel::read(
            "siphon_user",
            "t",
            &[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("_siphon_replicated_at", DataType::String),
                ("_siphon_deleted", DataType::Bool),
            ],
        )
        .filter(and([
            gt(
                col("t", "_siphon_replicated_at"),
                param("last_watermark", DataType::String),
            ),
            le(
                col("t", "_siphon_replicated_at"),
                param("watermark", DataType::String),
            ),
        ]))
        .sort(&[(col("t", "id"), SortDir::Asc)])
        .project(&[
            (col("t", "id"), "id"),
            (col("t", "name"), "name"),
            (col("t", "_siphon_replicated_at"), "_version"),
            (col("t", "_siphon_deleted"), "_deleted"),
        ])
        .fetch(100, None)
        .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("SELECT"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("siphon_user AS t"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("WHERE"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("ORDER BY"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("LIMIT 100"), "sql: {}", pq.sql);
        assert!(
            pq.sql.contains("{last_watermark:String}"),
            "sql: {}",
            pq.sql
        );
        assert!(
            pq.sql.contains("_siphon_replicated_at AS _version"),
            "sql: {}",
            pq.sql
        );
    }

    #[test]
    fn parameterized_literals() {
        let plan = Rel::read("users", "u", &[("id", DataType::Int64)])
            .filter(eq(col("u", "id"), int(42)))
            .project(&[(col("u", "id"), "id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("{p0:Int64}"), "sql: {}", pq.sql);
        assert_eq!(pq.params["p0"].value, Value::Number(42.into()));
    }

    #[test]
    fn inner_join() {
        let projects = Rel::read("gl_project", "p", &[("id", DataType::Int64)]);
        let mrs = Rel::read(
            "gl_merge_request",
            "mr",
            &[("id", DataType::Int64), ("project_id", DataType::Int64)],
        );

        let plan = projects
            .join(
                JoinType::Inner,
                mrs,
                eq(col("p", "id"), col("mr", "project_id")),
            )
            .project(&[(col("p", "id"), "project_id"), (col("mr", "id"), "mr_id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("INNER JOIN"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("gl_merge_request AS mr"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("ON"), "sql: {}", pq.sql);
    }

    #[test]
    fn aggregate_group_by() {
        let plan = Rel::read(
            "gl_project",
            "p",
            &[("namespace_id", DataType::Int64), ("id", DataType::Int64)],
        )
        .aggregate(
            &[col("p", "namespace_id")],
            &[Measure::new("count", &[col("p", "id")], "cnt")],
        )
        .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("GROUP BY"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("count("), "sql: {}", pq.sql);
        assert!(pq.sql.contains("AS cnt"), "sql: {}", pq.sql);
    }

    #[test]
    fn having_filter_on_aggregate() {
        let plan = Rel::read(
            "gl_project",
            "p",
            &[("namespace_id", DataType::Int64), ("id", DataType::Int64)],
        )
        .aggregate(
            &[col("p", "namespace_id")],
            &[Measure::new("count", &[col("p", "id")], "cnt")],
        )
        .filter(gt(func("count", vec![col("p", "id")]), int(5)))
        .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("HAVING"), "sql: {}", pq.sql);
    }

    #[test]
    fn union_all() {
        let a = Rel::read("t1", "a", &[("id", DataType::Int64)]).project(&[(col("a", "id"), "id")]);
        let b = Rel::read("t2", "b", &[("id", DataType::Int64)]).project(&[(col("b", "id"), "id")]);

        let plan = Rel::union_all(vec![a, b], "combined")
            .project(&[(col("combined", "id"), "id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("UNION ALL"), "sql: {}", pq.sql);
        assert!(pq.sql.contains(") AS combined"), "sql: {}", pq.sql);
    }

    #[test]
    fn subquery() {
        let plan = Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .subquery("sq")
            .project(&[(col("sq", "id"), "id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains(") AS sq"), "sql: {}", pq.sql);
    }

    #[test]
    fn cte_with_recursive() {
        let base = Rel::read("gl_project", "p", &[("node_id", DataType::Int64)])
            .project(&[(col("p", "node_id"), "node_id")]);
        let recursive = Rel::read("path_cte", "c", &[("node_id", DataType::Int64)])
            .project(&[(col("c", "node_id"), "node_id")]);

        let cte_plan = Rel::union_all(vec![base, recursive], "cte_body")
            .project(&[(col("cte_body", "node_id"), "node_id")])
            .into_plan();

        let plan = Rel::read("path_cte", "r", &[("node_id", DataType::Int64)])
            .project(&[(col("r", "node_id"), "node_id")])
            .fetch(10, None)
            .into_plan_with_ctes(vec![CteDef {
                name: "path_cte".into(),
                plan: cte_plan,
                recursive: true,
            }]);

        let pq = emit(&plan);
        assert!(pq.sql.contains("WITH RECURSIVE"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("path_cte AS ("), "sql: {}", pq.sql);
        assert!(pq.sql.contains("UNION ALL"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("LIMIT 10"), "sql: {}", pq.sql);
    }

    #[test]
    fn raw_expr() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .filter(eq(raw("t.custom_col"), int(1)))
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("t.custom_col"), "sql: {}", pq.sql);
    }

    #[test]
    fn cast_expr() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(cast(col("t", "id"), DataType::String), "id_str")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("CAST(t.id AS String)"), "sql: {}", pq.sql);
    }

    #[test]
    fn if_then_expr() {
        let plan = Rel::read("t", "t", &[("status", DataType::Int64)])
            .project(&[(
                if_then(
                    vec![(eq(col("t", "status"), int(1)), string("active"))],
                    Some(string("inactive")),
                ),
                "label",
            )])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("CASE WHEN"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("ELSE"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("END"), "sql: {}", pq.sql);
    }

    #[test]
    fn in_list_expr() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .filter(in_list(col("t", "id"), vec![int(1), int(2), int(3)]))
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("IN ("), "sql: {}", pq.sql);
    }

    #[test]
    fn starts_with_function() {
        let plan = Rel::read("gl_project", "p", &[("traversal_path", DataType::String)])
            .filter(starts_with(col("p", "traversal_path"), string("42/")))
            .project(&[(col("p", "traversal_path"), "traversal_path")])
            .into_plan();

        let pq = emit(&plan);
        assert!(
            pq.sql.contains("startsWith(p.traversal_path,"),
            "sql: {}",
            pq.sql
        );
    }

    #[test]
    fn three_way_join() {
        let p = Rel::read("gl_project", "p", &[("id", DataType::Int64)]);
        let mr = Rel::read("gl_merge_request", "mr", &[("project_id", DataType::Int64)]);
        let u = Rel::read("gl_user", "u", &[("id", DataType::Int64)]);

        let plan = p
            .join(
                JoinType::Inner,
                mr,
                eq(col("p", "id"), col("mr", "project_id")),
            )
            .join(
                JoinType::Left,
                u,
                eq(col("mr", "project_id"), col("u", "id")),
            )
            .project(&[(col("p", "id"), "pid")])
            .into_plan();

        let pq = emit(&plan);
        assert!(pq.sql.contains("INNER JOIN"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("LEFT JOIN"), "sql: {}", pq.sql);
    }
}
