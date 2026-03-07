//! Codegen: Plan → SQL
//!
//! Delegates to llqm's ClickHouse SQL codegen and wraps the result with
//! query-engine metadata (result context, hydration plan).

use crate::enforce::ResultContext;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::input::QueryType;
use serde_json::Value;
use std::collections::HashMap;

/// A query parameter with its ClickHouse type and JSON value.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamValue {
    pub ch_type: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, ParamValue>,
    pub result_context: ResultContext,
}

#[derive(Debug, Clone)]
pub struct CompiledQueryContext {
    pub query_type: QueryType,
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub input: Input,
}

#[derive(Debug, Clone)]
pub enum HydrationPlan {
    /// No hydration needed (e.g., Aggregation).
    None,
    /// Entity types known at compile time (Traversal, Search).
    /// One template per entity type, with IDs to be filled at runtime.
    Static(Vec<HydrationTemplate>),
    /// Entity types discovered at runtime (PathFinding, Neighbors).
    Dynamic,
}

#[derive(Debug, Clone)]
pub struct HydrationTemplate {
    pub entity_type: String,
    /// Alias from the base query (e.g. "u", "p"). Used to correlate hydration
    /// results back to the base query's `_gkg_{alias}_id` / `_gkg_{alias}_type` columns.
    pub node_alias: String,
    /// Base JSON for the hydration query (without node_ids).
    /// Call `with_ids` to produce the final query JSON for execution.
    pub query_json: String,
}

impl HydrationTemplate {
    /// Produce a complete query JSON with the given entity IDs injected.
    pub fn with_ids(&self, ids: &[i64]) -> String {
        let mut value: serde_json::Value =
            serde_json::from_str(&self.query_json).expect("template is valid JSON");
        value["node"]["node_ids"] = serde_json::json!(ids);
        value.to_string()
    }
}

/// Display inlines parameters into SQL for debugging/testing.
///
/// Replaces `{name:Type}` placeholders with literal values.
/// **Not for production use** — use parameterized queries to prevent injection.
#[cfg(test)]
impl std::fmt::Display for ParameterizedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use regex::Regex;

        let re = Regex::new(r"\{(\w+):\w+\}").expect("valid regex");
        let result = re.replace_all(&self.sql, |caps: &regex::Captures| {
            let name = &caps[1];
            match self.params.get(name).map(|p| &p.value) {
                Some(Value::String(s)) => format!("'{}'", s.replace('\'', "''")),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Null) => "NULL".to_string(),
                Some(v) => format!("'{}'", v.to_string().replace('\'', "''")),
                None => caps[0].to_string(),
            }
        });
        write!(f, "{}", result)
    }
}

/// Generate a `ParameterizedQuery` from a finalized llqm `Plan` and a `ResultContext`.
pub fn codegen(
    plan: &llqm::plan::Plan,
    result_context: ResultContext,
) -> Result<ParameterizedQuery> {
    let llqm_pq = llqm::codegen::emit_clickhouse_sql(plan)
        .map_err(|e| QueryError::Codegen(format!("llqm codegen failed: {e}")))?;

    // Convert llqm ParamValues to query-engine ParamValues
    let params = llqm_pq
        .params
        .into_iter()
        .map(|(name, pv)| {
            (
                name,
                ParamValue {
                    ch_type: pv.ch_type,
                    value: pv.value,
                },
            )
        })
        .collect();

    Ok(ParameterizedQuery {
        sql: llqm_pq.sql,
        params,
        result_context,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::expr::{self, DataType};
    use llqm::plan::PlanBuilder;

    fn empty_ctx() -> ResultContext {
        ResultContext::new()
    }

    #[test]
    fn simple_select() {
        let mut b = PlanBuilder::new();
        let rel = b.read(
            "nodes",
            "n",
            &[("id", DataType::Int64), ("label", DataType::String)],
        );
        let rel = b.filter(rel, expr::eq(expr::col("n", "label"), expr::string("User")));
        let rel = b.project(
            rel,
            &[
                (expr::col("n", "id"), "node_id"),
                (expr::col("n", "label"), "node_type"),
            ],
        );
        let rel = b.fetch(rel, 10, None);
        let plan = b.build(rel);

        let result = codegen(&plan, empty_ctx()).unwrap();
        assert!(result.sql.contains("SELECT"), "sql: {}", result.sql);
        assert!(result.sql.contains("nodes AS n"), "sql: {}", result.sql);
        assert!(result.sql.contains("LIMIT 10"), "sql: {}", result.sql);
        assert!(result.sql.contains("WHERE"), "sql: {}", result.sql);
    }

    #[test]
    fn with_join() {
        let mut b = PlanBuilder::new();
        let n = b.read("nodes", "n", &[("id", DataType::Int64)]);
        let e = b.read(
            "edges",
            "e",
            &[("source_id", DataType::Int64), ("label", DataType::String)],
        );
        let joined = b.join(
            llqm::expr::JoinType::Inner,
            n,
            e,
            expr::eq(expr::col("n", "id"), expr::col("e", "source_id")),
        );
        let rel = b.project(
            joined,
            &[
                (expr::col("n", "id"), "node_id"),
                (expr::col("e", "label"), "rel_type"),
            ],
        );
        let plan = b.build(rel);

        let result = codegen(&plan, empty_ctx()).unwrap();
        assert!(result.sql.contains("INNER JOIN"), "sql: {}", result.sql);
    }

    #[test]
    fn result_context_preserved() {
        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");

        let mut b = PlanBuilder::new();
        let rel = b.read("nodes", "n", &[("id", DataType::Int64)]);
        let rel = b.project(rel, &[(expr::col("n", "id"), "id")]);
        let plan = b.build(rel);

        let result = codegen(&plan, ctx).unwrap();
        assert_eq!(result.result_context.len(), 1);
        assert_eq!(result.result_context.get("u").unwrap().entity_type, "User");
    }

    #[test]
    fn offset_clause() {
        let mut b = PlanBuilder::new();
        let rel = b.read("nodes", "n", &[("id", DataType::Int64)]);
        let rel = b.project(rel, &[(expr::col("n", "id"), "id")]);
        let rel = b.fetch(rel, 10, Some(40));
        let plan = b.build(rel);

        let result = codegen(&plan, empty_ctx()).unwrap();
        assert!(result.sql.contains("LIMIT 10"), "sql: {}", result.sql);
        assert!(result.sql.contains("OFFSET 40"), "sql: {}", result.sql);
    }
}
