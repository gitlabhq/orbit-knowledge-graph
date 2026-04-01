//! Codegen: AST → SQL
//!
//! Backend-specific code generators translate the shared AST into parameterized
//! SQL for a target database. Each backend lives in its own submodule and
//! exposes a single `codegen()` entry point with the same signature.

pub mod clickhouse;
pub mod duckdb;

use crate::input::{Input, QueryType};
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
pub use gkg_utils::clickhouse::ParamValue;
use std::collections::HashMap;

// Re-export the ClickHouse backend as the default `codegen` function so
// existing call-sites (`codegen::codegen(...)`) keep working unchanged.
pub use clickhouse::codegen;

/// Which SQL dialect a [`ParameterizedQuery`] was generated for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialect {
    #[default]
    ClickHouse,
    DuckDb,
}

#[derive(Debug, Clone)]
pub struct ParameterizedQuery {
    pub sql: String,
    pub params: HashMap<String, ParamValue>,
    pub result_context: ResultContext,
    pub dialect: SqlDialect,
    /// Query-level settings applied during execution (HTTP options + SQL SETTINGS).
    pub query_config: gkg_config::QueryConfig,
}

#[derive(Debug, Clone)]
pub struct CompiledQueryContext {
    pub query_type: QueryType,
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub input: Input,
}

impl ParameterizedQuery {
    /// Returns parameter values in positional order (`$1`, `$2`, ...) for DuckDB execution.
    ///
    /// Keys follow the `pN` convention from DuckDB codegen, where N matches `$N` in the SQL.
    pub fn params_in_order(&self) -> Vec<&ParamValue> {
        let mut entries: Vec<_> = self.params.iter().collect();
        entries.sort_by_key(|(k, _)| {
            k.strip_prefix('p')
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(usize::MAX)
        });
        entries.into_iter().map(|(_, v)| v).collect()
    }

    /// Render SQL with parameters inlined for debugging/observability.
    ///
    /// Dispatches on [`SqlDialect`]:
    /// - **ClickHouse:** replaces `{name:Type}` placeholders.
    /// - **DuckDB:** replaces `$N` positional placeholders.
    ///
    /// **Not for execution** — use parameterized queries to prevent injection.
    pub fn render(&self) -> String {
        match self.dialect {
            SqlDialect::ClickHouse => self.render_clickhouse(),
            SqlDialect::DuckDb => self.render_duckdb(),
        }
    }

    fn render_clickhouse(&self) -> String {
        use regex::Regex;

        let re = Regex::new(r"\{(\w+):[^}]+\}").expect("valid regex");
        re.replace_all(&self.sql, |caps: &regex::Captures| {
            let name = &caps[1];
            match self.params.get(name) {
                Some(param) => param.render_literal(),
                None => caps[0].to_string(),
            }
        })
        .into_owned()
    }

    fn render_duckdb(&self) -> String {
        use regex::Regex;

        let re = Regex::new(r"\$(\d+)").expect("valid regex");
        re.replace_all(&self.sql, |caps: &regex::Captures| {
            let name = format!("p{}", &caps[1]);
            match self.params.get(&name) {
                Some(param) => param.render_literal(),
                None => caps[0].to_string(),
            }
        })
        .into_owned()
    }
}

impl std::fmt::Display for ParameterizedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.render())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gkg_utils::clickhouse::ChType;
    use serde_json::Value;

    fn make_param(ch_type: ChType, value: Value) -> ParamValue {
        ParamValue { ch_type, value }
    }

    fn make_query(params: HashMap<String, ParamValue>) -> ParameterizedQuery {
        ParameterizedQuery {
            sql: String::new(),
            params,
            result_context: ResultContext::new(),
            dialect: SqlDialect::DuckDb,
            query_config: gkg_config::QueryConfig::default(),
        }
    }

    #[test]
    fn params_in_order_sorts_by_positional_index() {
        let params = HashMap::from([
            (
                "p3".into(),
                make_param(ChType::String, Value::String("c".into())),
            ),
            (
                "p1".into(),
                make_param(ChType::String, Value::String("a".into())),
            ),
            (
                "p2".into(),
                make_param(ChType::Int64, Value::Number(42.into())),
            ),
        ]);
        let query = make_query(params);
        let ordered = query.params_in_order();

        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].value, Value::String("a".into()));
        assert_eq!(ordered[1].value, Value::Number(42.into()));
        assert_eq!(ordered[2].value, Value::String("c".into()));
    }

    #[test]
    fn params_in_order_empty() {
        let query = make_query(HashMap::new());
        assert!(query.params_in_order().is_empty());
    }

    #[test]
    fn params_in_order_single() {
        let params = HashMap::from([("p1".into(), make_param(ChType::Bool, Value::Bool(true)))]);
        let query = make_query(params);
        let ordered = query.params_in_order();

        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].value, Value::Bool(true));
    }

    #[test]
    fn params_in_order_many_params() {
        let params: HashMap<String, ParamValue> = (1..=10)
            .map(|i| {
                (
                    format!("p{i}"),
                    make_param(ChType::Int64, Value::Number(i.into())),
                )
            })
            .collect();
        let query = make_query(params);
        let ordered = query.params_in_order();

        assert_eq!(ordered.len(), 10);
        for (i, param) in ordered.iter().enumerate() {
            assert_eq!(param.value, Value::Number((i as i64 + 1).into()));
        }
    }
}
