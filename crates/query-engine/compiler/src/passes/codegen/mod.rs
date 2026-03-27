//! Codegen: AST → SQL
//!
//! Backend-specific code generators translate the shared AST into parameterized
//! SQL for a target database. Each backend lives in its own submodule and
//! exposes a single `codegen()` entry point with the same signature.

pub mod clickhouse;
pub mod duckdb;

use crate::input::{Input, QueryType};
use crate::passes::enforce::ResultContext;
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
}

#[derive(Debug, Clone)]
pub struct CompiledQueryContext {
    pub query_type: QueryType,
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub input: Input,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HydrationPlan {
    /// No hydration needed (e.g., Aggregation).
    None,
    /// Entity types known at compile time (Traversal, Search).
    /// One template per entity type, with IDs to be filled at runtime.
    Static(Vec<HydrationTemplate>),
    /// Entity types discovered at runtime (PathFinding, Neighbors).
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HydrationTemplate {
    pub entity_type: String,
    /// Alias from the base query (e.g. "u", "p"). Used to correlate hydration
    /// results back to the base query's `_gkg_{alias}_id` / `_gkg_{alias}_type` columns.
    pub node_alias: String,
}

impl ParameterizedQuery {
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
