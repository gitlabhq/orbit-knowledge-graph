//! Imports the lowered AST into the relational plan and renders it through
//! the backend catalog.

pub mod ddl;
mod import;

use orbit_server_config::QueryConfig;
use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::implementations::duckdb::render::type_name;
use query_data_model::implementations::relational::plan::RelationalError;

use crate::ast::{Node, Query, SqlType};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::hydrate::HydrationPlan;
use crate::passes::result_contract::ResultContext;
pub use orbit_utils::query_types::ParamValue;
use std::collections::HashMap;
use std::sync::LazyLock;

pub fn codegen(
    ast: &Node,
    catalog: &Catalog<ClickHouse>,
    result_context: ResultContext,
    query_config: QueryConfig,
) -> Result<ParameterizedQuery> {
    let settings = query_config
        .to_clickhouse_settings()
        .map_err(QueryError::Codegen)?;
    let rendered = import::clickhouse(ast, catalog, settings).map_err(rejected)?;
    Ok(ParameterizedQuery {
        sql: rendered.sql,
        params: rendered.parameters,
        result_context,
        query_config,
        dialect: SqlDialect::ClickHouse,
    })
}

pub fn duckdb_codegen(
    ast: &Node,
    catalog: &Catalog<DuckDb>,
    result_context: ResultContext,
) -> Result<ParameterizedQuery> {
    let rendered = import::duckdb(ast, catalog).map_err(rejected)?;
    Ok(ParameterizedQuery {
        sql: rendered.sql,
        params: rendered.parameters,
        result_context,
        query_config: QueryConfig::default(),
        dialect: SqlDialect::DuckDb,
    })
}

/// A CTE's name, its body as a statement of its own with the CTEs before it attached, and its parameters.
pub type CteStatement = (String, String, HashMap<String, ParamValue>);

/// Each CTE body as a statement tests can EXPLAIN.
pub fn cte_statements(ast: &Node, catalog: &Catalog<ClickHouse>) -> Result<Vec<CteStatement>> {
    let Node::Query(query) = ast else {
        return Ok(Vec::new());
    };
    query
        .ctes
        .iter()
        .map(|cte| {
            let statement = Node::Query(Box::new(Query {
                ctes: query
                    .ctes
                    .iter()
                    .take_while(|other| other.name != cte.name)
                    .cloned()
                    .collect(),
                ..(*cte.query).clone()
            }));
            let rendered = import::clickhouse(&statement, catalog, Vec::new()).map_err(rejected)?;
            Ok((cte.name.clone(), rendered.sql, rendered.parameters))
        })
        .collect()
}

fn rejected(error: RelationalError) -> QueryError {
    QueryError::Codegen(error.to_string())
}

fn duckdb_literal(parameter: &ParamValue) -> String {
    let literal = orbit_utils::clickhouse::render_value(&parameter.value);
    match parameter.data_type {
        SqlType::Date | SqlType::Timestamp { .. } => {
            format!("CAST({literal} AS {})", type_name(parameter.data_type))
        }
        _ => literal,
    }
}

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
    /// Resolved query settings. Baked into the SQL SETTINGS clause by codegen
    /// and also applied as HTTP-level ClickHouse settings by the execution
    /// stage (defense-in-depth).
    pub query_config: QueryConfig,
    pub dialect: SqlDialect,
}

#[derive(Debug, Clone)]
pub struct CompiledQueryContext {
    pub query_type: QueryType,
    pub ast: Node,
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub input: Input,
    pub has_virtual_columns: bool,
}

impl ParameterizedQuery {
    /// **Not for execution** — inlines params into SQL; use parameterized
    /// queries to prevent injection.
    pub fn render(&self) -> String {
        match self.dialect {
            SqlDialect::ClickHouse => {
                static CH_RE: LazyLock<regex::Regex> =
                    LazyLock::new(|| regex::Regex::new(r"\{(\w+):[^}]+\}").expect("valid regex"));
                CH_RE
                    .replace_all(&self.sql, |caps: &regex::Captures| {
                        let name = &caps[1];
                        match self.params.get(name) {
                            Some(param) => param.render_clickhouse_literal(),
                            None => caps[0].to_string(),
                        }
                    })
                    .into_owned()
            }
            SqlDialect::DuckDb => {
                static DUCK_RE: LazyLock<regex::Regex> =
                    LazyLock::new(|| regex::Regex::new(r"\$(\d+)").expect("valid regex"));
                DUCK_RE
                    .replace_all(&self.sql, |caps: &regex::Captures| {
                        let key = format!("p{}", &caps[1]);
                        match self.params.get(&key) {
                            Some(param) => duckdb_literal(param),
                            None => caps[0].to_string(),
                        }
                    })
                    .into_owned()
            }
        }
    }
}

impl std::fmt::Display for ParameterizedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.render())
    }
}
