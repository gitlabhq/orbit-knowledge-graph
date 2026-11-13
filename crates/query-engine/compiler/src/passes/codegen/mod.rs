//! Renders the finished relational plan to SQL and parameters through the
//! backend catalog.

pub mod ddl;

use std::sync::Arc;

use orbit_server_config::QueryConfig;
use query_data_model::implementations::duckdb::render::type_name;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    RelationalError, RelationalPlan, SharePolicy,
};
use query_data_model::implementations::relational::render::ParameterizedSql;
use query_data_model::{Catalog, ClickHouse, DuckDb};

use crate::analytics::QueryStats;
use crate::error::{QueryError, Result};
use crate::passes::hydrate::HydrationPlan;
use crate::passes::result_contract::{ResultContext, ResultShape};
use crate::types::QueryType;
pub use orbit_utils::query_types::ParamValue;
use orbit_utils::query_types::SqlType;
use std::collections::HashMap;
use std::sync::LazyLock;

pub type ClickHousePlan = RelationalPlan<ClickHouse>;
/// A relational backend the compiler plans for and renders SQL through.
pub trait SqlBackend:
    RelationalBackend + crate::dag::StageBackend<Catalog = Catalog<Self>> + Send + Sync
{
    const DIALECT: SqlDialect;

    fn render(catalog: &Catalog<Self>, plan: &RelationalPlan<Self>) -> ParameterizedSql;

    /// The connection a catalog of this backend serves, as a stage peer.
    fn peer(catalog: Arc<Catalog<Self>>) -> crate::dag::Peer;
}

impl SqlBackend for ClickHouse {
    const DIALECT: SqlDialect = SqlDialect::ClickHouse;

    fn render(catalog: &Catalog<Self>, plan: &ClickHousePlan) -> ParameterizedSql {
        catalog.render(plan)
    }

    fn peer(catalog: Arc<Catalog<Self>>) -> crate::dag::Peer {
        crate::dag::Peer::ClickHouse(catalog)
    }
}

impl SqlBackend for DuckDb {
    const DIALECT: SqlDialect = SqlDialect::DuckDb;

    fn render(catalog: &Catalog<Self>, plan: &RelationalPlan<DuckDb>) -> ParameterizedSql {
        catalog.render(plan)
    }

    fn peer(catalog: Arc<Catalog<Self>>) -> crate::dag::Peer {
        crate::dag::Peer::DuckDb(catalog)
    }
}

/// Shares read more than once become `_orbit_relation_N` bodies; the rest inline into their reader.
pub fn materialize<B: SqlBackend>(
    plan: RelationalPlan<B>,
    model: &Catalog<B>,
) -> Result<RelationalPlan<B>> {
    let prefix = format!("{}relation_", crate::constants::internal_column_prefix());
    let policy = SharePolicy::of::<B>(prefix);
    model.materialize(plan, &policy).map_err(rejected)
}

pub fn configure(
    plan: ClickHousePlan,
    catalog: &Catalog<ClickHouse>,
    query_config: &QueryConfig,
) -> Result<ClickHousePlan> {
    let mut settings = Some(
        query_config
            .to_clickhouse_settings()
            .map_err(QueryError::Codegen)?,
    );
    catalog
        .rebuild(plan, |q, step| {
            if let Some(settings) = settings.take() {
                q.settings(settings)?;
            }
            step.keep(q)
        })
        .map_err(rejected)
}

pub fn render<B: SqlBackend>(
    plan: &RelationalPlan<B>,
    model: &Catalog<B>,
    result_context: ResultContext,
    query_config: QueryConfig,
) -> ParameterizedQuery {
    let rendered = B::render(model, plan);
    let root = plan.plan.root();
    let columns = plan
        .plan
        .outputs(root)
        .map(|output| plan.plan.name(output).unwrap_or_default().to_string())
        .collect();
    ParameterizedQuery {
        sql: rendered.sql,
        params: rendered.parameters,
        columns,
        result_context,
        query_config,
        dialect: B::DIALECT,
    }
}

/// A CTE or IN subquery's name, its body as a statement of its own with the CTEs it can read, and its parameters.
pub type SubqueryStatement = (String, String, HashMap<String, ParamValue>);

pub fn subquery_statements(
    plan: &ClickHousePlan,
    catalog: &Catalog<ClickHouse>,
) -> Vec<SubqueryStatement> {
    catalog
        .render_subqueries(plan)
        .into_iter()
        .map(|(name, rendered)| (name, rendered.sql, rendered.parameters))
        .collect()
}

pub(crate) fn rejected(error: RelationalError) -> QueryError {
    error.into()
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
    /// The names of the columns the statement returns, in order.
    pub columns: Vec<String>,
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
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub shape: ResultShape,
    pub stats: QueryStats,
    /// Whether the caller asked for the generated SQL in the response.
    pub debug: bool,
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
