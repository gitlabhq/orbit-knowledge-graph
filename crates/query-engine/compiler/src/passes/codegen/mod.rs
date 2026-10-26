//! Each backend lives in its own submodule and exposes a single `codegen()`
//! entry point with the same signature.

pub mod clickhouse;
pub mod ddl;
pub mod duckdb;

use orbit_server_config::QueryConfig;

use crate::ast::Node;
use crate::input::{Input, QueryType};
use crate::passes::hydrate::HydrationPlan;
use crate::passes::result_contract::ResultContext;
pub use orbit_utils::query_types::ParamValue;
use std::collections::HashMap;
use std::sync::LazyLock;

pub use clickhouse::codegen;

fn validate_aggregate(
    function: crate::input::AggFunction,
    argument: Option<&crate::ast::Expr>,
    distinct: bool,
) -> Result<(), String> {
    if argument.is_none() && (distinct || function != crate::input::AggFunction::Count) {
        let qualifier = if distinct { "distinct " } else { "" };
        return Err(format!("{qualifier}{function} requires an argument"));
    }
    Ok(())
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
                            Some(param) => duckdb::render_literal(param),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cte, Expr, Node, Query, SelectExpr, TableRef};

    #[test]
    fn aggregate_codegen_rejects_missing_arguments_except_row_count() {
        use crate::input::AggFunction;

        for function in [
            AggFunction::Count,
            AggFunction::Sum,
            AggFunction::Avg,
            AggFunction::Min,
            AggFunction::Max,
            AggFunction::Collect,
        ] {
            for distinct in [false, true] {
                let ast = Node::Query(Box::new(Query {
                    select: vec![SelectExpr::new(
                        Expr::Aggregate {
                            function,
                            argument: None,
                            distinct,
                        },
                        "result",
                    )],
                    from: TableRef::scan("nodes", "n"),
                    ..Default::default()
                }));
                let remote = codegen(&ast, ResultContext::new(), QueryConfig::default());
                let local = duckdb::codegen(&ast, ResultContext::new());
                let simple = clickhouse::emit_simple_query(&ast);
                if function == AggFunction::Count && !distinct {
                    assert!(remote.is_ok());
                    assert!(local.is_ok());
                    assert!(simple.is_ok());
                } else {
                    for error in [remote.unwrap_err(), local.unwrap_err(), simple.unwrap_err()] {
                        assert!(
                            matches!(error, crate::error::QueryError::Codegen(ref message) if message.contains("requires an argument")),
                            "{error}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn token_search_uses_clickhouse_modes_and_rejects_duckdb() {
        use crate::ast::TextMatch;
        for (mode, function) in [
            (TextMatch::TokenMatch, "hasToken"),
            (TextMatch::AllTokens, "hasAllTokens"),
            (TextMatch::AnyTokens, "hasAnyTokens"),
        ] {
            let ast = Node::Query(Box::new(Query {
                select: vec![SelectExpr::col("n", "id")],
                from: TableRef::scan("nodes", "n"),
                where_clause: Some(Expr::TextSearch {
                    mode,
                    value: Box::new(Expr::col("n", "text")),
                    query: Box::new(Expr::string("graph query")),
                }),
                ..Default::default()
            }));
            let remote = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();
            assert!(
                remote
                    .render()
                    .contains(&format!("{function}(n.text, 'graph query')"))
            );
            let error = duckdb::codegen(&ast, ResultContext::new()).unwrap_err();
            assert!(error.to_string().contains("token search is not supported"));
        }
    }

    #[test]
    fn semantic_aggregates_render_arguments_and_distinctness() {
        use crate::input::AggFunction;

        for (function, plain, distinct_name, local) in [
            (AggFunction::Count, "COUNT", "countDistinct", "COUNT"),
            (AggFunction::Sum, "SUM", "sumDistinct", "SUM"),
            (AggFunction::Avg, "AVG", "avgDistinct", "AVG"),
            (AggFunction::Min, "MIN", "minDistinct", "MIN"),
            (AggFunction::Max, "MAX", "maxDistinct", "MAX"),
            (
                AggFunction::Collect,
                "groupArray",
                "groupArrayDistinct",
                "array_agg",
            ),
        ] {
            for distinct in [false, true] {
                let ast = Node::Query(Box::new(Query {
                    select: vec![SelectExpr::new(
                        Expr::Aggregate {
                            function,
                            argument: Some(Box::new(Expr::col("n", "value"))),
                            distinct,
                        },
                        "result",
                    )],
                    from: TableRef::scan("nodes", "n"),
                    ..Default::default()
                }));
                let remote = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();
                let local_query = duckdb::codegen(&ast, ResultContext::new()).unwrap();
                let name = if distinct { distinct_name } else { plain };
                assert!(
                    remote
                        .sql
                        .starts_with(&format!("SELECT {name}(n.value) AS result")),
                    "{}",
                    remote.sql
                );
                if function == AggFunction::Collect {
                    assert!(
                        local_query.sql.contains("coalesce(array_agg("),
                        "{}",
                        local_query.sql
                    );
                    assert!(local_query.sql.contains("n.value IS NOT NULL"));
                    assert_eq!(local_query.sql.contains("DISTINCT"), distinct);
                } else {
                    assert_eq!(
                        local_query.sql,
                        format!(
                            "SELECT {local}({}n.value) AS result FROM nodes AS n",
                            if distinct { "DISTINCT " } else { "" }
                        )
                    );
                }
            }
        }
    }

    #[test]
    fn nested_cte_definitions_survive_both_renderers() {
        for recursive in [false, true] {
            let seed = Query {
                select: vec![SelectExpr::new(Expr::int(7), "id")],
                from: TableRef::scan("system.one", "one"),
                ..Default::default()
            };
            let body = Query {
                ctes: vec![Cte::new("seed", seed)],
                select: vec![SelectExpr::col("s", "id")],
                from: TableRef::scan("seed", "s"),
                limit: Some(1),
                ..Default::default()
            };
            let mut outer = Cte::new("result", body);
            outer.recursive = recursive;
            let ast = Node::Query(Box::new(Query {
                ctes: vec![outer],
                select: vec![SelectExpr::col("r", "id")],
                from: TableRef::scan("result", "r"),
                ..Default::default()
            }));
            let remote = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();
            let local = duckdb::codegen(&ast, ResultContext::new()).unwrap();
            for sql in [&remote.sql, &local.sql] {
                assert!(sql.contains("result AS (WITH seed AS (SELECT"), "{sql}");
                assert_eq!(sql.starts_with("WITH RECURSIVE"), recursive, "{sql}");
                assert!(sql.contains("FROM seed AS s"), "{sql}");
            }
            assert!(remote.sql.contains("LIMIT 1"));
            assert_eq!(local.sql.contains("LIMIT 1"), !recursive);
        }
    }
}
