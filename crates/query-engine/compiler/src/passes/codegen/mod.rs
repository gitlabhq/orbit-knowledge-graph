//! Each backend lives in its own submodule and exposes a single `codegen()`
//! entry point with the same signature.

pub mod clickhouse;
pub mod ddl;
pub mod duckdb;

use orbit_server_config::QueryConfig;

use crate::input::{Input, QueryType};
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
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
    pub base: ParameterizedQuery,
    pub hydration: HydrationPlan,
    pub input: Input,
    pub pagination: PaginationContext,
    pub has_virtual_columns: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PaginationContext {
    pub query_hash: u64,
    pub key_count: usize,
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
                for filtered in [false, true] {
                    let ast = Node::Query(Box::new(Query {
                        select: vec![SelectExpr::new(
                            Expr::Aggregate {
                                function,
                                argument: None,
                                distinct,
                                condition: filtered.then(|| Box::new(Expr::col("n", "keep"))),
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
                        for error in [remote.unwrap_err(), local.unwrap_err(), simple.unwrap_err()]
                        {
                            assert!(
                                matches!(error, crate::error::QueryError::Codegen(ref message) if message.contains("requires an argument")),
                                "{error}"
                            );
                        }
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
    fn semantic_aggregates_render_arguments_distinctness_and_conditions() {
        use crate::input::AggFunction;

        for (function, plain, conditional, local) in [
            (AggFunction::Count, "COUNT", "countIf", "COUNT"),
            (AggFunction::Sum, "SUM", "sumIf", "SUM"),
            (AggFunction::Avg, "AVG", "avgIf", "AVG"),
            (AggFunction::Min, "MIN", "minIf", "MIN"),
            (AggFunction::Max, "MAX", "maxIf", "MAX"),
            (
                AggFunction::Collect,
                "groupArray",
                "groupArrayIf",
                "array_agg",
            ),
        ] {
            for filtered in [false, true] {
                for distinct in [false, true] {
                    let ast = Node::Query(Box::new(Query {
                        select: vec![SelectExpr::new(
                            Expr::Aggregate {
                                function,
                                argument: Some(Box::new(Expr::col("n", "value"))),
                                distinct,
                                condition: filtered.then(|| Box::new(Expr::col("n", "keep"))),
                            },
                            "result",
                        )],
                        from: TableRef::scan("nodes", "n"),
                        ..Default::default()
                    }));
                    let remote =
                        codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();
                    let local_query = duckdb::codegen(&ast, ResultContext::new()).unwrap();
                    let name = if distinct {
                        format!(
                            "{}Distinct{}",
                            conditional.strip_suffix("If").unwrap(),
                            if filtered { "If" } else { "" }
                        )
                    } else if filtered {
                        conditional.into()
                    } else {
                        plain.into()
                    };
                    assert!(
                        remote.sql.starts_with(&format!(
                            "SELECT {name}(n.value{}) AS result",
                            if filtered { ", n.keep" } else { "" }
                        )),
                        "{}",
                        remote.sql
                    );
                    let aggregate = format!(
                        "SELECT {local}({}n.value){} AS result FROM nodes AS n",
                        if distinct { "DISTINCT " } else { "" },
                        if filtered {
                            " FILTER (WHERE n.keep)"
                        } else {
                            ""
                        }
                    );
                    if function == AggFunction::Collect {
                        assert!(
                            local_query.sql.contains("coalesce(array_agg("),
                            "{}",
                            local_query.sql
                        );
                        assert!(local_query.sql.contains("n.value IS NOT NULL"));
                        assert_eq!(local_query.sql.contains("n.keep"), filtered);
                        assert_eq!(local_query.sql.contains("DISTINCT"), distinct);
                    } else {
                        assert_eq!(local_query.sql, aggregate);
                    }
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
