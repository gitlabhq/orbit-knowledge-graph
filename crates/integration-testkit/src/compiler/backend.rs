use std::sync::Arc;

use query_engine::compiler;
use serde::Deserialize;

use super::{Compiled, Expectations, Models, Scenario, clickhouse, hydration_input};
use compiler::config::{self, CompilerCtx};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Backend {
    ClickHouse,
    DuckDb,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ClickHouse => "clickhouse",
            Self::DuckDb => "duckdb",
        })
    }
}

impl Backend {
    pub(super) fn compile(
        self,
        scenario: &Scenario,
        models: &Models,
        language: &str,
        raw: &str,
        observe: &Expectations,
    ) -> Compiled {
        let security = || crate::query_scenario::build_security(&scenario.security);
        let remote = &models.remote;
        match (self, language, scenario.hydration.as_ref()) {
            (Self::ClickHouse, _, Some(setup)) => {
                match hydration_input(setup, language, raw, remote.as_ref()) {
                    Ok((input, options)) => {
                        let mut ctx = config::ChHydrationCtx::new(security(), Arc::clone(remote));
                        ctx.set_input(input);
                        ctx.set_hydration_options(options);
                        observe.run(ctx, config::ChHydrationCtx::PHASES, |ctx, f| {
                            config::run_ch_hydration_observed(ctx, f)
                        })
                    }
                    Err(error) => (vec![format!("{}: {error}", observe.label)], None),
                }
            }
            (Self::ClickHouse, "json", None) => {
                let mut ctx = config::ClickhouseJsonDslCtx::new(security(), Arc::clone(remote));
                ctx.set_raw(raw.to_string());
                observe.run(ctx, config::ClickhouseJsonDslCtx::PHASES, |ctx, f| {
                    config::run_clickhouse_json_dsl_observed(ctx, f)
                })
            }
            (Self::ClickHouse, "gql", None) => {
                let mut ctx = config::ClickhouseGqlCtx::new(security(), Arc::clone(remote));
                ctx.set_raw(raw.to_string());
                observe.run(ctx, config::ClickhouseGqlCtx::PHASES, |ctx, f| {
                    config::run_clickhouse_gql_observed(ctx, f)
                })
            }
            (Self::DuckDb, "json", None) => {
                let mut ctx = config::DuckdbJsonDslCtx::new(Arc::clone(&models.local));
                ctx.set_raw(raw.to_string());
                observe.run(ctx, config::DuckdbJsonDslCtx::PHASES, |ctx, f| {
                    config::run_duckdb_json_dsl_observed(ctx, f)
                })
            }
            (Self::DuckDb, "gql", None) => {
                let mut ctx = config::DuckdbGqlCtx::new(Arc::clone(&models.local));
                ctx.set_raw(raw.to_string());
                observe.run(ctx, config::DuckdbGqlCtx::PHASES, |ctx, f| {
                    config::run_duckdb_gql_observed(ctx, f)
                })
            }
            _ => (
                vec![format!(
                    "{}: no {self} pipeline for this fixture",
                    observe.label
                )],
                None,
            ),
        }
    }

    pub(super) fn check_output(
        self,
        models: &Models,
        output: &compiler::CompiledQueryContext,
        label: &str,
    ) -> Vec<String> {
        let sql = output.base.render();
        match self {
            Self::DuckDb => models
                .local_database
                .query_arrow(&sql)
                .err()
                .map(|error| format!("{label}: DuckDB rejects the SQL: {error}\n{sql}"))
                .into_iter()
                .collect(),
            Self::ClickHouse => match clickhouse::check_cte_reads(&output.ast, &sql) {
                Ok(reads) => {
                    if reads.reads_other {
                        models.reads_other_cte.set(true);
                    }
                    reads
                        .failures
                        .into_iter()
                        .map(|read| format!("{label}: {read}"))
                        .collect()
                }
                Err(error) => vec![format!("{label}: {error}")],
            },
        }
    }

    pub(super) fn index_exists(self, models: &Models, table: &str, index: &str) -> bool {
        match self {
            Self::ClickHouse => models
                .remote
                .layout()
                .schema()
                .table(table)
                .is_some_and(|table| table.options.indexes.iter().any(|item| item.name == index)),
            Self::DuckDb => false,
        }
    }
}
