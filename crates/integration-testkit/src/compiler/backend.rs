use std::sync::Arc;

use query_engine::compiler;

use super::{
    Compiled, Expectations, IndexAssertion, Models, Scenario, clickhouse, hydration_input,
};
use crate::scenario::Backend;
use compiler::config::{self, CompilerCtx};

type Pipeline = fn(&Scenario, &Models, &str, &Expectations) -> Compiled;

const PIPELINES: &[(Backend, &str, Pipeline)] = &[
    (Backend::ClickHouse, "json", clickhouse_json),
    (Backend::ClickHouse, "gql", clickhouse_gql),
    (Backend::DuckDb, "json", duckdb_json),
    (Backend::DuckDb, "gql", duckdb_gql),
];

fn security(scenario: &Scenario) -> compiler::SecurityContext {
    crate::query_scenario::build_security(&scenario.security)
}

fn clickhouse_hydration(
    scenario: &Scenario,
    models: &Models,
    language: &str,
    raw: &str,
    observe: &Expectations,
) -> Compiled {
    let Some(setup) = scenario.hydration.as_ref() else {
        return (vec![], None);
    };
    match hydration_input(setup, language, raw, models.remote.as_ref()) {
        Ok((input, options)) => {
            let mut ctx =
                config::ChHydrationCtx::new(security(scenario), Arc::clone(&models.remote));
            ctx.set_input(input);
            ctx.set_hydration_options(options);
            observe.run(ctx, config::ChHydrationCtx::PHASES, |ctx, f| {
                config::run_ch_hydration_observed(ctx, f)
            })
        }
        Err(error) => (vec![format!("{}: {error}", observe.label)], None),
    }
}

fn clickhouse_json(
    scenario: &Scenario,
    models: &Models,
    raw: &str,
    observe: &Expectations,
) -> Compiled {
    let mut ctx = config::ClickhouseJsonDslCtx::new(security(scenario), Arc::clone(&models.remote));
    ctx.set_raw(raw.to_string());
    observe.run(ctx, config::ClickhouseJsonDslCtx::PHASES, |ctx, f| {
        config::run_clickhouse_json_dsl_observed(ctx, f)
    })
}

fn clickhouse_gql(
    scenario: &Scenario,
    models: &Models,
    raw: &str,
    observe: &Expectations,
) -> Compiled {
    let mut ctx = config::ClickhouseGqlCtx::new(security(scenario), Arc::clone(&models.remote));
    ctx.set_raw(raw.to_string());
    observe.run(ctx, config::ClickhouseGqlCtx::PHASES, |ctx, f| {
        config::run_clickhouse_gql_observed(ctx, f)
    })
}

fn duckdb_json(_: &Scenario, models: &Models, raw: &str, observe: &Expectations) -> Compiled {
    let mut ctx = config::DuckdbJsonDslCtx::new(Arc::clone(&models.local));
    ctx.set_raw(raw.to_string());
    observe.run(ctx, config::DuckdbJsonDslCtx::PHASES, |ctx, f| {
        config::run_duckdb_json_dsl_observed(ctx, f)
    })
}

fn duckdb_gql(_: &Scenario, models: &Models, raw: &str, observe: &Expectations) -> Compiled {
    let mut ctx = config::DuckdbGqlCtx::new(Arc::clone(&models.local));
    ctx.set_raw(raw.to_string());
    observe.run(ctx, config::DuckdbGqlCtx::PHASES, |ctx, f| {
        config::run_duckdb_gql_observed(ctx, f)
    })
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
        if self == Self::ClickHouse && scenario.hydration.is_some() {
            return clickhouse_hydration(scenario, models, language, raw, observe);
        }
        match PIPELINES
            .iter()
            .find(|(backend, frontend, _)| *backend == self && *frontend == language)
        {
            Some((_, _, pipeline)) if scenario.hydration.is_none() => {
                pipeline(scenario, models, raw, observe)
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

    pub(super) fn settings(
        self,
        config: &orbit_server_config::QueryConfig,
    ) -> Result<Vec<(String, String)>, String> {
        match self {
            Self::ClickHouse => config
                .to_clickhouse_settings()
                .map_err(|error| error.to_string()),
            Self::DuckDb => Err("DuckDB has no query settings".into()),
        }
    }

    pub(super) fn indexes(self, scenario: &Scenario) -> Vec<&IndexAssertion> {
        scenario
            .physical
            .get(&self)
            .into_iter()
            .flat_map(|phases| phases.values())
            .filter_map(|fields| fields.output.as_ref())
            .flat_map(|output| &output.indexes)
            .collect()
    }

    pub(super) fn record_coverage(
        self,
        scenario: &Scenario,
        ontology: &Arc<ontology::Ontology>,
        coverage: &mut crate::query_scenario::coverage::Coverage,
    ) {
        if self != Self::DuckDb {
            return;
        }
        for (language, raw) in &scenario.query {
            let frontend = match language.as_str() {
                "json" => compiler::Frontend::JsonDsl,
                _ => compiler::Frontend::Gql,
            };
            if let Ok(compiled) = compiler::compile_local(raw, frontend, ontology) {
                coverage.record(&compiled.ast);
            }
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
