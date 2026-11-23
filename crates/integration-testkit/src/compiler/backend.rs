use std::sync::Arc;

use query_data_model::{ClickHouseDataModel, DuckDbDataModel};
use query_engine::compiler;

use super::{
    Compiled, Expectations, IndexAssertion, Models, Scenario, clickhouse, hydration_input,
};
use crate::scenario::Backend;
use compiler::config::{self, CompilerCtx, PhaseObserver, Pipeline};
use compiler::{CompileSource, CompileTarget, Frontend};

pub(super) struct Observer<'a, 'b> {
    expectations: &'a Expectations<'b>,
    failures: Vec<String>,
    output: Option<compiler::CompiledQueryContext>,
}

impl<'a, 'b> Observer<'a, 'b> {
    pub(super) fn new(expectations: &'a Expectations<'b>) -> Self {
        Self {
            expectations,
            failures: Vec::new(),
            output: None,
        }
    }

    pub(super) fn into_compiled(self) -> Compiled {
        (self.failures, self.output)
    }
}

impl PhaseObserver for Observer<'_, '_> {
    fn start<C: Pipeline>(&mut self) -> bool {
        self.failures = self.expectations.unknown_targets(C::PHASES);
        self.failures.is_empty()
    }

    fn phase<C: Pipeline>(&mut self, phase: &'static str, ctx: &C) {
        self.failures
            .extend(self.expectations.check_phase(phase, ctx));
    }

    fn finished<C: Pipeline>(&mut self, result: &compiler::Result<()>, ctx: &mut C) {
        let expectations = self.expectations;
        match (result, expectations.rejection) {
            (Ok(()), Some(_)) => self.failures.push(format!(
                "{}: expected a rejection, but the query compiled",
                expectations.label
            )),
            (Err(error), Some(rejection)) => self.failures.extend(rejection.check(
                expectations.frontend,
                ctx.current_phase(),
                error,
                expectations.label,
            )),
            (Err(error), None) => self
                .failures
                .push(format!("{}: {error}", expectations.label)),
            (Ok(()), None) => {
                if let Some(output) = ctx.output() {
                    self.failures.extend(expectations.backend.check_output(
                        expectations.models,
                        output,
                        expectations.label,
                    ));
                }
                self.output = ctx.output().clone();
            }
        }
    }
}

fn security(scenario: &Scenario) -> compiler::SecurityContext {
    crate::query_scenario::build_security(&scenario.security)
}

impl Backend {
    pub(super) fn compile(
        self,
        scenario: &Scenario,
        models: &Models,
        language: &str,
        raw: &str,
        expectations: &Expectations,
    ) -> Compiled {
        let mut observer = Observer::new(expectations);
        let frontend = language.parse::<Frontend>().ok();
        match (self, frontend, scenario.hydration.as_ref()) {
            (Self::ClickHouse, _, Some(setup)) => {
                match hydration_input(setup, language, raw, models.remote.as_ref()) {
                    Ok((input, options)) => {
                        let mut ctx = config::ChHydrationCtx::new(
                            security(scenario),
                            Arc::clone(&models.remote),
                        );
                        ctx.set_input(input);
                        ctx.set_hydration_options(options);
                        let _ = config::run_observed(&mut ctx, &mut observer);
                    }
                    Err(error) => {
                        return (vec![format!("{}: {error}", expectations.label)], None);
                    }
                }
            }
            (Self::ClickHouse, Some(frontend), None) => {
                let _ = ClickHouseDataModel::compile_observed(
                    CompileSource::Raw(raw),
                    frontend,
                    &models.remote,
                    Some(&security(scenario)),
                    &mut observer,
                );
            }
            (Self::DuckDb, Some(frontend), None) => {
                let _ = DuckDbDataModel::compile_observed(
                    CompileSource::Raw(raw),
                    frontend,
                    &models.local,
                    None,
                    &mut observer,
                );
            }
            _ => {
                return (
                    vec![format!(
                        "{}: no {self} pipeline for this fixture",
                        expectations.label
                    )],
                    None,
                );
            }
        }
        observer.into_compiled()
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
