use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use duckdb_client::DuckDbClient;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::{CompiledQueryContext, Frontend, compile_local, generate_local_ddl};
use query_engine::pipeline::PipelineError;
use query_engine::shared::PipelineOutput;

use super::coverage::Coverage;
use super::engine::{Engine, run_steps, scenarios_for};
use super::format::Backend;
use super::{pipeline_output, seed_preset};
use crate::scenario::Seed;
use crate::scenario::seed::{group_by_columns, values_insert};

pub fn run_duckdb_dir(root: &str, presets: &str, seed: &str) -> Coverage {
    let root = Path::new(root);
    let ontology = Arc::new(crate::load_unprefixed_ontology());
    let base = seed_preset(Path::new(presets), seed);
    let scenarios = scenarios_for(root, Backend::Duckdb);
    assert!(
        !scenarios.is_empty(),
        "no duckdb scenarios under {}",
        root.display()
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("DuckDB scenario runtime");
    let coverage = RefCell::new(Coverage::default());
    for (name, scenario) in scenarios {
        let ontology = scenario.config.ontology_overlay.as_deref().map_or_else(
            || Arc::clone(&ontology),
            |name| Arc::new(crate::load_ontology_overlay(name)),
        );
        let directory = tempfile::tempdir().expect("temporary DuckDB directory");
        let database = DuckDbClient::open(&directory.path().join("scenario.duckdb"))
            .unwrap_or_else(|e| panic!("{name}: open DuckDB: {e}"));
        database
            .initialize_schema(&generate_local_ddl(&ontology, ""))
            .unwrap_or_else(|e| panic!("{name}: local DDL: {e}"));
        let (config, steps) = scenario.into_steps(&name);
        let preset = config
            .seed
            .as_ref()
            .map(|preset| seed_preset(Path::new(presets), preset));
        let engine = DuckdbEngine {
            database,
            ontology,
            coverage: &coverage,
        };
        runtime.block_on(async {
            engine.seed(preset.as_ref().unwrap_or(&base), &name).await;
            engine.seed(&config.extra_seed, &name).await;
            run_steps(&engine, &steps, &name).await;
        });
    }
    coverage.into_inner()
}

struct DuckdbEngine<'a> {
    database: DuckDbClient,
    ontology: Arc<ontology::Ontology>,
    coverage: &'a RefCell<Coverage>,
}

impl Engine for DuckdbEngine<'_> {
    fn backend(&self) -> Backend {
        Backend::Duckdb
    }

    async fn seed(&self, seed: &Seed, name: &str) {
        seed_local(&self.database, seed, name);
    }

    async fn compile(
        &self,
        query: &str,
        frontend: Frontend,
    ) -> Result<Arc<CompiledQueryContext>, PipelineError> {
        compile_local(query, frontend, &self.ontology)
            .map(Arc::new)
            .map_err(PipelineError::from)
    }

    async fn execute(
        &self,
        compiled: &Arc<CompiledQueryContext>,
        _frontend: Frontend,
        label: &str,
    ) -> PipelineOutput {
        self.coverage.borrow_mut().record(&compiled.ast);
        let sql = compiled.base.render();
        let batches = self
            .database
            .query_arrow(&sql)
            .unwrap_or_else(|error| panic!("{label}: {error}\n{sql}"));
        let query_result =
            QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
        pipeline_output(
            compiled,
            query_result,
            0,
            compiled.base.result_context.clone(),
            sql,
        )
    }
}

fn seed_local(database: &DuckDbClient, seed: &Seed, name: &str) {
    let batches = database
        .query_arrow("SELECT table_name, column_name FROM information_schema.columns")
        .unwrap_or_else(|e| panic!("{name}: local columns: {e}"));
    let mut columns: HashMap<String, HashSet<String>> = HashMap::new();
    for (table, column) in duckdb_client::string_column(&batches, "table_name")
        .into_iter()
        .zip(duckdb_client::string_column(&batches, "column_name"))
    {
        columns.entry(table).or_default().insert(column);
    }
    for (table, rows) in seed {
        let Some(local) = columns.get(table) else {
            continue;
        };
        let rows = rows.iter().map(|row| {
            row.iter()
                .filter(|(column, _)| local.contains(*column))
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect()
        });
        for (column_names, group) in group_by_columns(rows) {
            let quoted: Vec<String> = column_names.iter().map(|c| format!("\"{c}\"")).collect();
            database
                .execute(
                    &values_insert(table, &quoted, &column_names, &group, name),
                    &[],
                )
                .unwrap_or_else(|e| panic!("{name}: seed {table}: {e}"));
        }
    }
}
