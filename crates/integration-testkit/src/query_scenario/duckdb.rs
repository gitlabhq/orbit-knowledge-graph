use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use duckdb_client::DuckDbClient;
use query_data_model::DuckDbDataModel;
use query_engine::compiler::{Frontend, generate_local_ddl};
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{
    CompilationStage, ExecutionStage, ExtractionStage, OutputStage, PipelineOutput,
};

use super::coverage::Coverage;
use super::engine::{Engine, Planned, run_all, run_steps};
use super::seed_preset;
use super::stages::{LocalExecutor, LocalFinishStage};
use crate::scenario::seed::{group_by_columns, values_insert};
use crate::scenario::{Backend, Seed};

pub fn run_duckdb_dir(root: &str, presets: &str, seed: &str) -> Coverage {
    let presets = Path::new(presets);
    let env = DuckdbEnv {
        ontology: Arc::new(crate::load_unprefixed_ontology()),
        base: seed_preset(presets, seed),
        coverage: Mutex::new(Coverage::default()),
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("DuckDB scenario runtime")
        .block_on(run_all::<DuckdbEngine>(Path::new(root), presets, |_| env))
        .coverage
        .into_inner()
        .unwrap()
}

struct DuckdbEnv {
    ontology: Arc<ontology::Ontology>,
    base: Seed,
    coverage: Mutex<Coverage>,
}

struct DuckdbEngine<'a> {
    database: Mutex<DuckDbClient>,
    ontology: Arc<ontology::Ontology>,
    data_model: Arc<DuckDbDataModel>,
    coverage: &'a Mutex<Coverage>,
}

impl Engine for DuckdbEngine<'_> {
    const BACKEND: Backend = Backend::DuckDb;

    type Env = DuckdbEnv;

    async fn seed(&self, seed: &Seed, name: &str) {
        seed_local(&self.database.lock().unwrap(), seed, name);
    }

    async fn run(&self, query: &str, frontend: Frontend) -> Result<PipelineOutput, PipelineError> {
        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.data_model));
        let mut ctx = QueryPipelineContext {
            frontend,
            query_json: query.to_owned(),
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: None,
            server_extensions,
            phases: TypeMap::default(),
        };
        let mut obs = NoOpObserver;
        PipelineRunner::start(&mut ctx, &mut obs)
            .then(&CompilationStage::<DuckDbDataModel>::new())
            .await?
            .then(&ExecutionStage(LocalExecutor {
                database: &self.database,
                coverage: self.coverage,
            }))
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&LocalFinishStage)
            .await?
            .then(&OutputStage)
            .await?
            .finish()
            .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))
    }

    async fn run_scenario(env: &DuckdbEnv, plan: Planned, presets: &Path) {
        let Planned {
            name,
            config,
            steps,
        } = plan;
        let ontology = config.ontology_overlay.as_deref().map_or_else(
            || Arc::clone(&env.ontology),
            |name| Arc::new(crate::load_ontology_overlay(name)),
        );
        let directory = tempfile::tempdir().expect("temporary DuckDB directory");
        let database = DuckDbClient::open(&directory.path().join("scenario.duckdb"))
            .unwrap_or_else(|e| panic!("{name}: open DuckDB: {e}"));
        database
            .initialize_schema(&generate_local_ddl(&ontology, ""))
            .unwrap_or_else(|e| panic!("{name}: local DDL: {e}"));
        let preset = config
            .seed
            .as_ref()
            .map(|preset| seed_preset(presets, preset));
        let engine = DuckdbEngine {
            database: Mutex::new(database),
            data_model: query_engine::compiler::local_data_model(&ontology)
                .unwrap_or_else(|e| panic!("{name}: local data model: {e}")),
            ontology,
            coverage: &env.coverage,
        };
        engine
            .seed(preset.as_ref().unwrap_or(&env.base), &name)
            .await;
        engine.seed(&config.extra_seed, &name).await;
        run_steps(&engine, &steps, &name).await;
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
