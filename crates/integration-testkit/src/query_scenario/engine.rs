use std::path::Path;
use std::sync::Arc;

use query_engine::compiler::Frontend;
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::PipelineError;
use query_engine::shared::PipelineOutput;

use super::format::{QueryExpect, QueryScenario, QueryStep, ScenarioConfig};
use super::pagination::{PageCollection, assert_page_bounds, page_query, unpaginated};
use super::{
    assert_compilation, assert_output, assert_pagination, assert_repeated_response, read_scenarios,
    repeat_count, step_label, validate_response,
};
use crate::collect_subtest_results;
use crate::scenario::{self, Backend, Seed};

pub(super) struct Planned {
    pub(super) name: String,
    pub(super) config: ScenarioConfig,
    pub(super) steps: Vec<QueryStep>,
}

pub(super) trait Engine: Sized {
    const BACKEND: Backend;

    type Env: Send + Sync + 'static;

    async fn seed(&self, seed: &Seed, name: &str);

    async fn run(&self, query: &str, frontend: Frontend) -> Result<PipelineOutput, PipelineError>;

    fn run_scenario(
        env: &Self::Env,
        plan: Planned,
        presets: &Path,
    ) -> impl Future<Output = ()> + Send;
}

pub(super) async fn run_all<E: Engine>(
    root: &Path,
    presets: &Path,
    env: impl FnOnce(&[Planned]) -> E::Env,
) -> E::Env {
    let mut scenarios = scenarios_for::<E>(root);
    assert!(
        !scenarios.is_empty(),
        "no {} query scenarios found under {}",
        E::BACKEND,
        root.display()
    );

    if let Ok(filter) = std::env::var("SCENARIO_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            scenarios.retain(|(name, _)| name.contains(filter));
            assert!(
                !scenarios.is_empty(),
                "SCENARIO_FILTER='{filter}' matched no query scenarios under {}",
                root.display()
            );
        }
    }

    scenario::assert_distinct_database_names(scenarios.iter().map(|(name, _)| name.clone()));

    let concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let plans: Vec<Planned> = scenarios
        .into_iter()
        .map(|(name, scenario)| {
            let (config, steps) = scenario.into_steps(&name);
            Planned {
                name,
                config,
                steps,
            }
        })
        .collect();
    let env = Arc::new(env(&plans));
    let presets: Arc<Path> = Arc::from(presets);

    let mut handles = Vec::new();
    for plan in plans {
        let semaphore = Arc::clone(&semaphore);
        let env = Arc::clone(&env);
        let presets = Arc::clone(&presets);
        let name = plan.name.clone();
        let task_name = name.clone();
        let handle = tokio::task::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            E::run_scenario(&env, plan, &presets).await;
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
    Arc::into_inner(env).expect("scenario tasks released the environment")
}

pub(super) fn scenarios_for<E: Engine>(root: &Path) -> Vec<(String, QueryScenario)> {
    let mut files = Vec::new();
    scenario::discover(root, &mut files);
    files.sort();
    files
        .iter()
        .flat_map(|file| read_scenarios(root, file))
        .filter(|(_, scenario)| scenario.config.backends.contains(&E::BACKEND))
        .collect()
}

pub(super) async fn run_steps(engine: &impl Engine, steps: &[QueryStep], name: &str) {
    for (index, step) in steps.iter().enumerate() {
        if !step.seed.is_empty() {
            engine.seed(&step.seed, name).await;
        }
        let name = &step_label(name, index, steps.len());
        for (key, query) in &step.query {
            let frontend = key
                .parse::<Frontend>()
                .unwrap_or_else(|_| panic!("{name}: unknown query language '{key}'"));
            run_frontend(engine, (frontend, key), query, &step.expect, name).await;
        }
    }
}

async fn run_frontend<E: Engine>(
    engine: &E,
    (frontend, key): (Frontend, &str),
    query: &str,
    expect: &QueryExpect,
    name: &str,
) {
    let backend = E::BACKEND.name();
    let label = &format!("{name} [{key} on {backend}]");
    expect.validate_pages_exclusive(label);

    if !expect.pages.is_empty() && expect.compile_error.is_none() {
        run_pages(engine, frontend, query, expect, label).await;
        return;
    }

    let result = engine.run(query, frontend).await;
    let Some(output) = assert_compilation(result, expect, (backend, key), label) else {
        return;
    };

    let baseline = assert_output(&output, expect, label);
    for run in 2..=repeat_count(expect, label) {
        let result = engine.run(query, frontend).await;
        let output = assert_compilation(result, expect, (backend, key), label)
            .expect("repeated query should compile");
        let rerun = assert_output(&output, expect, label);
        assert_repeated_response(&baseline, &rerun, run, label);
    }
}

async fn run_pages(
    engine: &impl Engine,
    frontend: Frontend,
    base_query: &str,
    expect: &QueryExpect,
    label: &str,
) {
    let mut collected = PageCollection::default();

    for (index, page_expect) in expect.pages.iter().enumerate() {
        let page_label = format!("{label} page {}", index + 1);
        let output = engine
            .run(&page_query(frontend, base_query, index), frontend)
            .await
            .unwrap_or_else(|error| panic!("{page_label}: query failed: {error}"));
        let response = assert_output(&output, page_expect, &page_label);
        let response = serde_json::from_value(response).expect("response should deserialize");
        collected.push(&response);
        assert_page_bounds(&response, index, expect.pages.len(), &page_label);
    }

    if let Some(all_pages) = &expect.all_pages {
        collected.assert_expect(all_pages, label);
        if all_pages.matches_unpaginated {
            let output = engine
                .run(&unpaginated(frontend, base_query), frontend)
                .await
                .unwrap_or_else(|error| panic!("{label}: unpaginated query failed: {error}"));
            assert_pagination(&output, label);
            let response = validate_response(
                &output.compiled.input,
                GraphFormatter.format(&output),
                label,
            );
            collected.assert_matches(&response, label);
        }
    }
}
