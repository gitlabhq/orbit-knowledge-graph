use std::path::Path;
use std::sync::Arc;

use query_engine::compiler::{CompiledQueryContext, Frontend};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::PipelineError;
use query_engine::shared::PipelineOutput;

use super::format::{Backend, QueryExpect, QueryScenario, QueryStep};
use super::pagination::{PageCollection, assert_page_bounds, page_query, unpaginated};
use super::{
    assert_compilation, assert_output, assert_pagination, assert_repeated_response, read_scenarios,
    repeat_count, step_label, validate_response,
};
use crate::scenario::{self, Seed};

pub(super) trait Engine {
    const BACKEND: Backend;

    async fn seed(&self, seed: &Seed, name: &str);

    async fn compile(
        &self,
        query: &str,
        frontend: Frontend,
    ) -> Result<Arc<CompiledQueryContext>, PipelineError>;

    async fn execute(
        &self,
        compiled: &Arc<CompiledQueryContext>,
        frontend: Frontend,
        label: &str,
    ) -> PipelineOutput;
}

pub(super) fn scenarios_for(root: &Path, backend: Backend) -> Vec<(String, QueryScenario)> {
    let mut files = Vec::new();
    scenario::discover(root, &mut files);
    files.sort();
    files
        .iter()
        .flat_map(|file| read_scenarios(root, file))
        .filter(|(_, scenario)| scenario.config.backends.contains(&backend))
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

    let compiled = engine.compile(query, frontend).await;
    let Some(compiled) = assert_compilation(compiled, expect, (backend, key), label) else {
        return;
    };

    if !expect.pages.is_empty() {
        run_pages(engine, frontend, query, expect, label).await;
        return;
    }

    let output = engine.execute(&compiled, frontend, label).await;
    let baseline = assert_output(&output, expect, label);
    for run in 2..=repeat_count(expect, label) {
        let compiled = assert_compilation(
            engine.compile(query, frontend).await,
            expect,
            (backend, key),
            label,
        )
        .expect("repeated query should compile");
        let output = engine.execute(&compiled, frontend, label).await;
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
        let compiled = engine
            .compile(&page_query(frontend, base_query, index), frontend)
            .await
            .unwrap_or_else(|error| panic!("{page_label}: compile failed: {error}"));
        let output = engine.execute(&compiled, frontend, &page_label).await;
        let response = assert_output(&output, page_expect, &page_label);
        let response = serde_json::from_value(response).expect("response should deserialize");
        collected.push(&response);
        assert_page_bounds(&response, index, expect.pages.len(), &page_label);
    }

    if let Some(all_pages) = &expect.all_pages {
        collected.assert_expect(all_pages, label);
        if all_pages.matches_unpaginated {
            let compiled = engine
                .compile(&unpaginated(frontend, base_query), frontend)
                .await
                .unwrap_or_else(|error| panic!("{label}: unpaginated compile failed: {error}"));
            let output = engine.execute(&compiled, frontend, label).await;
            assert_pagination(&output, label);
            let response =
                validate_response(&compiled.input, GraphFormatter.format(&output), label);
            collected.assert_matches(&response, label);
        }
    }
}
