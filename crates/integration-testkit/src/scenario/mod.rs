//! Each scenario file seeds datalake rows, runs one or more indexer
//! handlers, and asserts on the resulting graph nodes and edges. See
//! `config/schemas/indexer_scenario.schema.json` for the format.

mod expect;
mod format;
mod seed;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::collect_subtest_results;
use crate::context::TestContext;

pub use format::{
    ContainsMatcher, EdgeExpect, Expect, Matcher, NodeExpect, RunSpec, Scenario, Scope, Seed,
    SeedSettings, Step,
};

/// Maps a scenario `run:` handler name to an actual indexer handler
/// invocation. Implemented by test crates because handler construction
/// depends on the `indexer` crate, which this crate must not depend on.
#[async_trait]
pub trait ScenarioHandlers: Send + Sync {
    async fn run(&self, ctx: &TestContext, handler: &str, scope: Option<Scope>);
}

/// Discover and run every scenario under `root` as a concurrent subtest
/// with its own forked database. Pass an absolute path, e.g.
/// `concat!(env!("CARGO_MANIFEST_DIR"), "/tests/indexer/scenarios/sdlc")`.
///
/// Set `SCENARIO_FILTER` to a substring to run only matching scenarios
/// (matched against the `<dir>/<relative path>` name, so it selects by
/// folder, e.g. `ci/`, or by name, e.g. `processes_jobs`).
pub async fn run_dir(ctx: &TestContext, root: &str, handlers: Arc<dyn ScenarioHandlers>) {
    let root = Path::new(root);
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    assert!(
        !files.is_empty(),
        "no scenario files found under {}",
        root.display()
    );

    if let Ok(filter) = std::env::var("SCENARIO_FILTER") {
        let filter = filter.trim();
        if !filter.is_empty() {
            files.retain(|file| scenario_name(root, file).contains(filter));
            assert!(
                !files.is_empty(),
                "SCENARIO_FILTER='{filter}' matched no scenarios under {}",
                root.display()
            );
        }
    }

    assert_distinct_database_names(root, &files);

    let concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let ctx = Arc::new(ctx.clone());

    let mut handles = Vec::new();
    for file in files {
        let name = scenario_name(root, &file);
        let semaphore = Arc::clone(&semaphore);
        let ctx = Arc::clone(&ctx);
        let handlers = Arc::clone(&handlers);
        let task_name = name.clone();
        let handle = tokio::task::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let db = ctx.fork(&database_name(&task_name)).await;
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            run_scenario(&db, &file, &task_name, handlers.as_ref()).await;
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
}

async fn run_scenario(ctx: &TestContext, file: &Path, name: &str, handlers: &dyn ScenarioHandlers) {
    let raw = std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
    let scenario: Scenario = serde_yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("{}: invalid scenario: {e}", file.display()));
    let scope = scenario.scope;
    let steps = scenario.into_steps();
    let columns = seed::fetch_table_columns(ctx).await;

    for (index, step) in steps.iter().enumerate() {
        let location = format!("{name} (step {})", index + 1);
        seed::apply_seed(ctx, &step.seed, &step.seed_settings, &columns, &location).await;
        for handler in step.handlers() {
            handlers.run(ctx, handler, scope).await;
        }
        if let Some(expect) = &step.expect {
            expect::check_expect(ctx, expect, &location).await;
        }
    }
}

fn discover(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => panic!("failed to read scenario directory {}: {e}", dir.display()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            discover(&path, files);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            files.push(path);
        }
    }
}

fn scenario_name(root: &Path, file: &Path) -> String {
    let relative = file
        .strip_prefix(root)
        .expect("scenario file is under root")
        .with_extension("");
    let prefix = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("{prefix}/{}", relative.display())
}

// database_name collapses every non-alphanumeric character to '_', so two scenario
// paths differing only in such characters would fork the same database and silently
// interfere. Catch the collision loudly before any task spawns.
fn assert_distinct_database_names(root: &Path, files: &[PathBuf]) {
    let mut seen: HashMap<String, String> = HashMap::new();
    for file in files {
        let name = scenario_name(root, file);
        let db = database_name(&name);
        if let Some(previous) = seen.insert(db.clone(), name.clone()) {
            panic!(
                "scenarios '{previous}' and '{name}' map to the same database '{db}'; \
                 rename one so their sanitised names differ"
            );
        }
    }
}

fn database_name(scenario_name: &str) -> String {
    scenario_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
