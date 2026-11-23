mod expect;
mod format;
pub(crate) mod seed;

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;

use crate::collect_subtest_results;
use crate::context::TestContext;

pub use format::{
    CdcEvent, CdcOperation, ContainsMatcher, DispatchExpect, DispatchKind, DispatchSpec,
    EdgeExpect, Expect, Matcher, NodeExpect, NotMatcher, RunSpec, Scenario, Scope, Seed,
    SeedSettings, Step,
};
pub use seed::DEFAULT_REPLICATED_AT;

#[derive(Debug, Clone)]
pub struct DispatchedMessage {
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Copy)]
pub struct HandlerInput<'a> {
    pub scope: Option<Scope>,
    pub targets: &'a [String],
    pub cdc: &'a [CdcEvent],
}

#[async_trait]
pub trait ScenarioHandlers: Send + Sync {
    async fn run(
        &self,
        ctx: &TestContext,
        handler: &str,
        input: HandlerInput<'_>,
    ) -> Vec<DispatchedMessage>;
}

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

    assert_distinct_database_names(files.iter().map(|file| scenario_name(root, file)));

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
            let database = database_name(&task_name);
            let db = ctx.fork(&database).await;
            let started = std::time::Instant::now();
            eprintln!("--- {task_name}");
            let outcome = AssertUnwindSafe(run_scenario(&db, &file, &task_name, handlers.as_ref()))
                .catch_unwind()
                .await;
            let cleanup = AssertUnwindSafe(ctx.drop_fork(&database))
                .catch_unwind()
                .await;
            if let Err(failure) = outcome.and(cleanup) {
                resume_unwind(failure);
            }
            eprintln!("    {task_name} {:.2?}", started.elapsed());
        });
        handles.push((name, handle));
    }

    collect_subtest_results(handles).await;
}

async fn run_scenario(ctx: &TestContext, file: &Path, name: &str, handlers: &dyn ScenarioHandlers) {
    let raw = std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
    let scenario: Scenario = orbit_utils::yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("{}: invalid scenario: {e}", file.display()));
    let scope = scenario.scope;
    let steps = scenario.into_steps();
    let columns = seed::fetch_table_columns(ctx).await;

    for (index, step) in steps.iter().enumerate() {
        let location = format!("{name} (step {})", index + 1);
        for table in &step.drop_tables {
            ctx.drop_table(table).await;
        }
        for table in &step.create_tables {
            ctx.create_table(table).await;
        }
        seed::apply_seed(ctx, &step.seed, &step.seed_settings, &columns, &location).await;
        let input = HandlerInput {
            scope,
            targets: &step.targets,
            cdc: &step.cdc,
        };
        let mut dispatched = Vec::new();
        for handler in step.handlers() {
            dispatched.extend(handlers.run(ctx, handler, input).await);
        }
        if let Some(expect) = &step.expect {
            expect::check_expect(ctx, expect, &dispatched, &location).await;
        }
    }
}

pub(crate) fn discover(dir: &Path, files: &mut Vec<PathBuf>) {
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

pub fn yaml_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    files
}

pub(crate) fn scenario_name(root: &Path, file: &Path) -> String {
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

pub(crate) fn assert_distinct_database_names(names: impl IntoIterator<Item = String>) {
    let mut seen: HashMap<String, String> = HashMap::new();
    for name in names {
        let db = database_name(&name);
        if let Some(previous) = seen.insert(db.clone(), name.clone()) {
            panic!(
                "scenarios '{previous}' and '{name}' map to the same database '{db}'; \
                 rename one so their sanitised names differ"
            );
        }
    }
}

pub(crate) fn read_cases<T: serde::de::DeserializeOwned>(file: &Path) -> Vec<(Option<String>, T)> {
    let fail = |error: &dyn std::fmt::Display| -> ! { panic!("{}: {error}", file.display()) };
    let raw = std::fs::read_to_string(file).unwrap_or_else(|e| fail(&e));
    let mut defaults: serde_json::Value =
        orbit_utils::yaml::from_str(&raw).unwrap_or_else(|e| fail(&e));
    let Some(cases) = defaults
        .as_object_mut()
        .and_then(|file| file.remove("cases"))
    else {
        return vec![(
            None,
            orbit_utils::yaml::from_str(&raw).unwrap_or_else(|e| fail(&e)),
        )];
    };
    let serde_json::Value::Array(cases) = cases else {
        fail(&"cases must be a list");
    };
    cases
        .into_iter()
        .enumerate()
        .map(|(index, mut case)| {
            merge_defaults(&mut case, &defaults);
            let label = case
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map_or_else(|| index.to_string(), str::to_string);
            let case = serde_json::from_value(case)
                .unwrap_or_else(|e| fail(&format!("case {label}: {e}")));
            (Some(label), case)
        })
        .collect()
}

fn merge_defaults(case: &mut serde_json::Value, defaults: &serde_json::Value) {
    let (Some(case), Some(defaults)) = (case.as_object_mut(), defaults.as_object()) else {
        return;
    };
    for (key, value) in defaults {
        match case.get_mut(key) {
            Some(existing) => merge_defaults(existing, value),
            None => {
                case.insert(key.clone(), value.clone());
            }
        }
    }
}

pub(crate) fn database_name(scenario_name: &str) -> String {
    scenario_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
