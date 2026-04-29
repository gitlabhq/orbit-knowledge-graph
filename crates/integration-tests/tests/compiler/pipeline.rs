//! Tests for the compiler pipeline infrastructure.
//!
//! Validates generic pipeline behavior: pass execution order, seal state
//! cleanup, disable/enable toggling, observer callbacks, error propagation,
//! and that the concrete presets (clickhouse, from_input, hydration) produce
//! correct output through the full pipeline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use compiler::pipelines;
use compiler::{
    Pipeline, PipelineObserver, QueryError, QueryState, SealJson, SecureEnv, Validator,
};
use ontology::Ontology;

use super::setup::{embedded_ontology, test_ctx, test_ontology};

// ═════════════════════════════════════════════════════════════════════════════
// Test observer — records callback invocations
// ═════════════════════════════════════════════════════════════════════════════

#[derive(Clone, Default)]
struct TestObserver {
    events: Arc<Mutex<Vec<ObserverEvent>>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ObserverEvent {
    Completed(&'static str),
    Failed(&'static str),
    Skipped(&'static str),
}

impl PipelineObserver for TestObserver {
    fn pass_completed(&self, pass_name: &'static str, _elapsed: Duration) {
        self.events
            .lock()
            .unwrap()
            .push(ObserverEvent::Completed(pass_name));
    }

    fn pass_failed(&self, pass_name: &'static str, _error: &QueryError) {
        self.events
            .lock()
            .unwrap()
            .push(ObserverEvent::Failed(pass_name));
    }

    fn pass_skipped(&self, pass_name: &'static str) {
        self.events
            .lock()
            .unwrap()
            .push(ObserverEvent::Skipped(pass_name));
    }
}

impl TestObserver {
    fn events(&self) -> Vec<ObserverEvent> {
        self.events.lock().unwrap().clone()
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Helpers
// ═════════════════════════════════════════════════════════════════════════════

fn search_json() -> &'static str {
    r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
        "limit": 10
    }"#
}

fn secure_env(ontology: &Ontology) -> SecureEnv {
    SecureEnv::new(Arc::new(ontology.clone()), test_ctx())
}

// ═════════════════════════════════════════════════════════════════════════════
// Seal behavior
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn seal_json_drops_json_after_validate() {
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = Pipeline::builder()
        .pass(compiler::ValidatePass)
        .seal(SealJson)
        .pass(compiler::NormalizePass)
        .pass(compiler::LowerPass)
        .pass(compiler::OptimizePass)
        .pass(compiler::EnforcePass)
        .pass(compiler::SecurityPass)
        .pass(compiler::CheckPass)
        .pass(compiler::CodegenPass)
        .build()
        .seal();

    let result = pipeline.execute(state, &env).unwrap();

    // json should have been sealed (dropped) after ValidatePass
    assert!(
        result.json.is_none(),
        "json should be sealed after validate"
    );
    // output should be populated
    assert!(result.output.is_some(), "output should be present");
}

#[test]
fn sealed_state_not_accessible_to_later_passes() {
    // If we seal input after ValidatePass (before LowerPass needs it),
    // the pipeline should fail because LowerPass requires HasInput.
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = Pipeline::builder()
        .pass(compiler::ValidatePass)
        .seal(SealJson)
        .seal(compiler::SealInput) // seal input too early
        .pass(compiler::NormalizePass) // needs input — should fail
        .build()
        .seal();

    let result = pipeline.execute(state, &env);
    assert!(result.is_err(), "should fail because input was sealed");
    let err = result.err().unwrap().to_string();
    assert!(err.contains("input"), "error should mention input: {err}");
}

// ═════════════════════════════════════════════════════════════════════════════
// Disable / enable
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn disable_skips_pass() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = pipelines::clickhouse()
        .disable("security")
        .observe(observer.clone())
        .seal();

    // May fail at CheckPass because security filters are missing — we only
    // care that the observer recorded "security" as skipped.
    let _result = pipeline.execute(state, &env);
    let events = observer.events();

    assert!(
        events.contains(&ObserverEvent::Skipped("security")),
        "security pass should be skipped: {events:?}"
    );
}

#[test]
fn disable_then_enable_runs_pass() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = pipelines::clickhouse()
        .disable("security")
        .enable("security")
        .observe(observer.clone())
        .seal();

    let result = pipeline.execute(state, &env);
    if let Err(e) = &result {
        panic!("pipeline should succeed: {e}");
    }

    let events = observer.events();
    assert!(
        events.contains(&ObserverEvent::Completed("security")),
        "security pass should have run: {events:?}"
    );
    assert!(
        !events.contains(&ObserverEvent::Skipped("security")),
        "security pass should not be skipped: {events:?}"
    );
}

#[test]
fn passes_lists_all_steps_with_enabled_status() {
    let pipeline = pipelines::clickhouse().disable("security").disable("check");

    let passes = pipeline.passes();
    let security = passes.iter().find(|(name, _)| *name == "security");
    let check = passes.iter().find(|(name, _)| *name == "check");
    let codegen = passes.iter().find(|(name, _)| *name == "codegen");

    assert_eq!(security, Some(&("security", false)));
    assert_eq!(check, Some(&("check", false)));
    assert_eq!(codegen, Some(&("codegen", true)));
}

// ═════════════════════════════════════════════════════════════════════════════
// Observer callbacks
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn observer_records_all_pass_completions() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = pipelines::clickhouse().observe(observer.clone()).seal();

    pipeline.execute(state, &env).unwrap();

    let events = observer.events();
    let completed: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            ObserverEvent::Completed(name) => Some(*name),
            _ => None,
        })
        .collect();

    assert_eq!(
        completed,
        vec![
            "validate",
            "normalize",
            "restrict",
            "lower",
            "optimize",
            "enforce",
            "deduplicate",
            "security",
            "check",
            "hydrate_plan",
            "settings",
            "codegen"
        ]
    );
}

#[test]
fn observer_records_failure_on_bad_input() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json("not valid json {{{");

    let pipeline = pipelines::clickhouse().observe(observer.clone()).seal();

    let result = pipeline.execute(state, &env);
    assert!(result.is_err());

    let events = observer.events();
    assert!(
        events.contains(&ObserverEvent::Failed("validate")),
        "should record validate failure: {events:?}"
    );
    // No other passes should have run
    assert_eq!(events.len(), 1, "only one event expected: {events:?}");
}

#[test]
fn observer_records_skipped_for_disabled_passes() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let pipeline = pipelines::clickhouse()
        .disable("security")
        .disable("check")
        .observe(observer.clone())
        .seal();

    pipeline.execute(state, &env).unwrap();

    let events = observer.events();
    assert!(events.contains(&ObserverEvent::Skipped("security")));
    assert!(events.contains(&ObserverEvent::Skipped("check")));
    assert!(events.contains(&ObserverEvent::Completed("codegen")));
}

// ═════════════════════════════════════════════════════════════════════════════
// Error propagation
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn error_stops_pipeline_early() {
    let observer = TestObserver::default();
    let env = secure_env(&test_ontology());
    // Invalid entity type — ValidatePass will succeed (valid JSON + schema),
    // but NormalizePass should fail on unknown entity.
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "x", "entity": "NonExistent", "node_ids": [1], "columns": []},
        "limit": 10
    }"#;
    let state = QueryState::from_json(json);

    let pipeline = pipelines::clickhouse().observe(observer.clone()).seal();

    let result = pipeline.execute(state, &env);
    assert!(result.is_err());

    let events = observer.events();
    let completed: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            ObserverEvent::Completed(name) => Some(*name),
            _ => None,
        })
        .collect();

    // validate should succeed, then the failing pass should be recorded
    assert!(
        !completed.contains(&"lower"),
        "lower should not have run after failure: {events:?}"
    );
    assert!(
        !completed.contains(&"codegen"),
        "codegen should not have run after failure: {events:?}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// Pipeline presets — end-to-end
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn clickhouse_preset_compiles_search_query() {
    let env = secure_env(&test_ontology());
    let state = QueryState::from_json(search_json());

    let result = pipelines::clickhouse()
        .seal()
        .execute(state, &env)
        .unwrap()
        .into_output()
        .unwrap();

    assert!(!result.base.sql.is_empty());
    assert_eq!(result.query_type, compiler::QueryType::Traversal);
}

#[test]
fn clickhouse_preset_compiles_traversal_query() {
    let env = secure_env(&test_ontology());
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "p"}],
        "limit": 10
    }"#;
    let state = QueryState::from_json(json);

    let result = pipelines::clickhouse()
        .seal()
        .execute(state, &env)
        .unwrap()
        .into_output()
        .unwrap();

    assert_eq!(result.query_type, compiler::QueryType::Traversal);
    assert!(!result.base.sql.is_empty());
}

#[test]
fn from_input_preset_compiles_pre_built_input() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let env = SecureEnv::new(Arc::new(ontology.clone()), ctx);

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]},
        "limit": 10
    }"#;
    let v = Validator::new(&ontology);
    let value = v.check_json(json).unwrap();
    v.check_ontology(&value).unwrap();
    let input: compiler::Input = serde_json::from_value(value).unwrap();
    v.check_references(&input).unwrap();
    let input = compiler::normalize(input, &ontology).unwrap();

    let state = QueryState::from_input(input);
    let result = pipelines::from_input()
        .seal()
        .execute(state, &env)
        .unwrap()
        .into_output()
        .unwrap();

    assert_eq!(result.query_type, compiler::QueryType::Traversal);
    assert!(!result.base.sql.is_empty());
}

#[test]
fn hydration_preset_skips_security_and_check() {
    let observer = TestObserver::default();
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let env = SecureEnv::new(Arc::new(Ontology::new()), ctx);

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]},
        "limit": 10
    }"#;
    let v = Validator::new(&ontology);
    let value = v.check_json(json).unwrap();
    v.check_ontology(&value).unwrap();
    let input: compiler::Input = serde_json::from_value(value).unwrap();
    v.check_references(&input).unwrap();
    let mut input = compiler::normalize(input, &ontology).unwrap();
    input.query_type = compiler::QueryType::Hydration;

    let state = QueryState::from_input(input);
    let result = pipelines::hydration()
        .observe(observer.clone())
        .seal()
        .execute(state, &env)
        .unwrap()
        .into_output()
        .unwrap();

    assert_eq!(result.query_type, compiler::QueryType::Hydration);
    assert_eq!(result.hydration, compiler::HydrationPlan::None);

    let events = observer.events();
    let pass_names: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            ObserverEvent::Completed(name) => Some(*name),
            _ => None,
        })
        .collect();

    assert_eq!(
        pass_names,
        vec![
            "restrict",
            "lower",
            "optimize",
            "enforce",
            "deduplicate",
            "settings",
            "codegen"
        ]
    );
    assert!(!pass_names.contains(&"security"));
    assert!(!pass_names.contains(&"check"));
}

#[test]
fn clickhouse_preset_matches_compile_output() {
    let ontology = test_ontology();
    let ctx = test_ctx();

    // Via compile() public API
    let via_api = compiler::compile(search_json(), &ontology, &ctx).unwrap();

    // Via pipeline preset directly
    let env = secure_env(&ontology);
    let state = QueryState::from_json(search_json());
    let via_pipeline = pipelines::clickhouse()
        .seal()
        .execute(state, &env)
        .unwrap()
        .into_output()
        .unwrap();

    assert_eq!(via_api.base.sql, via_pipeline.base.sql);
    assert_eq!(via_api.query_type, via_pipeline.query_type);
}
