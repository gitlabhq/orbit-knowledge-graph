use std::collections::HashSet;

use integration_tests_codegraph::assertions::{IncrementalStep, TestSuite};
use tree_dsl::pipeline::{self, Changes};
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, State};
use tree_dsl_tests::runner::sources;

fn load_suite() -> TestSuite {
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../integration-tests-codegraph/fixtures_new/python/incremental/basic.yaml"
    ))
    .expect("fixture not found");
    orbit_utils::yaml::from_str(&yaml).expect("bad yaml")
}

fn index(env: &Env, suite: &TestSuite) -> State {
    pipeline::index(Context::new(env), sources(&suite.fixtures))
        .unwrap()
        .into_value()
        .state
}

fn reindex(env: &Env, state: State, step: &IncrementalStep) -> State {
    let changes = Changes {
        added: sources(&step.add),
        modified: sources(&step.modify),
        removed: step.remove.clone(),
    };
    pipeline::reindex(Context::new(env), state, changes)
        .unwrap()
        .into_value()
        .state
}

fn count_defs(state: &tree_dsl::State) -> usize {
    state
        .trees
        .iter()
        .flat_map(|t| t.root().descendants())
        .filter(|c| c.is(tree_dsl::canonical::Canonical::Def))
        .count()
}

fn file_set(state: &tree_dsl::State) -> HashSet<String> {
    state.trees.iter().map(|t| t.label.clone()).collect()
}

fn def_names(state: &tree_dsl::State, env: &tree_dsl::Env) -> Vec<String> {
    state
        .trees
        .iter()
        .flat_map(|t| {
            t.root()
                .descendants()
                .filter(|c| c.is(tree_dsl::canonical::Canonical::Def))
                .filter_map(|c| c.child_sym(tree_dsl::canonical::Canonical::DefName))
                .map(|s| env.lang.syms.resolve(s).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn round_trip_save_load() {
    let suite = load_suite();
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, &suite);
    assert_eq!(state.trees.len(), suite.fixtures.len());
    assert!(!state.edges.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");

    state.save(&env, &snap).unwrap();
    assert!(std::fs::metadata(&snap).unwrap().len() > 0);

    let (_, loaded) = tree_dsl::State::load(&snap, SupportLang::Python).unwrap();

    assert_eq!(loaded.trees.len(), state.trees.len());
    assert_eq!(loaded.edges.len(), state.edges.len());
    assert_eq!(count_defs(&loaded), count_defs(&state));
    assert_eq!(file_set(&loaded), file_set(&state));

    for (orig, restored) in state.trees.iter().zip(loaded.trees.iter()) {
        assert_eq!(orig.label, restored.label);
        assert_eq!(orig.len(), restored.len());
    }
    for (orig, restored) in state.edges.iter().zip(loaded.edges.iter()) {
        assert_eq!(orig.from_tree, restored.from_tree);
        assert_eq!(orig.from_node, restored.from_node);
        assert_eq!(orig.to_tree, restored.to_tree);
        assert_eq!(orig.to_node, restored.to_node);
        assert_eq!(orig.site, restored.site);
    }
}

#[test]
fn incremental_via_snapshot_and_reindex() {
    let suite = load_suite();
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, &suite);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    state.save(&env, &snap).unwrap();

    let (env, mut current) = tree_dsl::State::load(&snap, SupportLang::Python).unwrap();
    assert_eq!(
        file_set(&current),
        HashSet::from_iter(["main.py".into(), "utils.py".into()])
    );

    for step in &suite.steps {
        current = reindex(&env, current, step);
    }

    assert_eq!(
        file_set(&current),
        HashSet::from_iter(["main.py".into(), "consumer.py".into()]),
    );
    let names = def_names(&current, &env);
    assert!(!names.contains(&"helper".to_string()));
    assert!(!names.contains(&"extra".to_string()));
}

#[test]
fn modify_preserves_resolution_after_reindex() {
    let suite = load_suite();
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, &suite);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    state.save(&env, &snap).unwrap();

    let (env, loaded) = tree_dsl::State::load(&snap, SupportLang::Python).unwrap();
    let initial_edges = loaded.edges.len();
    assert!(initial_edges > 0);

    let step = IncrementalStep {
        add: Vec::new(),
        remove: Vec::new(),
        ..suite.steps[0].clone()
    };
    let updated = reindex(&env, loaded, &step);

    assert_eq!(updated.trees.len(), 2);
    assert!(updated.edges.len() >= initial_edges);

    let names = def_names(&updated, &env);
    assert!(names.contains(&"helper".to_string()));
    assert!(names.contains(&"extra".to_string()));
    assert_eq!(count_defs(&updated), 2);
}
