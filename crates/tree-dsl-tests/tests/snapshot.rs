use std::collections::HashSet;

use std::path::Path;

use integration_tests_codegraph::assertions::{FixtureFile, IncrementalStep, TestSuite};
use tree_dsl::pipeline::Changes;
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, State, inventory, templates};
use tree_dsl_tests::runner::write_files;

fn load_suite() -> TestSuite {
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../integration-tests-codegraph/fixtures_new/python/incremental/basic.yaml"
    ))
    .expect("fixture not found");
    orbit_utils::yaml::from_str(&yaml).expect("bad yaml")
}

/// The fixtures written to `repo`, walked and indexed like a repository.
fn index(env: &Env, repo: &Path, files: &[FixtureFile]) -> State {
    write_files(files, repo);
    let inventory = inventory::walk(repo).unwrap().to_vec();
    templates::index(Context::new(env), repo, inventory)
        .unwrap()
        .into_value()
        .state
}

/// The step applied to `repo` on disk, then reindexed from the change list.
fn reindex(env: &Env, state: State, repo: &Path, step: &IncrementalStep) -> State {
    for removed in &step.remove {
        std::fs::remove_file(repo.join(removed)).ok();
    }
    let mut changed = write_files(&step.add, repo);
    changed.extend(write_files(&step.modify, repo));
    let changes = Changes {
        changed: inventory::classify(repo, changed),
        removed: step.remove.clone(),
    };
    templates::reindex(Context::new(env), state, repo, changes)
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
    let repo = tempfile::tempdir().unwrap();
    let state = index(&env, repo.path(), &suite.fixtures);
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
    let repo = tempfile::tempdir().unwrap();
    let state = index(&env, repo.path(), &suite.fixtures);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    state.save(&env, &snap).unwrap();

    let (env, mut current) = tree_dsl::State::load(&snap, SupportLang::Python).unwrap();
    assert_eq!(
        file_set(&current),
        HashSet::from_iter(["main.py".into(), "utils.py".into()])
    );

    for step in &suite.steps {
        current = reindex(&env, current, repo.path(), step);
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
    let repo = tempfile::tempdir().unwrap();
    let state = index(&env, repo.path(), &suite.fixtures);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    state.save(&env, &snap).unwrap();

    let (env, loaded) = tree_dsl::State::load(&snap, SupportLang::Python).unwrap();
    let cross_file = |state: &State| {
        state
            .edges
            .iter()
            .filter(|e| e.from_tree != e.to_tree)
            .count()
    };
    let initial_cross_file = cross_file(&loaded);
    assert!(initial_cross_file > 0);

    let step = IncrementalStep {
        add: Vec::new(),
        remove: Vec::new(),
        ..suite.steps[0].clone()
    };
    let updated = reindex(&env, loaded, repo.path(), &step);

    assert_eq!(updated.trees.len(), 2);
    assert_eq!(
        cross_file(&updated),
        initial_cross_file,
        "the import still resolves"
    );

    let names = def_names(&updated, &env);
    assert!(names.contains(&"helper".to_string()));
    assert!(names.contains(&"extra".to_string()));
    assert_eq!(count_defs(&updated), 2);
}

/// A manifest is not source, so it never becomes a tree; it lives on the
/// graph so the resolver still sees it after a snapshot and after reindexes
/// that do not touch it.
#[test]
fn manifests_survive_snapshot_and_reindex() {
    let fixture = |path: &str, content: &str| FixtureFile {
        path: path.into(),
        content: content.into(),
    };
    let step = |modify: Vec<FixtureFile>, remove: Vec<String>| IncrementalStep {
        name: String::new(),
        snapshot: false,
        add: Vec::new(),
        modify,
        remove,
        tests: Vec::new(),
    };
    let env = Env::for_lang(SupportLang::Rust).unwrap();
    let repo = tempfile::tempdir().unwrap();
    let state = index(
        &env,
        repo.path(),
        &[
            fixture("Cargo.toml", "[package]\nname = \"one\"\n"),
            fixture("src/main.rs", "fn main() {}\n"),
        ],
    );
    let parsed: Vec<&str> = state.trees.iter().map(|t| t.label.as_str()).collect();
    assert_eq!(parsed, ["src/main.rs", "Cargo.toml"]);
    assert_eq!(state.configs[0].path, "Cargo.toml");

    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    state.save(&env, &snap).unwrap();
    let (env, loaded) = State::load(&snap, SupportLang::Rust).unwrap();
    assert_eq!(loaded.configs.len(), 1);

    let edit_source = step(
        vec![fixture("src/main.rs", "fn main() { run() }\nfn run() {}\n")],
        vec![],
    );
    let state = reindex(&env, loaded, repo.path(), &edit_source);
    assert_eq!(state.configs[0].content, "[package]\nname = \"one\"\n");

    let edit_manifest = step(
        vec![fixture("Cargo.toml", "[package]\nname = \"two\"\n")],
        vec![],
    );
    let state = reindex(&env, state, repo.path(), &edit_manifest);
    assert_eq!(state.configs.len(), 1);
    assert_eq!(state.configs[0].content, "[package]\nname = \"two\"\n");

    let drop_manifest = step(vec![], vec!["Cargo.toml".into()]);
    let state = reindex(&env, state, repo.path(), &drop_manifest);
    assert!(state.configs.is_empty());
    assert_eq!(state.trees.len(), 1);
}
