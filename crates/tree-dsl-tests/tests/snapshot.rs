use std::collections::HashSet;

use tree_dsl::grammar::SupportLang;

#[derive(serde::Deserialize)]
struct FixtureFile {
    path: String,
    content: String,
}

#[derive(serde::Deserialize)]
struct Step {
    #[serde(default)]
    add: Vec<FixtureFile>,
    #[serde(default)]
    modify: Vec<FixtureFile>,
    #[serde(default)]
    remove: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Suite {
    fixtures: Vec<FixtureFile>,
    #[serde(default)]
    steps: Vec<Step>,
}

fn load_suite() -> Suite {
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../integration-tests-codegraph/fixtures/python/incremental/basic.yaml"
    ))
    .expect("fixture not found");
    serde_yaml::from_str(&yaml).expect("bad yaml")
}

fn count_defs(result: &tree_dsl::IndexResult) -> usize {
    result
        .trees
        .iter()
        .flat_map(|t| (0..t.len()).map(move |i| t.cursor(i)))
        .filter(|c| tree_dsl::canonical::has_def_type(*c))
        .count()
}

fn file_set(result: &tree_dsl::IndexResult) -> HashSet<String> {
    result.trees.iter().map(|t| t.label.clone()).collect()
}

fn def_names(result: &tree_dsl::IndexResult) -> Vec<String> {
    result
        .trees
        .iter()
        .flat_map(|t| {
            (0..t.len())
                .map(move |i| t.cursor(i))
                .filter(|c| tree_dsl::canonical::has_def_type(*c))
                .filter_map(|c| c.child_sym(tree_dsl::canonical::Canonical::DefName))
                .map(|s| result.lang.syms.resolve(s).to_string())
        })
        .collect()
}

#[test]
fn round_trip_save_load() {
    let suite = load_suite();
    let fixtures: Vec<(String, String)> = suite
        .fixtures
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();

    let result = tree_dsl::index(SupportLang::Python, &fixtures);
    assert_eq!(result.trees.len(), fixtures.len());
    assert!(!result.cross_edges.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");

    result.save(&snap).unwrap();
    assert!(std::fs::metadata(&snap).unwrap().len() > 0);

    let loaded = tree_dsl::IndexResult::load(&snap, SupportLang::Python).unwrap();

    assert_eq!(loaded.trees.len(), result.trees.len());
    assert_eq!(loaded.cross_edges.len(), result.cross_edges.len());
    assert_eq!(count_defs(&loaded), count_defs(&result));
    assert_eq!(file_set(&loaded), file_set(&result));

    for (orig, restored) in result.trees.iter().zip(loaded.trees.iter()) {
        assert_eq!(orig.label, restored.label);
        assert_eq!(orig.nodes.len(), restored.nodes.len());
        assert_eq!(orig.edges().len(), restored.edges().len());
    }
    for (orig, restored) in result.cross_edges.iter().zip(loaded.cross_edges.iter()) {
        assert_eq!(orig.from.tree, restored.from.tree);
        assert_eq!(orig.from.node, restored.from.node);
        assert_eq!(orig.to.tree, restored.to.tree);
        assert_eq!(orig.to.node, restored.to.node);
    }
}

#[test]
fn incremental_lifecycle_through_serialization() {
    let suite = load_suite();
    let fixtures: Vec<(String, String)> = suite
        .fixtures
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();

    let result = tree_dsl::index(SupportLang::Python, &fixtures);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    result.save(&snap).unwrap();

    let mut current = tree_dsl::IndexResult::load(&snap, SupportLang::Python).unwrap();
    assert_eq!(file_set(&current), HashSet::from_iter(["main.py".into(), "utils.py".into()]));

    for (i, step) in suite.steps.iter().enumerate() {
        let added: Vec<(String, String)> = step
            .add
            .iter()
            .map(|f| (f.path.clone(), f.content.clone()))
            .collect();
        let modified: Vec<(String, String)> = step
            .modify
            .iter()
            .map(|f| (f.path.clone(), f.content.clone()))
            .collect();
        current.update(&added, &modified, &step.remove);

        let step_snap = dir.path().join(format!("step{i}.bin"));
        current.save(&step_snap).unwrap();
        current = tree_dsl::IndexResult::load(&step_snap, SupportLang::Python).unwrap();
    }

    assert_eq!(
        file_set(&current),
        HashSet::from_iter(["main.py".into(), "consumer.py".into()]),
    );
    let names = def_names(&current);
    assert!(!names.contains(&"helper".to_string()));
    assert!(!names.contains(&"extra".to_string()));
}

#[test]
fn modify_step_preserves_resolution() {
    let suite = load_suite();
    let fixtures: Vec<(String, String)> = suite
        .fixtures
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();

    let result = tree_dsl::index(SupportLang::Python, &fixtures);
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");
    result.save(&snap).unwrap();

    let mut current = tree_dsl::IndexResult::load(&snap, SupportLang::Python).unwrap();
    let initial_cross = current.cross_edges.len();
    assert!(initial_cross > 0);

    let step = &suite.steps[0];
    let modified: Vec<(String, String)> = step
        .modify
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();
    current.update(&[], &modified, &[]);

    assert_eq!(current.trees.len(), 2);
    assert!(current.cross_edges.len() >= initial_cross);

    let names = def_names(&current);
    assert!(names.contains(&"helper".to_string()));
    assert!(names.contains(&"extra".to_string()));

    let snap2 = dir.path().join("after_modify.bin");
    current.save(&snap2).unwrap();
    let reloaded = tree_dsl::IndexResult::load(&snap2, SupportLang::Python).unwrap();
    assert_eq!(count_defs(&reloaded), count_defs(&current));
    assert_eq!(reloaded.cross_edges.len(), current.cross_edges.len());
}
