use std::collections::HashSet;

use tree_dsl::treesitter::SupportLang;

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
        .flat_map(|t| t.root().descendants())
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
            t.root()
                .descendants()
                .filter(|c| tree_dsl::canonical::has_def_type(*c))
                .filter_map(|c| c.child_sym(tree_dsl::canonical::Canonical::DefName))
                .map(|s| result.lang.syms.resolve(s).to_string())
                .collect::<Vec<_>>()
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
    assert!(!result.edges.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("graph.bin");

    result.save(&snap).unwrap();
    assert!(std::fs::metadata(&snap).unwrap().len() > 0);

    let loaded = tree_dsl::IndexResult::load(&snap, SupportLang::Python).unwrap();

    assert_eq!(loaded.trees.len(), result.trees.len());
    assert_eq!(loaded.edges.len(), result.edges.len());
    assert_eq!(count_defs(&loaded), count_defs(&result));
    assert_eq!(file_set(&loaded), file_set(&result));

    for (orig, restored) in result.trees.iter().zip(loaded.trees.iter()) {
        assert_eq!(orig.label, restored.label);
        assert_eq!(orig.len(), restored.len());
    }
    for (orig, restored) in result.edges.iter().zip(loaded.edges.iter()) {
        assert_eq!(orig.from_tree, restored.from_tree);
        assert_eq!(orig.from_node, restored.from_node);
        assert_eq!(orig.to_tree, restored.to_tree);
        assert_eq!(orig.to_node, restored.to_node);
    }
}

#[test]
fn incremental_via_snapshot_and_reindex() {
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
    assert_eq!(
        file_set(&current),
        HashSet::from_iter(["main.py".into(), "utils.py".into()])
    );

    for step in &suite.steps {
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
        current = tree_dsl::reindex(current, &added, &modified, &step.remove);
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
fn modify_preserves_resolution_after_reindex() {
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

    let loaded = tree_dsl::IndexResult::load(&snap, SupportLang::Python).unwrap();
    let initial_edges = loaded.edges.len();
    assert!(initial_edges > 0);

    let step = &suite.steps[0];
    let modified: Vec<(String, String)> = step
        .modify
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();
    let updated = tree_dsl::reindex(loaded, &[], &modified, &[]);

    assert_eq!(updated.trees.len(), 2);
    assert!(updated.edges.len() >= initial_edges);

    let names = def_names(&updated);
    assert!(names.contains(&"helper".to_string()));
    assert!(names.contains(&"extra".to_string()));
    assert_eq!(count_defs(&updated), 2);
}
