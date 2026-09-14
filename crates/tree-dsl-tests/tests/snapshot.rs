use tree_dsl::grammar::SupportLang;

fn fixtures() -> Vec<(String, String)> {
    vec![
        (
            "main.py".into(),
            "from utils import helper\nhelper()\n".into(),
        ),
        (
            "utils.py".into(),
            "def helper():\n    pass\n".into(),
        ),
    ]
}

fn count_defs(result: &tree_dsl::IndexResult) -> usize {
    result
        .trees
        .iter()
        .flat_map(|t| (0..t.len()).map(move |i| t.cursor(i)))
        .filter(|c| tree_dsl::canonical::has_def_type(*c))
        .count()
}

fn file_paths(result: &tree_dsl::IndexResult) -> Vec<String> {
    result.trees.iter().map(|t| t.label.clone()).collect()
}

#[test]
fn round_trip_save_load() {
    let result = tree_dsl::index(SupportLang::Python, &fixtures());
    assert_eq!(result.trees.len(), 2);
    assert!(!result.cross_edges.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let snap_path = dir.path().join("graph.bin");

    result.save(&snap_path).unwrap();
    assert!(snap_path.exists());
    assert!(std::fs::metadata(&snap_path).unwrap().len() > 0);

    let loaded = tree_dsl::IndexResult::load(&snap_path, SupportLang::Python).unwrap();

    assert_eq!(loaded.trees.len(), result.trees.len());
    assert_eq!(loaded.cross_edges.len(), result.cross_edges.len());
    for (orig, restored) in result.trees.iter().zip(loaded.trees.iter()) {
        assert_eq!(orig.label, restored.label);
        assert_eq!(orig.nodes.len(), restored.nodes.len());
        assert_eq!(orig.edges().len(), restored.edges().len());
    }

    let orig_defs = count_defs(&result);
    let loaded_defs = count_defs(&loaded);
    assert_eq!(orig_defs, loaded_defs);

    for (orig, restored) in result.cross_edges.iter().zip(loaded.cross_edges.iter()) {
        assert_eq!(orig.from.tree, restored.from.tree);
        assert_eq!(orig.from.node, restored.from.node);
        assert_eq!(orig.to.tree, restored.to.tree);
        assert_eq!(orig.to.node, restored.to.node);
    }
}

#[test]
fn incremental_update_after_load() {
    let result = tree_dsl::index(SupportLang::Python, &fixtures());

    let dir = tempfile::tempdir().unwrap();
    let snap_path = dir.path().join("graph.bin");
    result.save(&snap_path).unwrap();

    let mut loaded = tree_dsl::IndexResult::load(&snap_path, SupportLang::Python).unwrap();
    assert_eq!(file_paths(&loaded), vec!["main.py", "utils.py"]);
    let initial_cross = loaded.cross_edges.len();

    loaded.update(
        &[("consumer.py".into(), "from utils import helper\nhelper()\n".into())],
        &[],
        &[],
    );
    assert_eq!(loaded.trees.len(), 3);
    assert!(file_paths(&loaded).contains(&"consumer.py".to_string()));
    assert!(loaded.cross_edges.len() > initial_cross);

    loaded.update(&[], &[], &["utils.py".into()]);
    assert_eq!(loaded.trees.len(), 2);
    assert!(!file_paths(&loaded).contains(&"utils.py".to_string()));

    let snap2 = dir.path().join("graph2.bin");
    loaded.save(&snap2).unwrap();
    let reloaded = tree_dsl::IndexResult::load(&snap2, SupportLang::Python).unwrap();
    assert_eq!(reloaded.trees.len(), 2);
    assert_eq!(
        file_paths(&reloaded).into_iter().collect::<std::collections::HashSet<_>>(),
        file_paths(&loaded).into_iter().collect::<std::collections::HashSet<_>>(),
    );
}

#[test]
fn modify_preserves_resolution() {
    let mut result = tree_dsl::index(SupportLang::Python, &fixtures());
    let initial_cross = result.cross_edges.len();
    assert!(initial_cross > 0);

    result.update(
        &[],
        &[("utils.py".into(), "def helper():\n    pass\n\ndef extra():\n    pass\n".into())],
        &[],
    );

    assert_eq!(result.trees.len(), 2);
    assert!(result.cross_edges.len() >= initial_cross);

    let defs: Vec<String> = result
        .trees
        .iter()
        .flat_map(|t| {
            (0..t.len())
                .map(move |i| t.cursor(i))
                .filter(|c| tree_dsl::canonical::has_def_type(*c))
                .filter_map(|c| c.child_sym(tree_dsl::canonical::Canonical::DefName))
                .map(|s| result.lang.syms.resolve(s).to_string())
        })
        .collect();
    assert!(defs.contains(&"helper".to_string()));
    assert!(defs.contains(&"extra".to_string()));
}
