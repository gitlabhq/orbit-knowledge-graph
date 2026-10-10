use std::collections::HashSet;
use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{Changes, SNAPSHOT_VERSION};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, State, inventory, pattern, rules, templates};

const MAIN: &str = "from utils import helper\nhelper()\n";
const UTILS: &str = "def helper():\n    pass\n";
const UTILS_EXTRA: &str = "def helper():\n    pass\n\ndef extra():\n    pass\n";

fn write(root: &Path, files: &[(&str, &str)]) -> Vec<String> {
    files
        .iter()
        .map(|(path, content)| {
            std::fs::write(root.join(path), content).unwrap();
            path.to_string()
        })
        .collect()
}

fn index(env: &Env, repo: &Path) -> State {
    let inventory = inventory::walk(repo).unwrap().to_vec();
    templates::index(Context::new(env), repo, inventory)
        .unwrap()
        .into_value()
        .state
}

fn reindex(env: &Env, state: State, repo: &Path, changed: Vec<String>, removed: &[&str]) -> State {
    for path in removed {
        std::fs::remove_file(repo.join(path)).unwrap();
    }
    let changes = Changes {
        changed: inventory::classify(repo, changed),
        removed: removed.iter().map(|s| s.to_string()).collect(),
    };
    templates::reindex(Context::new(env), state, repo, changes)
        .unwrap()
        .into_value()
        .state
}

fn save_and_load(env: &Env, state: &State) -> (Env, State) {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("graph.bin");
    state.save(env, &snapshot).unwrap();
    State::load(&snapshot, SupportLang::Python).unwrap()
}

fn files(state: &State) -> HashSet<String> {
    state.trees.iter().map(|t| t.label.clone()).collect()
}

fn def_names(state: &State, env: &Env) -> Vec<String> {
    let mut names: Vec<String> = state
        .trees
        .iter()
        .flat_map(|t| t.root().descendants())
        .filter(|c| c.is(C::Def))
        .filter_map(|c| c.child_sym(C::DefName))
        .map(|s| env.lang.syms.resolve(s).to_string())
        .collect();
    names.sort();
    names
}

fn cross_file_edges(state: &State) -> usize {
    state
        .edges
        .iter()
        .filter(|e| e.from_tree != e.to_tree)
        .count()
}

#[test]
fn a_snapshot_round_trips_the_graph() {
    let repo = tempfile::tempdir().unwrap();
    write(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, repo.path());

    let (_, loaded) = save_and_load(&env, &state);

    assert_eq!(files(&loaded), files(&state));
    assert_eq!(loaded.edges.len(), state.edges.len());
    for (before, after) in state.trees.iter().zip(&loaded.trees) {
        assert_eq!(
            (before.label.as_str(), before.len()),
            (after.label.as_str(), after.len())
        );
    }
    for (before, after) in state.edges.iter().zip(&loaded.edges) {
        assert_eq!(
            (before.from(), before.to(), before.kind),
            (after.from(), after.to(), after.kind)
        );
    }
}

#[test]
fn post_link_tree_edits_preserve_node_ids_tags_and_edges_across_snapshots() {
    let repo = tempfile::tempdir().unwrap();
    write(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let mut state = index(&env, repo.path());
    let edits = rules::load_rules(
        r#"
stages:
  - rules:
      - match: '(__defname)'
        append: ['(__name "owner")', '(__name "discard")', '(__name "member")']
  - rules:
      - match: '(__name "discard")'
        replace: '(discard)'
      - match: '(__name "member")'
        tag: { identity: member }
"#,
        &env.lang,
    )
    .unwrap();
    state.trees = state
        .trees
        .into_iter()
        .map(|tree| {
            let mut tree: code_graph_incremental::tree::Tree = tree.into();
            for stage in &edits {
                pattern::apply_rewrites(&mut tree, &env.lang, stage, &[]).unwrap();
            }
            tree.prune();
            tree.into()
        })
        .collect();
    let structure = |state: &State| {
        state
            .trees
            .iter()
            .map(|tree| {
                std::iter::once(tree.root())
                    .chain(tree.root().descendants())
                    .map(|node| {
                        (
                            node.index(),
                            node.kind(),
                            node.sym(),
                            node.parent().map(|parent| parent.index()),
                            node.tag(env.lang.syms.lookup("identity")),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    let expected = structure(&state);
    let (loaded_env, loaded) = save_and_load(&env, &state);
    assert_eq!(structure(&loaded), expected);
    assert_eq!(loaded.edges.len(), state.edges.len());
    for (before, after) in state.edges.iter().zip(&loaded.edges) {
        assert_eq!(
            (before.from(), before.to(), before.site),
            (after.from(), after.to(), after.site)
        );
    }
    let updated = reindex(&loaded_env, loaded, repo.path(), vec![], &[]);
    assert_eq!(structure(&updated), expected);
    assert_eq!(cross_file_edges(&updated), cross_file_edges(&state));
    let (_, restored) = save_and_load(&loaded_env, &updated);
    assert_eq!(structure(&restored), expected);
    let changed = write(
        repo.path(),
        &[("main.py", "from utils import helper\nhelper()\nhelper()\n")],
    );
    let updated = reindex(&loaded_env, restored, repo.path(), changed, &[]);
    for edge in updated
        .edges
        .iter()
        .filter(|edge| edge.from_tree != edge.to_tree)
    {
        let target = updated.trees[edge.to_tree as usize].cursor(edge.to_node);
        assert_eq!(
            target.child_sym(C::DefName),
            Some(loaded_env.lang.syms.lookup("helper"))
        );
    }
}

/// The rules compiled against a restored interner must agree with the ids in
/// the graph, or the first reindex after a load displays the wrong names.
#[test]
fn reindex_after_load_keeps_resolution_and_names() {
    let repo = tempfile::tempdir().unwrap();
    write(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, repo.path());
    let before = cross_file_edges(&state);
    assert!(before > 0);

    let (env, loaded) = save_and_load(&env, &state);
    let changed = write(repo.path(), &[("utils.py", UTILS_EXTRA)]);
    let updated = reindex(&env, loaded, repo.path(), changed, &[]);

    assert_eq!(files(&updated), files(&state));
    assert_eq!(
        cross_file_edges(&updated),
        before,
        "the import still resolves"
    );
    assert_eq!(def_names(&updated, &env), ["extra", "helper"]);
}

#[test]
fn removed_files_leave_the_graph_and_dependents_are_revisited() {
    let repo = tempfile::tempdir().unwrap();
    write(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let state = index(&env, repo.path());
    let (env, loaded) = save_and_load(&env, &state);

    let updated = reindex(&env, loaded, repo.path(), Vec::new(), &["utils.py"]);

    assert_eq!(files(&updated), HashSet::from(["main.py".to_string()]));
    assert_eq!(def_names(&updated, &env), Vec::<String>::new());
    assert_eq!(cross_file_edges(&updated), 0, "nothing left to resolve to");
    assert!(
        updated
            .edges
            .iter()
            .all(|e| e.from_fi() == 0 && e.to_fi() == 0)
    );
}

#[test]
fn a_snapshot_from_another_format_version_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("graph.bin");
    let mut file = zstd::Encoder::new(std::fs::File::create(&snapshot).unwrap(), 3).unwrap();
    std::io::Write::write_all(&mut file, &99u32.to_le_bytes()).unwrap();
    std::io::Write::write_all(&mut file, b"whatever came after").unwrap();
    file.finish().unwrap();

    let Err(error) = State::load(&snapshot, SupportLang::Python) else {
        panic!("a v99 snapshot loaded");
    };

    assert_eq!(
        error.to_string(),
        format!("snapshot format v99; this build reads v{SNAPSHOT_VERSION}")
    );
}
