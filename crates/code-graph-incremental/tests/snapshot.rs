use std::collections::HashSet;
use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::driver::{self, SNAPSHOT_VERSION};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, Sentinel, State};
mod common;

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
    driver::index(env, &common::walk(repo), &Sentinel::disabled())
        .unwrap()
        .0
}

fn reindex(env: &Env, state: State, repo: &Path, changed: Vec<String>, removed: &[&str]) -> State {
    for path in removed {
        std::fs::remove_file(repo.join(path)).unwrap();
    }
    let changes = common::load(orbit_utils::vfs::sources::Diff {
        root: repo,
        paths: changed,
    });
    let removed: Vec<String> = removed.iter().map(|s| s.to_string()).collect();
    driver::reindex(env, &changes, state, &removed, &Sentinel::disabled())
        .unwrap()
        .0
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
fn saving_after_a_one_file_edit_rewrites_one_block() {
    let repo = tempfile::tempdir().unwrap();
    let sources: Vec<(String, String)> = (0..300)
        .map(|i| (format!("m{i}.py"), format!("def f{i}():\n    pass\n")))
        .collect();
    let sources: Vec<(&str, &str)> = sources
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect();
    write(repo.path(), &sources);
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("graph.bin");
    let blocks = || -> HashSet<String> {
        std::fs::read_dir(dir.path().join("graph.blocks"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    };

    index(&env, repo.path()).save(&env, &snapshot).unwrap();
    let before = blocks();
    let (env, loaded) = State::load(&snapshot, SupportLang::Python).unwrap();
    let changed = write(repo.path(), &[("m7.py", "def f7():\n    return 1\n")]);
    reindex(&env, loaded, repo.path(), changed, &[])
        .save(&env, &snapshot)
        .unwrap();
    let after = blocks();

    assert!(before.len() > 1, "300 files fill more than one block");
    assert_eq!(before.difference(&after).count(), 1);
    assert_eq!(after.difference(&before).count(), 1);
    let (_, reloaded) = State::load(&snapshot, SupportLang::Python).unwrap();
    assert_eq!(reloaded.trees.len(), 300);
}

#[test]
fn a_block_from_another_snapshot_is_refused_with_an_error() {
    let env = Env::for_lang(SupportLang::Python).unwrap();
    let saved = |file: &str| {
        let repo = tempfile::tempdir().unwrap();
        write(repo.path(), &[(file, UTILS)]);
        let dir = tempfile::tempdir().unwrap();
        index(&env, repo.path())
            .save(&env, &dir.path().join("graph.bin"))
            .unwrap();
        dir
    };
    let block = |dir: &Path| {
        std::fs::read_dir(dir.join("graph.blocks"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    };
    let (ours, theirs) = (saved("ours.py"), saved("theirs.py"));
    std::fs::copy(block(theirs.path()), block(ours.path())).unwrap();

    let Err(error) = State::load(&ours.path().join("graph.bin"), SupportLang::Python) else {
        panic!("a block from another snapshot loaded");
    };

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("theirs.py"), "{error}");
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

fn reindex_and_stage(
    snapshot: &Path,
    repo: &Path,
    changed: Vec<String>,
    removed: &[&str],
) -> (Env, State) {
    let (env, loaded) = State::load(snapshot, SupportLang::Markdown).unwrap();
    for path in removed {
        std::fs::remove_file(repo.join(path)).unwrap();
    }
    let changes = common::load(orbit_utils::vfs::sources::Diff {
        root: repo,
        paths: changed,
    });
    let removed: Vec<String> = removed.iter().map(|s| s.to_string()).collect();
    let (state, _) =
        driver::reindex(&env, &changes, loaded, &removed, &Sentinel::disabled()).unwrap();
    let staged = snapshot.with_extension("staged");
    state.stage(&env, snapshot, &staged, None).unwrap();
    State::commit_staged(&staged, snapshot).unwrap();
    (env, state)
}

fn links(state: &State) -> HashSet<(String, String)> {
    state
        .edges
        .iter()
        .filter(|edge| edge.kind == code_graph_incremental::edge::EdgeKind::Links)
        .map(|edge| {
            (
                state.trees[edge.from_tree as usize].label.clone(),
                state.trees[edge.to_tree as usize].label.clone(),
            )
        })
        .collect()
}

#[test]
fn link_targets_removed_or_added_in_another_block_update_the_source_block() {
    let block_of = |label: &str| twox_hash::XxHash3_64::oneshot(label.as_bytes()) % 2;
    let other_block = |prefix: &str| {
        (0..)
            .map(|i| format!("{prefix}{i}.md"))
            .find(|label| block_of(label) != block_of("index.md"))
            .unwrap()
    };
    let (removed, added) = (other_block("gone"), other_block("new"));
    let mut sources: Vec<(String, String)> = (0..100)
        .map(|i| (format!("t{i}.md"), "# T\n".to_string()))
        .collect();
    sources.push((removed.clone(), "# Gone\n".to_string()));
    sources.push((
        "index.md".to_string(),
        format!("[gone]({removed}) [new]({added}) [t](t0.md)\n"),
    ));
    let sources: Vec<(&str, &str)> = sources
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect();
    let repo = tempfile::tempdir().unwrap();
    write(repo.path(), &sources);
    let env = Env::for_lang(SupportLang::Markdown).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("graph.bin");
    index(&env, repo.path()).save(&env, &snapshot).unwrap();

    let (_, state) = reindex_and_stage(&snapshot, repo.path(), Vec::new(), &[&removed]);
    let (_, reloaded) = State::load(&snapshot, SupportLang::Markdown).unwrap();
    assert_eq!(links(&reloaded), links(&state));

    let changed = write(repo.path(), &[(&added, "# New\n")]);
    let (_, state) = reindex_and_stage(&snapshot, repo.path(), changed, &[]);
    let (_, reloaded) = State::load(&snapshot, SupportLang::Markdown).unwrap();
    assert!(links(&state).contains(&("index.md".to_string(), added.clone())));
    assert_eq!(links(&reloaded), links(&state));
}
