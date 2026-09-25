use std::path::Path;

use code_graph_incremental::pipeline::{Each, Parse, Parsed, Prepare, Sources, Workset};
use code_graph_incremental::tree::Tree;
use code_graph_incremental::treesitter::{SupportLang, all_languages};
use code_graph_incremental::{Context, Env, Limits, Pipeline, inventory};
use orbit_utils::fs_walk::{Decision, FileInventoryEntry};

fn write_all(root: &Path, files: &[(&str, &[u8])]) {
    for (path, content) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
}

fn parse_repo(env: &Env, root: &Path) -> Workset<Vec<Parsed>> {
    let entries = inventory::walk(root).unwrap().into_inner();
    let sources = Sources {
        root: root.to_path_buf(),
        entries,
    };
    Pipeline::new(Context::new(env), sources)
        .then(Prepare)
        .unwrap()
        .then(Each(Parse))
        .unwrap()
        .into_value()
}

fn error_nodes(tree: &Tree, env: &Env) -> usize {
    let error = env.lang.kinds.lookup("ERROR") as u16;
    tree.root()
        .descendants()
        .filter(|c| c.kind() == error)
        .count()
}

/// A language parses only when production's classification says Parse for
/// its files; Haskell and OCaml are configured but never classified so.
#[test]
fn every_configured_language_parses_when_classified_for_parsing() {
    for (lang, entry) in all_languages() {
        let repo = tempfile::tempdir().unwrap();
        let path = format!("a.{}", entry.extensions()[0]);
        write_all(repo.path(), &[(&path, b"x")]);
        let env = Env::with_limits(lang, Limits::UNLIMITED);
        let classified = inventory::walk(repo.path()).unwrap().into_inner()[0].decision;

        let parsed = parse_repo(&env, repo.path());

        let labels: Vec<_> = parsed.items.iter().map(|p| p.0.label.as_str()).collect();
        match classified {
            Decision::Parse => {
                assert_eq!(labels, [path.as_str()], "{lang:?}");
                assert!(
                    parsed.items[0].0.root().descendants().count() > 0,
                    "{lang:?}"
                );
            }
            _ => assert!(labels.is_empty(), "{lang:?} is not classified for parsing"),
        }
    }
}

#[test]
fn tsx_inside_the_typescript_pipeline_gets_the_tsx_grammar() {
    let repo = tempfile::tempdir().unwrap();
    let jsx = b"export const App = () => <div className=\"a\">hi</div>;\n";
    write_all(
        repo.path(),
        &[("app.tsx", jsx), ("util.ts", b"export const n = 1;\n")],
    );
    let env = Env::with_limits(SupportLang::TypeScript, Limits::UNLIMITED);

    let parsed = parse_repo(&env, repo.path());

    let mut labels: Vec<_> = parsed.items.iter().map(|p| p.0.label.clone()).collect();
    labels.sort();
    assert_eq!(labels, ["app.tsx", "util.ts"]);
    for Parsed(tree) in &parsed.items {
        assert_eq!(error_nodes(tree, &env), 0, "{}", tree.label);
    }
}

#[test]
fn only_files_with_a_grammar_are_parsed() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("src/main.py", b"def f(): pass\n"),
            ("README.md", b"# hi\n"),
            ("logo.png", b"\x89PNG\x00\x00"),
            ("data.json", b"{}"),
        ],
    );
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED);

    let parsed = parse_repo(&env, repo.path());

    let labels: Vec<_> = parsed.items.iter().map(|p| p.0.label.as_str()).collect();
    assert_eq!(labels, ["src/main.py"]);
}

#[test]
fn classify_agrees_with_walk() {
    let repo = tempfile::tempdir().unwrap();
    let files: [(&str, &[u8]); 5] = [
        ("src/main.rs", b"fn main() {}\n"),
        ("Cargo.toml", b"[package]\n"),
        ("README.md", b"# hi\n"),
        ("logo.png", b"\x89PNG\x00\x00"),
        ("dist/app.min.js", b"var a=1;"),
    ];
    write_all(repo.path(), &files);

    let walked = inventory::walk(repo.path()).unwrap().into_inner();
    let paths = walked.iter().map(|e| e.path.clone());
    let mut classified = inventory::classify(repo.path(), paths);
    classified.sort_by(|a, b| a.path.cmp(&b.path));

    assert_eq!(walked.len(), files.len());
    assert_eq!(strip(walked), strip(classified));
}

fn strip(entries: Vec<FileInventoryEntry>) -> Vec<(String, u64, String, Option<String>)> {
    entries
        .into_iter()
        .map(|e| {
            (
                e.path,
                e.size,
                e.decision.to_string(),
                e.label.skip.map(|s| s.to_string()),
            )
        })
        .collect()
}
