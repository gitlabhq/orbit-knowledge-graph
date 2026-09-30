use std::path::Path;

use code_graph_incremental::pipeline::{Each, Parse, Parsed, Prepare, Sources, Workset};
use code_graph_incremental::tree::Tree;
use code_graph_incremental::treesitter::{SupportLang, all_languages};
use code_graph_incremental::{Context, Env, Limits, Pipeline, inventory};
use orbit_utils::files::{Decision, File};

fn write_all(root: &Path, files: &[(&str, &[u8])]) {
    for (path, content) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
}

fn parse_repo(env: &Env, root: &Path) -> Workset<Vec<Parsed>> {
    let (repo, entries) = inventory::walk(root).unwrap();
    let sources = Sources {
        repo,
        entries: entries.into_inner(),
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
        let env = Env::with_limits(lang, Limits::UNLIMITED).unwrap();
        let classified = inventory::walk(repo.path()).unwrap().1[0].decision;

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
    let env = Env::with_limits(SupportLang::TypeScript, Limits::UNLIMITED).unwrap();

    let parsed = parse_repo(&env, repo.path());

    let mut labels: Vec<_> = parsed.items.iter().map(|p| p.0.label.clone()).collect();
    labels.sort();
    assert_eq!(labels, ["app.tsx", "util.ts"]);
    for Parsed(tree) in &parsed.items {
        assert_eq!(error_nodes(tree, &env), 0, "{}", tree.label);
    }
}

#[test]
fn only_this_familys_files_are_parsed() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("src/main.py", b"def f(): pass\n"),
            ("src/main.go", b"package main\n"),
            ("README.md", b"# hi\n"),
            ("logo.png", b"\x89PNG\x00\x00"),
            ("data.json", b"{}"),
        ],
    );
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();

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
    std::os::unix::fs::symlink("src/main.rs", repo.path().join("link.rs")).unwrap();

    let walked = inventory::walk(repo.path()).unwrap().1.into_inner();
    let paths = walked.iter().map(|e| e.path.clone()).collect();
    let classified = inventory::classify(repo.path(), paths)
        .unwrap()
        .1
        .into_inner();

    assert_eq!(walked.len(), files.len() + 1);
    assert_eq!(strip(walked), strip(classified));
}

fn strip(entries: Vec<File>) -> Vec<(String, u64, String, Option<String>)> {
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

#[test]
fn a_family_parses_each_member_with_its_own_grammar() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("User.java", b"public class User { }\n"),
            ("Service.kt", b"class Service(val user: User)\n"),
            ("main.py", b"x = 1\n"),
        ],
    );
    let env = Env::with_limits(SupportLang::Kotlin, Limits::UNLIMITED).unwrap();
    assert_eq!(
        env.members,
        [SupportLang::Java, SupportLang::Kotlin, SupportLang::Scala]
    );

    let parsed = parse_repo(&env, repo.path());

    let mut labels: Vec<_> = parsed.items.iter().map(|p| p.0.label.clone()).collect();
    labels.sort();
    assert_eq!(labels, ["Service.kt", "User.java"]);
    for Parsed(tree) in &parsed.items {
        assert_eq!(error_nodes(tree, &env), 0, "{}", tree.label);
    }
}

#[test]
fn languages_in_one_family_share_a_qualified_name_separator() {
    for (lang, _) in all_languages() {
        for member in lang.family_members() {
            assert_eq!(
                member.fqn_separator(),
                lang.fqn_separator(),
                "{member:?} and {lang:?} are both {:?}",
                lang.family()
            );
        }
    }
}
