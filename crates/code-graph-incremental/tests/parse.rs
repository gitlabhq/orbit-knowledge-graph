use std::path::Path;

use code_graph_incremental::language::{SupportLang, all_languages};
use code_graph_incremental::tree::Tree;
use code_graph_incremental::{Engine, Limits, Sentinel, driver, frontend};
use orbit_utils::vfs::{Decision, Vfs};
mod common;

fn write_all(root: &Path, files: &[(&str, &[u8])]) {
    for (path, content) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
}

fn parse_repo(env: &Engine, root: &Path) -> Vec<Tree> {
    let repository = common::walk(root);
    driver::candidates(&repository, 0)
        .filter(|file| SupportLang::from_path(&file.path).is_some_and(|l| env.in_family(l)))
        .map(|file| {
            let source = String::from_utf8(repository.read(&file.path).unwrap().to_vec()).unwrap();
            frontend::parse(env, &file.path, &source, &Sentinel::disabled())
                .unwrap()
                .0
        })
        .collect()
}

fn error_nodes(tree: &Tree, env: &Engine) -> usize {
    let error = env.lang.kinds.lookup("ERROR") as u16;
    tree.root()
        .descendants()
        .filter(|c| c.kind() == error)
        .count()
}

#[test]
fn unread_sources_and_manifests_remain_in_the_graph_and_report() {
    struct LazyFiles;
    impl orbit_utils::vfs::Pass for LazyFiles {
        type Tag = ();
        fn metadata(&self, _: &orbit_utils::vfs::File<'_, ()>) -> Decision<()> {
            Decision::KeepLazy(())
        }
    }
    let root = tempfile::tempdir().unwrap();
    write_all(
        root.path(),
        &[("main.ts", b"export const x = 1;"), ("package.json", b"{}")],
    );
    let repository = std::sync::Arc::new(
        orbit_utils::vfs::Vfs::load(
            orbit_utils::vfs::sources::Directory(root.path()),
            LazyFiles,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
    );
    for path in ["main.ts", "package.json"] {
        std::fs::remove_file(root.path().join(path)).unwrap();
    }
    let env = Engine::builder(SupportLang::TypeScript)
        .limits(Limits::UNLIMITED)
        .build()
        .unwrap();
    let (state, report) = driver::index(&env, &repository, &Sentinel::disabled()).unwrap();
    assert_eq!(report.unread_files, ["main.ts", "package.json"]);
    assert!(
        state.configs.is_empty(),
        "unread manifests must not become SourceFile inputs"
    );
    assert_eq!(state.trees.len(), 2);
    for tree in state.trees {
        let reason = tree.root().tag(env.lang.syms.lookup("reason")).unwrap();
        assert_eq!(env.lang.syms.resolve(reason), "fault_file_read");
    }
}

/// A language parses only when production's classification says Parse for
/// its files; Haskell and OCaml are configured but never classified so.
#[test]
fn every_configured_language_parses_when_classified_for_parsing() {
    for (lang, entry) in all_languages() {
        let repo = tempfile::tempdir().unwrap();
        let path = format!("a.{}", entry.extensions()[0]);
        let source: &[u8] = match lang {
            SupportLang::Markdown => b"# x",
            _ => b"x",
        };
        write_all(repo.path(), &[(&path, source)]);
        let env = Engine::builder(lang)
            .limits(Limits::UNLIMITED)
            .build()
            .unwrap();

        let parsed = parse_repo(&env, repo.path());

        let labels: Vec<_> = parsed.iter().map(|tree| tree.label.as_str()).collect();
        match lang.has_rules() {
            true => {
                assert_eq!(labels, [path.as_str()], "{lang:?}");
                assert!(parsed[0].root().descendants().count() > 0, "{lang:?}");
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
    let env = Engine::builder(SupportLang::TypeScript)
        .limits(Limits::UNLIMITED)
        .build()
        .unwrap();

    let parsed = parse_repo(&env, repo.path());

    let mut labels: Vec<_> = parsed.iter().map(|tree| tree.label.clone()).collect();
    labels.sort();
    assert_eq!(labels, ["app.tsx", "util.ts"]);
    for tree in &parsed {
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
    let env = Engine::builder(SupportLang::Python)
        .limits(Limits::UNLIMITED)
        .build()
        .unwrap();

    let parsed = parse_repo(&env, repo.path());

    let labels: Vec<_> = parsed.iter().map(|tree| tree.label.as_str()).collect();
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

    let walked = common::walk(repo.path());
    let paths = walked.files().map(|e| e.path.to_string());
    let classified = common::load(orbit_utils::vfs::sources::Diff {
        root: repo.path(),
        paths: paths.collect(),
    });

    assert_eq!(walked.usage().files, files.len() + 1);
    assert_eq!(strip(walked), strip(classified));
}

fn strip(entries: std::sync::Arc<Vfs<()>>) -> Vec<(String, u64, Decision<()>)> {
    entries
        .files()
        .map(|e| (e.path.to_string(), e.size, e.decision()))
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
    let env = Engine::builder(SupportLang::Kotlin)
        .limits(Limits::UNLIMITED)
        .build()
        .unwrap();
    assert_eq!(
        env.members,
        [SupportLang::Java, SupportLang::Kotlin, SupportLang::Scala]
    );

    let parsed = parse_repo(&env, repo.path());

    let mut labels: Vec<_> = parsed.iter().map(|tree| tree.label.clone()).collect();
    labels.sort();
    assert_eq!(labels, ["Service.kt", "User.java"]);
    for tree in &parsed {
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

#[test]
fn container_build_files_are_recognized_by_name() {
    for path in [
        "Dockerfile",
        "Containerfile",
        "Dockerfile.dev",
        "svc/api/Containerfile.prod",
        "build/app.dockerfile",
        "dockerfile",
        "DOCKERFILE",
        "app.Dockerfile",
        "build.Containerfile",
        "Dockerfile.mri.release",
    ] {
        assert_eq!(
            SupportLang::from_path(path),
            Some(SupportLang::Dockerfile),
            "{path}"
        );
    }
    for path in [
        "Dockerfile-notes.md",
        "MyDockerfile",
        "Dockerfile.md",
        "Dockerfile.tt",
        "Dockerfile.mri.erb",
        "Containerfile.template",
        "Dockerfile.wip.diff",
    ] {
        assert_ne!(
            SupportLang::from_path(path),
            Some(SupportLang::Dockerfile),
            "{path}"
        );
    }
    assert_eq!(
        SupportLang::from_path("Dockerfile.py"),
        Some(SupportLang::Python)
    );
}
