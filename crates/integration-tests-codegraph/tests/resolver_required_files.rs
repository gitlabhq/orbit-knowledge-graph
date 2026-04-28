//! Locks in the contract between any upstream extraction filter and the
//! indexer pipeline: paths matched by `is_required_by_indexer` must
//! survive whatever filter the caller applies, otherwise resolver
//! pipelines (generic or custom) silently degrade.

use std::path::Path;
use std::sync::Arc;

use code_graph::v2::config::{detect_language_from_path, is_required_by_indexer};
use code_graph::v2::{BatchSink, GraphConverter, NullSink, Pipeline, PipelineConfig, SinkError};

/// Stand-in for `is_parsable` from !1110. Mirrors the same logic so the
/// test is independent of that branch's exact shape.
fn is_parsable_simulated(rel_path: &Path) -> bool {
    let path_str = rel_path.to_string_lossy();
    let Some(lang) = detect_language_from_path(&path_str) else {
        return false;
    };
    !lang
        .exclude_extensions()
        .iter()
        .any(|excl| path_str.ends_with(excl))
}

fn keep(rel_path: &Path) -> bool {
    is_parsable_simulated(rel_path) || is_required_by_indexer(rel_path)
}

#[test]
fn filter_preserves_indexer_inputs() {
    let archive_entries = [
        // Source
        "src/main.rs",
        "crates/foo/src/lib.rs",
        "crates/foo/src/bin/foo.rs",
        "frontend/src/index.ts",
        "frontend/src/utils.ts",
        "frontend/src/component.vue",
        "frontend/queries/user.graphql",
        "tests/integration_test.rs",
        "scripts/build.py",
        // Indexer-required
        "Cargo.toml",
        "crates/foo/Cargo.toml",
        "crates/bar/Cargo.toml",
        "frontend/package.json",
        "frontend/tsconfig.json",
        "frontend/jsconfig.json",
        "config/webpack.config.js",
        "config/webpack.config.ts",
        "ee/webpack.config.cjs",
        "bun.lockb",
        "bunfig.toml",
        ".gitignore",
        "frontend/.gitignore",
        ".ignore",
        // Noise
        "README.md",
        "CHANGELOG.md",
        "docs/architecture.md",
        "assets/logo.png",
        "assets/banner.gif",
        "assets/icon.svg",
        "Cargo.lock",
        "frontend/yarn.lock",
        "vendor/jquery.min.js",
        "pkg/server_test.go",
        "Makefile",
        "LICENSE",
        ".env",
        ".dockerignore",
    ];

    let survived: Vec<&str> = archive_entries
        .iter()
        .filter(|p| keep(Path::new(p)))
        .copied()
        .collect();

    let must_survive = [
        "Cargo.toml",
        "crates/foo/Cargo.toml",
        "crates/bar/Cargo.toml",
        "frontend/package.json",
        "frontend/tsconfig.json",
        "frontend/jsconfig.json",
        "config/webpack.config.js",
        "config/webpack.config.ts",
        "ee/webpack.config.cjs",
        "bun.lockb",
        "bunfig.toml",
        ".gitignore",
        "frontend/.gitignore",
        ".ignore",
        "src/main.rs",
        "crates/foo/src/lib.rs",
        "crates/foo/src/bin/foo.rs",
        "frontend/src/index.ts",
        "frontend/src/utils.ts",
        "frontend/src/component.vue",
        "frontend/queries/user.graphql",
        "tests/integration_test.rs",
        "scripts/build.py",
    ];
    for path in must_survive {
        assert!(
            survived.contains(&path),
            "filtered out: {path}\nsurvivors: {survived:#?}"
        );
    }

    let must_drop = [
        "README.md",
        "CHANGELOG.md",
        "docs/architecture.md",
        "assets/logo.png",
        "assets/banner.gif",
        "assets/icon.svg",
        "Cargo.lock",
        "frontend/yarn.lock",
        "vendor/jquery.min.js",
        "pkg/server_test.go",
        "Makefile",
        "LICENSE",
        ".env",
        ".dockerignore",
    ];
    for path in must_drop {
        assert!(!survived.contains(&path), "noise survived: {path}");
    }
}

struct PassthroughConverter;

impl GraphConverter for PassthroughConverter {
    fn convert(
        &self,
        _graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, SinkError> {
        Ok(Vec::new())
    }
}

#[test]
fn pipeline_runs_against_filter_simulated_tree() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let root = tmp.path();

    let fixtures: &[(&str, &str)] = &[
        (
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/lib\", \"crates/app\"]\nresolver = \"2\"\n",
        ),
        (
            "crates/lib/Cargo.toml",
            "[package]\nname = \"lib\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        ),
        (
            "crates/lib/src/lib.rs",
            "pub fn greet() -> &'static str { \"hi\" }\n",
        ),
        (
            "crates/app/Cargo.toml",
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nlib = { path = \"../lib\" }\n",
        ),
        (
            "crates/app/src/main.rs",
            "fn main() { println!(\"{}\", lib::greet()); }\n",
        ),
        (
            "frontend/package.json",
            "{\"name\":\"frontend\",\"version\":\"0.0.0\"}\n",
        ),
        (
            "frontend/tsconfig.json",
            "{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"src/*\"]}}}\n",
        ),
        (
            "frontend/src/utils.ts",
            "export function helper() { return 42; }\n",
        ),
        (
            "frontend/src/main.ts",
            "import { helper } from '@/utils';\nexport function run() { return helper(); }\n",
        ),
        (".gitignore", "node_modules/\ntarget/\n"),
        ("README.md", "# project\n"),
        ("Cargo.lock", "# generated\n"),
        ("frontend/yarn.lock", "# generated\n"),
        ("assets/logo.png", "fake-png-bytes"),
        ("vendor/jquery.min.js", "/* minified */\n"),
        ("pkg/server_test.go", "package pkg\n"),
    ];

    for (rel, contents) in fixtures {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, contents).unwrap();
    }

    let mut survived = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(root).unwrap().to_path_buf();
        if keep(&rel) {
            survived.push(rel);
        } else {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let survived_strs: Vec<String> = survived
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    for required in [
        "Cargo.toml",
        "crates/lib/Cargo.toml",
        "crates/app/Cargo.toml",
        "frontend/package.json",
        "frontend/tsconfig.json",
        ".gitignore",
    ] {
        assert!(
            survived_strs.iter().any(|p| p == required),
            "deleted: {required}\nsurvivors: {survived_strs:#?}"
        );
    }
    for noise in [
        "Cargo.lock",
        "frontend/yarn.lock",
        "README.md",
        "assets/logo.png",
        "vendor/jquery.min.js",
        "pkg/server_test.go",
    ] {
        assert!(
            !survived_strs.iter().any(|p| p == noise),
            "noise survived: {noise}"
        );
    }

    let config = PipelineConfig::default();
    let converter: Arc<dyn GraphConverter> = Arc::new(PassthroughConverter);
    let sink: Arc<dyn BatchSink> = Arc::new(NullSink);
    let result = Pipeline::run(root, config, converter, sink);
    assert!(
        result.errors.is_empty(),
        "pipeline errors: {:#?}",
        result.errors
    );
}
