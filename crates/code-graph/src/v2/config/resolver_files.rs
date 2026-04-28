//! Predicates for what the indexer extracts and reads from disk.
//!
//! Two predicates, layered:
//!
//! - [`is_required_by_indexer`] — non-source files the resolver pipelines
//!   (generic or custom) load directly from disk: workspace manifests,
//!   ignore files, build configs. Backed by a [`globset::GlobSet`] over
//!   [`INDEXER_REQUIRED_GLOBS`], compiled once.
//! - [`is_extractable`] — the union of "parsable source" and
//!   `is_required_by_indexer`. This is the contract for any caller that
//!   filters archive entries before they reach the pipeline (the server
//!   indexer's archive extractor, the local CLI, anything in between).

use std::path::Path;
use std::sync::LazyLock;

use globset::{Glob, GlobSet, GlobSetBuilder};

use super::lang::Language;
use super::registry::detect_language_from_extension;

/// Glob patterns matched against the basename of each path, at any
/// directory depth.
///
/// `tsconfig.*.json` / `jsconfig.*.json` cover the monorepo convention
/// where the root config `extends` a sibling like `tsconfig.base.json`,
/// `tsconfig.app.json`, etc. The resolver follows the chain via
/// `extends` and reads each one.
pub const INDEXER_REQUIRED_GLOBS: &[&str] = &[
    "Cargo.toml",
    "package.json",
    "rust-analyzer.toml",
    "tsconfig.json",
    "tsconfig.*.json",
    "jsconfig.json",
    "jsconfig.*.json",
    "webpack.config.{js,cjs,mjs,ts}",
    "bun.lock",
    "bun.lockb",
    "bunfig.toml",
    ".gitignore",
    ".ignore",
];

static INDEXER_REQUIRED_GLOBSET: LazyLock<GlobSet> = LazyLock::new(|| {
    let mut builder = GlobSetBuilder::new();
    for pat in INDEXER_REQUIRED_GLOBS {
        builder.add(Glob::new(pat).expect("static indexer-required glob"));
    }
    builder.build().expect("static indexer-required globset")
});

pub const WEBPACK_CONFIG_STEM: &str = "webpack.config";
pub const WEBPACK_CONFIG_EXTENSIONS: &[&str] = &["js", "cjs", "mjs", "ts"];

/// Filenames that flip the JS resolver into Bun-loader extension priority.
pub const BUN_SIGNAL_FILES: &[&str] = &["bun.lock", "bun.lockb", "bunfig.toml"];

/// Returns `true` when `rel_path` is a non-source file the indexer reads
/// directly from disk. Match is on basename only — globs in
/// [`INDEXER_REQUIRED_GLOBS`] are evaluated against the file name, so
/// directory depth never matters.
pub fn is_required_by_indexer(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name() else {
        return false;
    };
    INDEXER_REQUIRED_GLOBSET.is_match(name)
}

/// Returns `Some(Language)` when `rel_path` would be picked up by the
/// parsing pipeline. Mirrors the registry lookup plus per-language
/// `exclude_extensions` (e.g. `*.min.js`, `*_test.go`).
pub fn parsable_language(rel_path: &Path) -> Option<Language> {
    let s = rel_path.to_str()?;
    let ext = rel_path.extension().and_then(|e| e.to_str())?;
    let lang = detect_language_from_extension(ext)?;
    if lang.exclude_extensions().iter().any(|e| s.ends_with(e)) {
        return None;
    }
    Some(lang)
}

/// Returns `true` when `rel_path` is parsable source.
pub fn is_parsable(rel_path: &Path) -> bool {
    parsable_language(rel_path).is_some()
}

/// Returns `true` when `rel_path` should be extracted to disk for the
/// indexer to read: parsable source plus the resolver-required
/// manifests / ignore files / build configs.
///
/// Used by both the server indexer (archive extractor) and the local
/// CLI walker so the two cannot disagree on what the indexer needs.
pub fn is_extractable(rel_path: &Path) -> bool {
    is_parsable(rel_path) || is_required_by_indexer(rel_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn manifests_match_at_any_depth() {
        for path in [
            "Cargo.toml",
            "crates/foo/Cargo.toml",
            "package.json",
            "packages/ui/package.json",
            "tsconfig.json",
            "packages/ui/tsconfig.json",
            "jsconfig.json",
            "rust-analyzer.toml",
            ".cargo/rust-analyzer.toml",
        ] {
            assert!(is_required_by_indexer(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn extended_tsconfig_files_match() {
        // Monorepo convention: tsconfig.json `extends` tsconfig.base.json
        // / tsconfig.app.json / etc. The resolver follows the chain via
        // `extends` and reads each file. All variants must survive.
        for path in [
            "tsconfig.base.json",
            "tsconfig.app.json",
            "tsconfig.lib.json",
            "tsconfig.spec.json",
            "tsconfig.build.json",
            "tsconfig.eslint.json",
            "packages/ui/tsconfig.base.json",
            "jsconfig.base.json",
        ] {
            assert!(is_required_by_indexer(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn bun_signal_files_match() {
        for path in ["bun.lock", "bun.lockb", "bunfig.toml"] {
            assert!(is_required_by_indexer(&p(path)));
        }
    }

    #[test]
    fn ignore_files_match_at_any_depth() {
        for path in [
            ".gitignore",
            "frontend/.gitignore",
            ".ignore",
            "crates/foo/.ignore",
        ] {
            assert!(is_required_by_indexer(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn webpack_configs_match_recognized_extensions() {
        for path in [
            "webpack.config.js",
            "webpack.config.cjs",
            "webpack.config.mjs",
            "webpack.config.ts",
            "ee/webpack.config.js",
        ] {
            assert!(is_required_by_indexer(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn unrelated_files_are_skipped() {
        for path in [
            ".env",
            ".npmrc",
            ".yarnrc",
            ".dockerignore",
            ".gitattributes",
            "go.mod",
            "pyproject.toml",
            "pom.xml",
            "Gemfile",
            "Cargo.lock",
            "webpack.config",
            "webpack.configurator.js",
            "webpack.config.json",
            // tsconfig glob must not over-match.
            "tsconfig",
            "tsconfigfoo.json",
            "jstsconfig.json",
            "tsconfig.toml",
        ] {
            assert!(!is_required_by_indexer(&p(path)), "wrongly kept: {path}");
        }
    }

    #[test]
    fn paths_without_basename_do_not_panic() {
        assert!(!is_required_by_indexer(&p("")));
        assert!(!is_required_by_indexer(&p("/")));
        assert!(!is_extractable(&p("")));
    }

    #[test]
    fn is_parsable_recognizes_supported_extensions() {
        for path in [
            "src/main.rs",
            "lib/foo.py",
            "app/user.rb",
            "pkg/server.go",
            "src/index.ts",
            "src/component.vue",
        ] {
            assert!(is_parsable(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn is_parsable_rejects_excluded_suffixes() {
        assert!(!is_parsable(&p("vendor/jquery.min.js")));
        assert!(!is_parsable(&p("pkg/server_test.go")));
    }

    #[test]
    fn is_parsable_rejects_unsupported_extensions() {
        for path in ["README.md", "image.png", "Cargo.lock", "Makefile"] {
            assert!(!is_parsable(&p(path)), "wrongly kept: {path}");
        }
    }

    #[test]
    fn is_extractable_keeps_source_and_required_files() {
        for path in [
            "src/main.rs",
            "frontend/src/index.ts",
            "Cargo.toml",
            "frontend/package.json",
            "frontend/tsconfig.json",
            ".gitignore",
            "ee/webpack.config.js",
        ] {
            assert!(is_extractable(&p(path)), "missed: {path}");
        }
    }

    #[test]
    fn is_extractable_drops_pure_noise() {
        for path in [
            "README.md",
            "Cargo.lock",
            "frontend/yarn.lock",
            "assets/logo.png",
            "vendor/jquery.min.js",
            "pkg/server_test.go",
            "Makefile",
            "LICENSE",
        ] {
            assert!(!is_extractable(&p(path)), "wrongly kept: {path}");
        }
    }
}
