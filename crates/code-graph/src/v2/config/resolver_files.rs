//! Non-source files the indexer reads from disk.
//!
//! Resolver pipelines (generic or custom) load workspace manifests, ignore
//! files, and build configs to map identifiers to files and to scope the
//! directory walk. They are not parsed as source. Anything that filters
//! files before they reach the pipeline must keep paths matched by
//! [`is_required_by_indexer`] in addition to parsable source.

use std::path::Path;

/// Exact basenames the indexer reads. Matched at any directory depth.
pub const INDEXER_REQUIRED_BASENAMES: &[&str] = &[
    "Cargo.toml",
    "package.json",
    "tsconfig.json",
    "jsconfig.json",
    "bun.lock",
    "bun.lockb",
    "bunfig.toml",
    ".gitignore",
    ".ignore",
];

pub const WEBPACK_CONFIG_STEM: &str = "webpack.config";
pub const WEBPACK_CONFIG_EXTENSIONS: &[&str] = &["js", "cjs", "mjs", "ts"];

/// Filenames that flip the JS resolver into Bun-loader extension priority.
pub const BUN_SIGNAL_FILES: &[&str] = &["bun.lock", "bun.lockb", "bunfig.toml"];

/// Returns `true` when `rel_path` is a non-source file the indexer reads
/// directly from disk. Match is on basename only.
pub fn is_required_by_indexer(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    INDEXER_REQUIRED_BASENAMES.contains(&name) || is_webpack_config_basename(name)
}

fn is_webpack_config_basename(basename: &str) -> bool {
    basename
        .strip_prefix(WEBPACK_CONFIG_STEM)
        .and_then(|rest| rest.strip_prefix('.'))
        .is_some_and(|ext| WEBPACK_CONFIG_EXTENSIONS.contains(&ext))
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
        ] {
            assert!(!is_required_by_indexer(&p(path)), "wrongly kept: {path}");
        }
    }

    #[test]
    fn paths_without_basename_do_not_panic() {
        assert!(!is_required_by_indexer(&p("")));
        assert!(!is_required_by_indexer(&p("/")));
    }
}
