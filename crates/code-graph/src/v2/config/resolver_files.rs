//! Files the custom resolver pipelines load directly from disk.
//!
//! These are *not* parsed as source — they configure how the resolvers map
//! identifiers to files (Cargo workspaces, package.json `exports`, tsconfig
//! `paths`, webpack aliases) or feed the directory walker (`.gitignore`).
//! If they are missing from the working tree the resolvers silently
//! degrade: cross-crate Rust resolution falls back to standalone parsing,
//! JS bare-specifier resolution stops finding modules, the walker descends
//! into `node_modules` / `target`, and so on.
//!
//! Any caller that filters files before they reach the pipeline (the
//! archive extractor in `indexer::modules::code::repository::cache`,
//! external pre-extract filters) must keep paths matched by
//! [`is_required_for_resolvers`] on disk in addition to anything
//! [`is_parsable`](super::is_parsable) accepts.
//!
//! ## Adding a file
//!
//! When a new custom resolver starts reading a manifest from disk, append
//! it here in the same MR. The companion integration test
//! `resolver_required_files` in `crates/integration-tests-codegraph` pins
//! the contract so an out-of-band filter cannot drop these paths without
//! breaking the build.

use std::path::Path;

/// Exact basenames the resolvers load. Matched at any directory depth.
///
/// - `Cargo.toml` — Rust workspace catalog and dependency manifest discovery
///   (`langs/custom/rust/workspace.rs`, `manifest.rs`).
/// - `package.json` — JS package boundary, `exports`/`main`/`types` maps,
///   `@types/bun` probe (`langs/custom/js/workspace.rs`,
///   `resolve/specifier.rs`).
/// - `tsconfig.json` / `jsconfig.json` — TypeScript path aliases, `extends`
///   and `references` chains (`langs/custom/js/workspace.rs`).
/// - `bun.lock`, `bun.lockb`, `bunfig.toml` — Bun detection switches the
///   resolver's extension priority order
///   (`langs/custom/js/constants.rs::BUN_SIGNAL_FILES`).
/// - `.gitignore` — consumed by `WalkBuilder::git_ignore(true)` in
///   `pipeline.rs::walk_and_group`. Without it the walker descends into
///   `node_modules`, `target`, `vendor`, etc.
/// - `.ignore` — consumed by `WalkBuilder::standard_filters(true)` in
///   `langs/custom/rust/workspace.rs::discover_manifest_paths`. Same
///   blast radius for the Rust manifest walk.
pub const RESOLVER_REQUIRED_BASENAMES: &[&str] = &[
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

/// Webpack config basename stem. Matched as `<stem>.{js,cjs,mjs,ts}`.
/// Re-exported from `langs/custom/js/constants.rs` so the upstream
/// extraction filter and the JS pipeline cannot disagree on which
/// configs exist.
pub const WEBPACK_CONFIG_STEM: &str = "webpack.config";

/// Extensions paired with [`WEBPACK_CONFIG_STEM`]. Re-used by
/// `langs::custom::js::constants` for in-tree config discovery.
pub const WEBPACK_CONFIG_EXTENSIONS: &[&str] = &["js", "cjs", "mjs", "ts"];

/// Filenames whose presence flips the JS resolver into Bun-loader
/// extension priority. Re-used by `langs::custom::js::constants`.
pub const BUN_SIGNAL_FILES: &[&str] = &["bun.lock", "bun.lockb", "bunfig.toml"];

/// Returns `true` when `rel_path` is a non-source file that one of the
/// custom resolver pipelines reads from disk.
///
/// Match is on basename only — directory depth is irrelevant because the
/// JS resolver walks every `package.json` under the repo root and Cargo
/// workspaces can sit in subdirectories.
pub fn is_required_for_resolvers(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if RESOLVER_REQUIRED_BASENAMES.contains(&name) {
        return true;
    }
    is_webpack_config_basename(name)
}

fn is_webpack_config_basename(basename: &str) -> bool {
    let Some(rest) = basename.strip_prefix(WEBPACK_CONFIG_STEM) else {
        return false;
    };
    let Some(ext) = rest.strip_prefix('.') else {
        return false;
    };
    WEBPACK_CONFIG_EXTENSIONS.contains(&ext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn rust_manifests_are_required_at_any_depth() {
        assert!(is_required_for_resolvers(&p("Cargo.toml")));
        assert!(is_required_for_resolvers(&p("crates/foo/Cargo.toml")));
        assert!(is_required_for_resolvers(&p(
            "vendor/nested/deep/Cargo.toml"
        )));
    }

    #[test]
    fn js_manifests_are_required_at_any_depth() {
        assert!(is_required_for_resolvers(&p("package.json")));
        assert!(is_required_for_resolvers(&p("packages/ui/package.json")));
        assert!(is_required_for_resolvers(&p("tsconfig.json")));
        assert!(is_required_for_resolvers(&p("packages/ui/tsconfig.json")));
        assert!(is_required_for_resolvers(&p("jsconfig.json")));
    }

    #[test]
    fn bun_signal_files_are_required() {
        assert!(is_required_for_resolvers(&p("bun.lock")));
        assert!(is_required_for_resolvers(&p("bun.lockb")));
        assert!(is_required_for_resolvers(&p("bunfig.toml")));
    }

    #[test]
    fn ignore_files_are_required_at_any_depth() {
        // .gitignore must survive extraction or the walker descends into
        // node_modules/target/vendor and the Rust manifest discovery
        // walks unrelated nested workspaces.
        assert!(is_required_for_resolvers(&p(".gitignore")));
        assert!(is_required_for_resolvers(&p("frontend/.gitignore")));
        assert!(is_required_for_resolvers(&p(".ignore")));
        assert!(is_required_for_resolvers(&p("crates/foo/.ignore")));
    }

    #[test]
    fn webpack_configs_are_required_for_all_recognized_extensions() {
        assert!(is_required_for_resolvers(&p("webpack.config.js")));
        assert!(is_required_for_resolvers(&p("webpack.config.cjs")));
        assert!(is_required_for_resolvers(&p("webpack.config.mjs")));
        assert!(is_required_for_resolvers(&p("webpack.config.ts")));
        assert!(is_required_for_resolvers(&p("ee/webpack.config.js")));
    }

    #[test]
    fn unrelated_dotfiles_are_not_required() {
        // .env, .npmrc, .yarnrc are NOT read by any current resolver. If
        // a future resolver adds them, update this list and the test.
        assert!(!is_required_for_resolvers(&p(".env")));
        assert!(!is_required_for_resolvers(&p(".npmrc")));
        assert!(!is_required_for_resolvers(&p(".yarnrc")));
        assert!(!is_required_for_resolvers(&p(".dockerignore")));
        assert!(!is_required_for_resolvers(&p(".gitattributes")));
    }

    #[test]
    fn unrelated_manifests_are_not_required() {
        // None of these are read today. Adding them without a
        // corresponding resolver is dead weight and will rot.
        assert!(!is_required_for_resolvers(&p("go.mod")));
        assert!(!is_required_for_resolvers(&p("pyproject.toml")));
        assert!(!is_required_for_resolvers(&p("pom.xml")));
        assert!(!is_required_for_resolvers(&p("Gemfile")));
        assert!(!is_required_for_resolvers(&p("Cargo.lock")));
    }

    #[test]
    fn webpack_lookalikes_are_not_matched() {
        assert!(!is_required_for_resolvers(&p("webpack.config")));
        assert!(!is_required_for_resolvers(&p("webpack.configurator.js")));
        assert!(!is_required_for_resolvers(&p("webpack.config.json")));
    }

    #[test]
    fn paths_without_basename_do_not_panic() {
        assert!(!is_required_for_resolvers(&p("")));
        assert!(!is_required_for_resolvers(&p("/")));
    }
}
