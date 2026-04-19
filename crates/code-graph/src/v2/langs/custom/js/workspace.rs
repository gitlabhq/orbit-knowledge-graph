//! One-shot filesystem probe for a JS workspace.
//!
//! `WorkspaceProbe::load` reads every manifest/config file the pipeline
//! cares about *exactly once* at the start of `JsPipeline::process_files`
//! and hands the parsed results to every downstream consumer:
//! `JsCrossFileResolver`, tsconfig discovery, the webpack evaluator, and
//! `is_bun` detection.
//!
//! Before this existed, the pipeline re-read `package.json` twice, probed
//! seven manifest filenames in one place and three more in another, and
//! walked eight webpack-config candidates from inside the evaluator. All
//! of that collapses into this struct.

use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};
use std::path::{Path, PathBuf};

use super::constants::{BUN_SIGNAL_FILES, WEBPACK_CONFIG_CANDIDATES};

/// Every manifest/config fact the JS pipeline derives from the
/// repository root, computed once.
pub struct WorkspaceProbe {
    root_dir: PathBuf,
    /// Raw `package.json` text. Kept for substring probes (e.g.
    /// `"@types/bun"`) without re-reading from disk.
    manifest_raw: Option<String>,
    tsconfig_path: Option<PathBuf>,
    jsconfig_path: Option<PathBuf>,
    webpack_configs: Vec<PathBuf>,
    bun_signal_present: bool,
}

impl WorkspaceProbe {
    /// Load every interesting manifest / config once. `indexed_paths`
    /// are the repo-relative files the outer walker already surfaced;
    /// the probe does not re-walk the tree.
    pub fn load(root_dir: &Path, indexed_paths: &[String]) -> Self {
        // Canonicalize once so downstream path containment checks
        // (webpack evaluator, specifier resolver) all operate in the
        // same absolute form.
        let root_dir = std::fs::canonicalize(root_dir).unwrap_or_else(|_| root_dir.to_path_buf());

        let manifest_raw = std::fs::read_to_string(root_dir.join("package.json")).ok();

        let tsconfig_path = existing_file(&root_dir, "tsconfig.json");
        let jsconfig_path = existing_file(&root_dir, "jsconfig.json");

        let webpack_configs = WEBPACK_CONFIG_CANDIDATES
            .iter()
            .filter_map(|relative| {
                let path = root_dir.join(relative);
                path.is_file().then_some(path)
            })
            .collect();

        let bun_signal_present = BUN_SIGNAL_FILES
            .iter()
            .any(|name| indexed_paths.iter().any(|p| p == name) || root_dir.join(name).is_file());

        Self {
            root_dir,
            manifest_raw,
            tsconfig_path,
            jsconfig_path,
            webpack_configs,
            bun_signal_present,
        }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn is_bun(&self) -> bool {
        self.bun_signal_present
            || self
                .manifest_raw
                .as_deref()
                .is_some_and(|raw| raw.contains("\"@types/bun\""))
    }

    pub fn has_tsconfig(&self) -> bool {
        self.tsconfig_path.is_some() || self.jsconfig_path.is_some()
    }

    /// Resolver configuration for the tsconfig/jsconfig the repo exposes.
    ///
    /// `TsconfigDiscovery::Auto` only searches for `tsconfig.json`;
    /// `jsconfig.json` is functionally identical but needs explicit wiring.
    pub fn tsconfig_discovery(&self) -> TsconfigDiscovery {
        if let Some(jsconfig) = &self.jsconfig_path {
            return TsconfigDiscovery::Manual(TsconfigOptions {
                config_file: jsconfig.clone(),
                references: TsconfigReferences::Auto,
            });
        }
        TsconfigDiscovery::Auto
    }

    pub fn webpack_configs(&self) -> &[PathBuf] {
        &self.webpack_configs
    }
}

fn existing_file(root_dir: &Path, filename: &str) -> Option<PathBuf> {
    let path = root_dir.join(filename);
    path.is_file().then_some(path)
}
