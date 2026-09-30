//! One-shot probe of a JS workspace.
//!
//! `WorkspaceProbe::load` reads every manifest/config file the pipeline
//! cares about *exactly once* at the start of `JsPipeline::process_files`
//! and hands the parsed results to every downstream consumer:
//! `JsCrossFileResolver`, tsconfig discovery, the webpack evaluator, and
//! `is_bun` detection.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use orbit_utils::files::Vfs;
use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};

use super::constants::{BUN_SIGNAL_FILES, is_webpack_config_path};
use crate::v2::pipeline::VIRTUAL_ROOT;

/// Every manifest/config fact the JS pipeline derives from the
/// repository root, computed once.
pub struct WorkspaceProbe {
    vfs: Arc<Vfs>,
    /// Raw `package.json` text. Kept for substring probes (e.g.
    /// `"@types/bun"`) without re-reading.
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
    pub fn load(vfs: Arc<Vfs>, indexed_paths: &[String]) -> Self {
        let root = Path::new(VIRTUAL_ROOT);
        let manifest_raw = read_bounded(&vfs, &root.join("package.json"));
        let tsconfig_path = existing_file(&vfs, "tsconfig.json");
        let jsconfig_path = existing_file(&vfs, "jsconfig.json");

        // webpack configs live anywhere in the repo — pop-culture
        // convention is root or `config/`, monolith convention is
        // `ee/`, and we have seen them in package sub-folders too. We
        // reuse the indexed file list instead of re-walking the tree.
        let webpack_configs = indexed_paths
            .iter()
            .filter(|path| is_webpack_config_path(path))
            .map(|relative| root.join(relative))
            .collect();

        let bun_signal_present = BUN_SIGNAL_FILES
            .iter()
            .any(|name| indexed_paths.iter().any(|p| p == name) || vfs.is_file(&root.join(name)));

        Self {
            vfs,
            manifest_raw,
            tsconfig_path,
            jsconfig_path,
            webpack_configs,
            bun_signal_present,
        }
    }

    pub fn root_dir(&self) -> &Path {
        Path::new(VIRTUAL_ROOT)
    }

    pub fn vfs(&self) -> &Arc<Vfs> {
        &self.vfs
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
    /// Always `Manual` and pinned to a file inside the repo, or `None`
    /// if neither config was discovered. `Auto` would walk parent
    /// directories, and the repository filesystem has nothing above `/`.
    pub fn tsconfig_discovery(&self) -> Option<TsconfigDiscovery> {
        if let Some(jsconfig) = &self.jsconfig_path {
            return Some(TsconfigDiscovery::Manual(TsconfigOptions {
                config_file: jsconfig.clone(),
                references: TsconfigReferences::Auto,
            }));
        }
        self.tsconfig_path.as_ref().map(|tsconfig| {
            TsconfigDiscovery::Manual(TsconfigOptions {
                config_file: tsconfig.clone(),
                references: TsconfigReferences::Auto,
            })
        })
    }

    pub fn webpack_configs(&self) -> &[PathBuf] {
        &self.webpack_configs
    }
}

fn existing_file(vfs: &Vfs, filename: &str) -> Option<PathBuf> {
    let path = Path::new(VIRTUAL_ROOT).join(filename);
    vfs.is_file(&path).then_some(path)
}

/// Read a manifest-sized file or skip it. Guards against a hostile
/// `package.json` the size of the whole repo.
fn read_bounded(vfs: &Vfs, path: &Path) -> Option<String> {
    let meta = vfs.metadata(path).ok()?;
    if meta.len > super::extract::MAX_FILE_BYTES {
        return None;
    }
    vfs.read_to_string(path).ok()
}
