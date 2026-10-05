//! One-shot filesystem probe for a JS workspace.
//!
//! `WorkspaceProbe::load` reads every manifest/config file the pipeline
//! cares about *exactly once* at the start of `JsPipeline::process_files`
//! and hands the parsed results to every downstream consumer:
//! `JsCrossFileResolver`, tsconfig discovery, the webpack evaluator, and
//! `is_bun` detection.

use crate::v2::config::Role;
use orbit_utils::files::{Kind, Vfs};
use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::constants::{BUN_SIGNAL_FILES, is_webpack_config_path};

/// Every manifest/config fact the JS pipeline derives from the
/// repository root, computed once.
pub struct WorkspaceProbe {
    pub(crate) vfs: Arc<Vfs<Role>>,
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
    pub fn load(vfs: Arc<Vfs<Role>>, indexed_paths: &[String]) -> Self {
        let root_dir = PathBuf::from("/");
        let manifest_raw = vfs
            .stat(Path::new("package.json"))
            .ok()
            .filter(|stat| stat.len <= super::extract::MAX_FILE_BYTES)
            .and_then(|_| vfs.read(Path::new("package.json")).ok())
            .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok());
        let existing_file = |name: &str| {
            vfs.stat(Path::new(name))
                .ok()
                .filter(|stat| stat.kind == Kind::File)
                .map(|stat| stat.path)
        };
        let tsconfig_path = existing_file("tsconfig.json");
        let jsconfig_path = existing_file("jsconfig.json");

        // webpack configs live anywhere in the repo — pop-culture
        // convention is root or `config/`, monolith convention is
        // `ee/`, and we have seen them in package sub-folders too. We
        // reuse the indexed file list instead of re-walking the tree.
        let webpack_configs = indexed_paths
            .iter()
            .filter(|path| is_webpack_config_path(path))
            .map(|relative| root_dir.join(relative))
            .collect();

        let bun_signal_present = BUN_SIGNAL_FILES.iter().any(|name| {
            indexed_paths.iter().any(|p| p == name)
                || vfs
                    .stat(Path::new(name))
                    .is_ok_and(|stat| stat.kind == Kind::File && stat.link.is_none())
        });

        Self {
            vfs,
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
    /// Always `Manual` and pinned to a file inside the repo, or `None`
    /// if neither config was discovered. `Auto` walks parent directories
    /// past `root_dir` and would pick up any ambient `tsconfig.json` from
    /// the server's filesystem; a hostile repo cannot make us honor a
    /// tsconfig we did not find underneath `root_dir`.
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
