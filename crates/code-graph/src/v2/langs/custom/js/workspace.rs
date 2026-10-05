//! Workspace configuration comes from the VFS inventory without another walk.
//! Package metadata is read once; the resolver and evaluator share the store.

use crate::v2::config::Role;
use orbit_utils::vfs::{Kind, Vfs};
use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::constants::{BUN_SIGNAL_FILES, is_webpack_config_path};

pub struct WorkspaceProbe {
    pub(crate) vfs: Arc<Vfs<Role>>,
    manifest_raw: Option<String>,
    tsconfig_path: Option<PathBuf>,
    jsconfig_path: Option<PathBuf>,
    webpack_configs: Vec<PathBuf>,
    bun_signal_present: bool,
}

impl WorkspaceProbe {
    pub fn load(vfs: Arc<Vfs<Role>>, indexed_paths: &[String]) -> Self {
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

        let webpack_configs = indexed_paths
            .iter()
            .filter(|path| is_webpack_config_path(path))
            .map(|relative| Path::new("/").join(relative))
            .collect();

        let bun_signal_present = BUN_SIGNAL_FILES.iter().any(|name| {
            indexed_paths.iter().any(|p| p == name)
                || vfs
                    .stat(Path::new(name))
                    .is_ok_and(|stat| stat.kind == Kind::File && stat.link.is_none())
        });

        Self {
            vfs,
            manifest_raw,
            tsconfig_path,
            jsconfig_path,
            webpack_configs,
            bun_signal_present,
        }
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

    pub fn tsconfig_discovery(&self) -> Option<TsconfigDiscovery> {
        self.jsconfig_path
            .as_ref()
            .or(self.tsconfig_path.as_ref())
            .map(|config| {
                TsconfigDiscovery::Manual(TsconfigOptions {
                    config_file: config.clone(),
                    references: TsconfigReferences::Auto,
                })
            })
    }

    pub fn webpack_configs(&self) -> &[PathBuf] {
        &self.webpack_configs
    }
}
