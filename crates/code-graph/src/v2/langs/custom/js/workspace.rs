use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};
use std::path::{Path, PathBuf};

use super::constants::{BUN_SIGNAL_FILES, is_webpack_config_path};

pub struct WorkspaceProbe {
    root_dir: PathBuf,
    manifest_raw: Option<String>,
    tsconfig_path: Option<PathBuf>,
    jsconfig_path: Option<PathBuf>,
    webpack_configs: Vec<PathBuf>,
    bun_signal_present: bool,
}

impl WorkspaceProbe {
    pub fn load(root_dir: &Path, indexed_paths: &[String]) -> Self {
        let Ok(root_dir) = std::fs::canonicalize(root_dir) else {
            orbit_log_fields::warn!(
                expanded(LOCAL_PATH) = %root_dir.display(),
                "js: failed to canonicalize root_dir, disabling workspace probe"
            );
            return Self {
                root_dir: root_dir.to_path_buf(),
                manifest_raw: None,
                tsconfig_path: None,
                jsconfig_path: None,
                webpack_configs: Vec::new(),
                bun_signal_present: false,
            };
        };

        let manifest_raw = read_bounded(&root_dir.join("package.json"));

        let tsconfig_path = existing_file(&root_dir, "tsconfig.json");
        let jsconfig_path = existing_file(&root_dir, "jsconfig.json");

        let webpack_configs = indexed_paths
            .iter()
            .filter(|path| is_webpack_config_path(path))
            .map(|relative| root_dir.join(relative))
            .collect();

        let bun_signal_present = BUN_SIGNAL_FILES.iter().any(|name| {
            indexed_paths.iter().any(|p| p == name) || is_regular_file(&root_dir.join(name))
        });

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

fn existing_file(root_dir: &Path, filename: &str) -> Option<PathBuf> {
    let path = root_dir.join(filename);
    path.is_file().then_some(path)
}

fn is_regular_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|meta| meta.file_type().is_file())
        .unwrap_or(false)
}

fn read_bounded(path: &Path) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    if meta.len() > super::extract::MAX_FILE_BYTES {
        return None;
    }
    std::fs::read_to_string(path).ok()
}
