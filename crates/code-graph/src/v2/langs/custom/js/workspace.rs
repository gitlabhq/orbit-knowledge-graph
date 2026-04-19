//! One-shot filesystem probe for a JS workspace.
//!
//! `WorkspaceProbe::load` reads every manifest/config file the pipeline
//! cares about *exactly once* at the start of `JsPipeline::process_files`
//! and hands the parsed results to every downstream consumer:
//! `JsCrossFileResolver`, `discover_tsconfig`, the webpack evaluator, and
//! `is_bun` detection.
//!
//! Before this existed, the pipeline re-read `package.json` twice, probed
//! seven manifest filenames in one place and three more in another, and
//! walked eight webpack-config candidates from inside the evaluator. All
//! of that collapses into this struct.

use oxc_resolver::{TsconfigDiscovery, TsconfigOptions, TsconfigReferences};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::constants::{BUN_SIGNAL_FILES, MANIFEST_FILENAMES, WEBPACK_CONFIG_CANDIDATES};

#[derive(Debug, Clone)]
pub struct WorkspacePackage {
    pub name: String,
    pub version: Option<String>,
    pub path: String,
}

/// Every manifest/config fact the JS pipeline derives from the
/// repository root, computed once.
pub struct WorkspaceProbe {
    root_dir: PathBuf,
    manifest: Option<ManifestSource>,
    pnpm_workspaces: Vec<String>,
    tsconfig_path: Option<PathBuf>,
    jsconfig_path: Option<PathBuf>,
    webpack_configs: Vec<PathBuf>,
    bun_signal_present: bool,
    manifest_paths_present: Vec<String>,
}

struct ManifestSource {
    raw: String,
    workspaces: Vec<String>,
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

        let manifest = read_manifest(&root_dir.join("package.json"));
        let pnpm_workspaces = read_pnpm_workspaces(&root_dir.join("pnpm-workspace.yaml"));

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

        let manifest_paths_present = MANIFEST_FILENAMES
            .iter()
            .filter(|name| root_dir.join(name).is_file())
            .map(|name| (*name).to_string())
            .collect();

        Self {
            root_dir,
            manifest,
            pnpm_workspaces,
            tsconfig_path,
            jsconfig_path,
            webpack_configs,
            bun_signal_present,
            manifest_paths_present,
        }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn is_bun(&self) -> bool {
        self.bun_signal_present
            || self
                .manifest
                .as_ref()
                .is_some_and(|m| m.raw.contains("\"@types/bun\""))
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

    /// Manifest filenames from `MANIFEST_FILENAMES` that exist at the
    /// repo root. Matches the previous `discovered_paths` augmentation
    /// without re-stating the hard-coded list at the call site.
    pub fn manifest_paths_present(&self) -> &[String] {
        &self.manifest_paths_present
    }

    pub fn workspaces(&self, indexed_paths: &[String]) -> Vec<WorkspacePackage> {
        let globs = self.workspace_globs();
        if globs.is_empty() {
            return Vec::new();
        }

        let mut packages = Vec::new();
        for glob in &globs {
            for dir in expand_workspace_glob(glob, indexed_paths, &self.root_dir) {
                if let Some(pkg) = read_package_meta(&dir.join("package.json"), &self.root_dir) {
                    packages.push(pkg);
                }
            }
        }

        packages.sort_by(|a, b| a.path.cmp(&b.path));
        packages
    }

    fn workspace_globs(&self) -> Vec<String> {
        if !self.pnpm_workspaces.is_empty() {
            return self.pnpm_workspaces.clone();
        }
        self.manifest
            .as_ref()
            .map(|m| m.workspaces.clone())
            .unwrap_or_default()
    }
}

fn existing_file(root_dir: &Path, filename: &str) -> Option<PathBuf> {
    let path = root_dir.join(filename);
    path.is_file().then_some(path)
}

fn read_manifest(path: &Path) -> Option<ManifestSource> {
    let raw = std::fs::read_to_string(path).ok()?;
    let workspaces = parse_manifest_workspaces(&raw);
    Some(ManifestSource { raw, workspaces })
}

fn read_pnpm_workspaces(path: &Path) -> Vec<String> {
    if !path.is_file() {
        return Vec::new();
    }
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    serde_yaml::from_str::<PnpmWorkspaceConfig>(&raw)
        .map(|config| config.packages)
        .unwrap_or_default()
}

fn parse_manifest_workspaces(raw: &str) -> Vec<String> {
    match serde_json::from_str::<PackageJsonWorkspaces>(raw) {
        Ok(PackageJsonWorkspaces {
            workspaces: WorkspacesField::Array(v),
        }) => v,
        Ok(PackageJsonWorkspaces {
            workspaces: WorkspacesField::Object { packages },
        }) => packages,
        _ => Vec::new(),
    }
}

#[derive(Deserialize)]
struct PnpmWorkspaceConfig {
    #[serde(default)]
    packages: Vec<String>,
}

#[derive(Deserialize)]
struct PackageJsonWorkspaces {
    #[serde(default)]
    workspaces: WorkspacesField,
}

#[derive(Deserialize, Default)]
#[serde(untagged)]
enum WorkspacesField {
    Array(Vec<String>),
    Object {
        #[serde(default)]
        packages: Vec<String>,
    },
    #[default]
    None,
}

#[derive(Deserialize)]
struct PackageJsonMeta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<String>,
}

fn read_package_meta(pkg_json_path: &Path, root_dir: &Path) -> Option<WorkspacePackage> {
    let content = std::fs::read_to_string(pkg_json_path).ok()?;
    let meta: PackageJsonMeta = serde_json::from_str(&content).ok()?;
    let name = meta.name?;
    let relative = pkg_json_path
        .parent()?
        .strip_prefix(root_dir)
        .ok()?
        .to_string_lossy()
        .to_string();
    Some(WorkspacePackage {
        name,
        version: meta.version,
        path: relative,
    })
}

/// Match workspace glob patterns against discovered file paths to find
/// workspace directories, instead of scanning the filesystem with
/// `read_dir`.
fn expand_workspace_glob(pattern: &str, indexed_paths: &[String], root_dir: &Path) -> Vec<PathBuf> {
    let pattern = pattern.strip_suffix('/').unwrap_or(pattern);
    let mut dirs: HashSet<PathBuf> = HashSet::new();

    if let Some(prefix) = pattern.strip_suffix("/**") {
        let prefix_with_slash = format!("{prefix}/");
        for path in indexed_paths {
            if let Some(rest) = path.strip_prefix(&prefix_with_slash)
                && let Some(slash_pos) = rest.find('/')
            {
                dirs.insert(root_dir.join(prefix).join(&rest[..slash_pos]));
            }
        }
    } else if let Some(prefix) = pattern.strip_suffix("/*") {
        let prefix_with_slash = format!("{prefix}/");
        for path in indexed_paths {
            if let Some(rest) = path.strip_prefix(&prefix_with_slash)
                && let Some(slash_pos) = rest.find('/')
            {
                dirs.insert(root_dir.join(prefix).join(&rest[..slash_pos]));
            }
        }
    } else if !pattern.contains('*') {
        let prefix_with_slash = format!("{pattern}/");
        if indexed_paths
            .iter()
            .any(|p| p.starts_with(&prefix_with_slash))
        {
            dirs.insert(root_dir.join(pattern));
        }
    } else {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let prefix_part = parts[0];
            let suffix_part = parts[1];
            let search_prefix = prefix_part.trim_end_matches('/');
            let search_with_slash = format!("{search_prefix}/");
            for path in indexed_paths {
                if let Some(rest) = path.strip_prefix(&search_with_slash)
                    && let Some(slash_pos) = rest.find('/')
                {
                    let dir_name = &rest[..slash_pos];
                    if suffix_part.is_empty() || dir_name.ends_with(suffix_part) {
                        dirs.insert(root_dir.join(search_prefix).join(dir_name));
                    }
                }
            }
        }
    }

    dirs.into_iter().collect()
}
