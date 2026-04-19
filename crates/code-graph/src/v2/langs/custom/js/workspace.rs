use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct WorkspacePackage {
    pub name: String,
    pub version: Option<String>,
    pub path: String,
}

/// Detect workspace packages by matching workspace glob patterns against
/// already-discovered file paths, eliminating recursive directory scanning.
///
/// `discovered_paths` should contain relative file paths from the indexer's
/// walk (e.g. `"packages/core/src/index.ts"`).
pub fn detect_workspaces(root_dir: &Path, discovered_paths: &[String]) -> Vec<WorkspacePackage> {
    let globs = read_workspace_globs(root_dir);
    if globs.is_empty() {
        return Vec::new();
    }

    let mut packages = Vec::new();
    for glob in &globs {
        for dir in expand_workspace_glob_from_paths(glob, discovered_paths, root_dir) {
            let pkg_json_path = dir.join("package.json");
            if let Some(pkg) = read_package_meta(&pkg_json_path, root_dir) {
                packages.push(pkg);
            }
        }
    }

    packages.sort_by(|a, b| a.path.cmp(&b.path));
    packages
}

/// Check if this is a Bun project by inspecting discovered file paths
/// and the root package.json content.
pub fn is_bun_project(root_dir: &Path, discovered_paths: &[String]) -> bool {
    let has_bun_signal = discovered_paths
        .iter()
        .any(|p| matches!(p.as_str(), "bun.lock" | "bun.lockb" | "bunfig.toml"));
    has_bun_signal || has_bun_types_dep(root_dir)
}

fn has_bun_types_dep(root_dir: &Path) -> bool {
    let pkg_path = root_dir.join("package.json");
    let content = match std::fs::read_to_string(&pkg_path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    content.contains("\"@types/bun\"")
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

fn read_workspace_globs(root_dir: &Path) -> Vec<String> {
    let pnpm_ws = root_dir.join("pnpm-workspace.yaml");
    if pnpm_ws.is_file()
        && let Ok(content) = std::fs::read_to_string(&pnpm_ws)
        && let Ok(config) = serde_yaml::from_str::<PnpmWorkspaceConfig>(&content)
        && !config.packages.is_empty()
    {
        return config.packages;
    }

    let pkg_json = root_dir.join("package.json");
    if pkg_json.is_file()
        && let Ok(content) = std::fs::read_to_string(&pkg_json)
        && let Ok(parsed) = serde_json::from_str::<PackageJsonWorkspaces>(&content)
    {
        return match parsed.workspaces {
            WorkspacesField::Array(v) => v,
            WorkspacesField::Object { packages } => packages,
            WorkspacesField::None => Vec::new(),
        };
    }

    Vec::new()
}

/// Match workspace glob patterns against discovered file paths to find
/// workspace directories, instead of scanning the filesystem with read_dir.
fn expand_workspace_glob_from_paths(
    pattern: &str,
    discovered_paths: &[String],
    root_dir: &Path,
) -> Vec<std::path::PathBuf> {
    let pattern = pattern.strip_suffix('/').unwrap_or(pattern);
    let mut dirs = HashSet::new();

    if let Some(prefix) = pattern.strip_suffix("/**") {
        // "packages/**" -> find all dirs under packages/ at any depth that have a package.json
        let prefix_with_slash = format!("{prefix}/");
        for path in discovered_paths {
            if let Some(rest) = path.strip_prefix(&prefix_with_slash)
                && let Some(slash_pos) = rest.find('/')
            {
                dirs.insert(root_dir.join(prefix).join(&rest[..slash_pos]));
            }
        }
    } else if let Some(prefix) = pattern.strip_suffix("/*") {
        // "packages/*" -> find all dirs under packages/ at depth 1
        let prefix_with_slash = format!("{prefix}/");
        for path in discovered_paths {
            if let Some(rest) = path.strip_prefix(&prefix_with_slash)
                && let Some(slash_pos) = rest.find('/')
            {
                dirs.insert(root_dir.join(prefix).join(&rest[..slash_pos]));
            }
        }
    } else if !pattern.contains('*') {
        // Exact directory name
        let prefix_with_slash = format!("{pattern}/");
        if discovered_paths
            .iter()
            .any(|p| p.starts_with(&prefix_with_slash))
        {
            dirs.insert(root_dir.join(pattern));
        }
    } else {
        // Generic "prefix*suffix" pattern (e.g. "packages/core-*")
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let prefix_part = parts[0];
            let suffix_part = parts[1];
            let search_prefix = prefix_part.trim_end_matches('/');
            let search_with_slash = format!("{search_prefix}/");
            for path in discovered_paths {
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
