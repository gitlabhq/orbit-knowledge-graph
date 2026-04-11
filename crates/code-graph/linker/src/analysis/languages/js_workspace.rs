use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct WorkspacePackage {
    pub name: String,
    pub version: Option<String>,
    pub path: String,
}

pub fn detect_workspaces(root_dir: &Path) -> Vec<WorkspacePackage> {
    let globs = read_workspace_globs(root_dir);
    if globs.is_empty() {
        return Vec::new();
    }

    let mut packages = Vec::new();
    for glob in &globs {
        for dir in expand_workspace_glob(root_dir, glob) {
            let pkg_json_path = dir.join("package.json");
            if let Some(pkg) = read_package_meta(&pkg_json_path, root_dir) {
                packages.push(pkg);
            }
        }
    }

    packages.sort_by(|a, b| a.path.cmp(&b.path));
    packages
}

pub fn is_bun_project(root_dir: &Path) -> bool {
    if root_dir.join("bun.lock").is_file() || root_dir.join("bun.lockb").is_file() {
        return true;
    }
    if root_dir.join("bunfig.toml").is_file() {
        return true;
    }
    has_bun_types_dep(root_dir)
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

fn expand_workspace_glob(root_dir: &Path, pattern: &str) -> Vec<std::path::PathBuf> {
    let pattern = pattern.strip_suffix('/').unwrap_or(pattern);
    let mut results = Vec::new();

    if let Some(prefix) = pattern.strip_suffix("/*") {
        let search_dir = root_dir.join(prefix);
        if let Ok(entries) = std::fs::read_dir(&search_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    results.push(path);
                }
            }
        }
    } else if let Some(prefix) = pattern.strip_suffix("/**") {
        collect_dirs_recursive(root_dir.join(prefix).as_path(), &mut results);
    } else if !pattern.contains('*') {
        let path = root_dir.join(pattern);
        if path.is_dir() {
            results.push(path);
        }
    } else {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let prefix_part = parts[0];
            let suffix_part = parts[1];
            let search_dir = root_dir.join(prefix_part.trim_end_matches('/'));
            if let Ok(entries) = std::fs::read_dir(&search_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let name = entry.file_name();
                        let name_str = name.to_string_lossy();
                        if suffix_part.is_empty() || name_str.ends_with(suffix_part) {
                            results.push(path);
                        }
                    }
                }
            }
        }
    }

    results
}

fn collect_dirs_recursive(dir: &Path, results: &mut Vec<std::path::PathBuf>) {
    if !dir.is_dir() {
        return;
    }
    results.push(dir.to_path_buf());
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_dirs_recursive(&path, results);
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_workspace_dir() -> TempDir {
        let dir = TempDir::new().unwrap();

        fs::create_dir_all(dir.path().join("packages/core")).unwrap();
        fs::write(
            dir.path().join("packages/core/package.json"),
            r#"{"name": "@myapp/core", "version": "1.0.0"}"#,
        )
        .unwrap();

        fs::create_dir_all(dir.path().join("packages/utils")).unwrap();
        fs::write(
            dir.path().join("packages/utils/package.json"),
            r#"{"name": "@myapp/utils", "version": "0.5.0"}"#,
        )
        .unwrap();

        dir
    }

    #[test]
    fn test_pnpm_workspace_yaml() {
        let dir = setup_workspace_dir();
        fs::write(
            dir.path().join("pnpm-workspace.yaml"),
            "packages:\n  - packages/*\n",
        )
        .unwrap();

        let packages = detect_workspaces(dir.path());
        assert_eq!(packages.len(), 2);

        let core = packages.iter().find(|p| p.name == "@myapp/core").unwrap();
        assert_eq!(core.version.as_deref(), Some("1.0.0"));
        assert_eq!(core.path, "packages/core");
    }

    #[test]
    fn test_package_json_workspaces_array() {
        let dir = setup_workspace_dir();
        fs::write(
            dir.path().join("package.json"),
            r#"{"name": "root", "workspaces": ["packages/*"]}"#,
        )
        .unwrap();

        let packages = detect_workspaces(dir.path());
        assert_eq!(packages.len(), 2);
    }

    #[test]
    fn test_package_json_workspaces_object() {
        let dir = setup_workspace_dir();
        fs::write(
            dir.path().join("package.json"),
            r#"{"name": "root", "workspaces": {"packages": ["packages/*"]}}"#,
        )
        .unwrap();

        let packages = detect_workspaces(dir.path());
        assert_eq!(packages.len(), 2);
    }

    #[test]
    fn test_pnpm_workspace_takes_priority() {
        let dir = setup_workspace_dir();

        fs::create_dir_all(dir.path().join("apps/web")).unwrap();
        fs::write(
            dir.path().join("apps/web/package.json"),
            r#"{"name": "@myapp/web"}"#,
        )
        .unwrap();

        fs::write(
            dir.path().join("pnpm-workspace.yaml"),
            "packages:\n  - apps/*\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("package.json"),
            r#"{"name": "root", "workspaces": ["packages/*"]}"#,
        )
        .unwrap();

        let packages = detect_workspaces(dir.path());
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "@myapp/web");
    }

    #[test]
    fn test_is_bun_project_bun_lock() {
        let dir = TempDir::new().unwrap();
        assert!(!is_bun_project(dir.path()));

        fs::write(dir.path().join("bun.lock"), "{}").unwrap();
        assert!(is_bun_project(dir.path()));
    }

    #[test]
    fn test_is_bun_project_bunfig() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("bunfig.toml"), "[install]\n").unwrap();
        assert!(is_bun_project(dir.path()));
    }

    #[test]
    fn test_is_bun_project_types_bun() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("package.json"),
            r#"{"devDependencies": {"@types/bun": "^1.0.0"}}"#,
        )
        .unwrap();
        assert!(is_bun_project(dir.path()));
    }

    #[test]
    fn test_no_workspaces() {
        let dir = TempDir::new().unwrap();
        let packages = detect_workspaces(dir.path());
        assert!(packages.is_empty());
    }
}
