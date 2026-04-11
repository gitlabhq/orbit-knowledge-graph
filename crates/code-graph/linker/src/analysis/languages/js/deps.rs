use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

use super::workspace::WorkspacePackage;

#[derive(Debug, Clone)]
pub struct DependencyInfo {
    pub package_name: String,
    pub version_range: String,
    pub workspace_package: Option<String>,
    pub is_dev_dependency: bool,
    pub is_peer_dependency: bool,
    pub is_optional_dependency: bool,
}

#[derive(Deserialize)]
struct PackageJsonDeps {
    #[serde(default)]
    dependencies: HashMap<String, String>,
    #[serde(default, rename = "devDependencies")]
    dev_dependencies: HashMap<String, String>,
    #[serde(default, rename = "peerDependencies")]
    peer_dependencies: HashMap<String, String>,
    #[serde(default, rename = "optionalDependencies")]
    optional_dependencies: HashMap<String, String>,
}

/// Parse dependencies from the root package.json and workspace package.json files.
///
/// Workspace packages are provided by the caller (from `detect_workspaces`)
/// rather than re-scanning the filesystem.
pub fn parse_all_dependencies(
    root_dir: &Path,
    workspace_packages: &[WorkspacePackage],
) -> Result<Vec<DependencyInfo>, String> {
    let mut all_deps = Vec::new();

    let root_pkg = root_dir.join("package.json");
    if root_pkg.is_file() {
        let content = std::fs::read_to_string(&root_pkg)
            .map_err(|e| format!("Cannot read root package.json: {e}"))?;
        all_deps.extend(parse_package_json(&content, None)?);
    }

    for pkg in workspace_packages {
        let pkg_json = root_dir.join(&pkg.path).join("package.json");
        if pkg_json.is_file()
            && let Ok(content) = std::fs::read_to_string(&pkg_json)
            && let Ok(deps) = parse_package_json(&content, Some(pkg.name.clone()))
        {
            all_deps.extend(deps);
        }
    }

    all_deps.sort_by(|a, b| {
        a.workspace_package
            .cmp(&b.workspace_package)
            .then(a.package_name.cmp(&b.package_name))
    });
    Ok(all_deps)
}

pub fn parse_package_json(
    content: &str,
    workspace_package: Option<String>,
) -> Result<Vec<DependencyInfo>, String> {
    let pkg: PackageJsonDeps =
        serde_json::from_str(content).map_err(|e| format!("Invalid package.json: {e}"))?;

    let mut deps = Vec::new();

    for (name, version) in &pkg.dependencies {
        deps.push(DependencyInfo {
            package_name: name.clone(),
            version_range: version.clone(),
            workspace_package: workspace_package.clone(),
            is_dev_dependency: false,
            is_peer_dependency: false,
            is_optional_dependency: false,
        });
    }

    for (name, version) in &pkg.dev_dependencies {
        deps.push(DependencyInfo {
            package_name: name.clone(),
            version_range: version.clone(),
            workspace_package: workspace_package.clone(),
            is_dev_dependency: true,
            is_peer_dependency: false,
            is_optional_dependency: false,
        });
    }

    for (name, version) in &pkg.peer_dependencies {
        if !deps.iter().any(|d| d.package_name == *name) {
            deps.push(DependencyInfo {
                package_name: name.clone(),
                version_range: version.clone(),
                workspace_package: workspace_package.clone(),
                is_dev_dependency: false,
                is_peer_dependency: true,
                is_optional_dependency: false,
            });
        }
    }

    for (name, version) in &pkg.optional_dependencies {
        if !deps.iter().any(|d| d.package_name == *name) {
            deps.push(DependencyInfo {
                package_name: name.clone(),
                version_range: version.clone(),
                workspace_package: workspace_package.clone(),
                is_dev_dependency: false,
                is_peer_dependency: false,
                is_optional_dependency: true,
            });
        }
    }

    Ok(deps)
}

#[cfg(test)]
mod tests {
    use super::super::workspace;
    use super::*;

    #[test]
    fn test_parse_basic_deps() {
        let content = r#"{
            "dependencies": { "react": "^18.2.0", "express": "^4.18.0" },
            "devDependencies": { "typescript": "^5.0.0" }
        }"#;
        let deps = parse_package_json(content, None).unwrap();
        assert_eq!(deps.len(), 3);

        let react = deps.iter().find(|d| d.package_name == "react").unwrap();
        assert_eq!(react.version_range, "^18.2.0");
        assert!(!react.is_dev_dependency);
        assert!(react.workspace_package.is_none());

        let ts = deps
            .iter()
            .find(|d| d.package_name == "typescript")
            .unwrap();
        assert!(ts.is_dev_dependency);
    }

    #[test]
    fn test_parse_peer_and_optional() {
        let content = r#"{
            "dependencies": { "react": "^18.0.0" },
            "peerDependencies": { "react-dom": "^18.0.0" },
            "optionalDependencies": { "fsevents": "^2.3.0" }
        }"#;
        let deps = parse_package_json(content, None).unwrap();
        assert_eq!(deps.len(), 3);

        let rd = deps.iter().find(|d| d.package_name == "react-dom").unwrap();
        assert!(rd.is_peer_dependency);

        let fs = deps.iter().find(|d| d.package_name == "fsevents").unwrap();
        assert!(fs.is_optional_dependency);
    }

    #[test]
    fn test_parse_empty() {
        let deps = parse_package_json("{}", None).unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn test_parse_scoped_packages() {
        let content = r#"{
            "dependencies": { "@types/node": "^20.0.0", "@vue/compiler-sfc": "^3.3.0" }
        }"#;
        let deps = parse_package_json(content, None).unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.iter().any(|d| d.package_name == "@types/node"));
    }

    #[test]
    fn test_workspace_protocol() {
        let content = r#"{
            "dependencies": { "@org/shared": "workspace:*", "react": "^18.0.0" }
        }"#;
        let deps = parse_package_json(content, None).unwrap();
        let shared = deps
            .iter()
            .find(|d| d.package_name == "@org/shared")
            .unwrap();
        assert_eq!(shared.version_range, "workspace:*");
    }

    #[test]
    fn test_workspace_package_tag() {
        let content = r#"{
            "name": "@org/web",
            "dependencies": { "react": "^18.0.0" }
        }"#;
        let deps = parse_package_json(content, Some("@org/web".to_string())).unwrap();
        assert_eq!(deps[0].workspace_package.as_deref(), Some("@org/web"));
    }

    #[test]
    fn test_parse_all_with_workspaces() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*"], "devDependencies": { "turbo": "^2.0.0" } }"#,
        )
        .unwrap();

        let pkg_dir = dir.path().join("packages").join("ui");
        std::fs::create_dir_all(&pkg_dir).unwrap();
        std::fs::write(
            pkg_dir.join("package.json"),
            r#"{ "name": "@org/ui", "dependencies": { "react": "^18.0.0" } }"#,
        )
        .unwrap();

        let discovered_paths = vec![
            "packages/ui/src/index.ts".to_string(),
            "packages/ui/package.json".to_string(),
        ];
        let workspace_packages = workspace::detect_workspaces(dir.path(), &discovered_paths);
        let deps = parse_all_dependencies(dir.path(), &workspace_packages).unwrap();

        assert!(deps.iter().any(|d| d.package_name == "turbo"), "root dep");
        assert!(
            deps.iter()
                .any(|d| d.package_name == "react"
                    && d.workspace_package.as_deref() == Some("@org/ui")),
            "workspace dep with package tag"
        );
    }
}
