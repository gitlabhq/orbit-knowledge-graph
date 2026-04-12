use super::helpers::{collect_discovered_paths, fixture_root};
use crate::analysis::languages::js::{detect_workspaces, is_bun_project};

#[test]
fn pnpm_workspace() {
    let root = fixture_root("workspace-cases/pnpm");
    let paths = collect_discovered_paths(&root);
    let packages = detect_workspaces(&root, &paths);

    assert_eq!(packages.len(), 2);
    let core = packages
        .iter()
        .find(|p| p.name == "@myapp/core")
        .expect("should find core");
    assert_eq!(core.version.as_deref(), Some("1.0.0"));
    assert_eq!(core.path, "packages/core");
}

#[test]
fn package_json_array_workspaces() {
    let root = fixture_root("workspace-cases/package-array");
    let paths = collect_discovered_paths(&root);
    assert_eq!(detect_workspaces(&root, &paths).len(), 2);
}

#[test]
fn package_json_object_workspaces() {
    let root = fixture_root("workspace-cases/package-object");
    let paths = collect_discovered_paths(&root);
    assert_eq!(detect_workspaces(&root, &paths).len(), 2);
}

#[test]
fn pnpm_workspace_takes_priority_over_package_json() {
    let root = fixture_root("workspace-cases/pnpm-priority");
    let paths = collect_discovered_paths(&root);
    let packages = detect_workspaces(&root, &paths);
    assert_eq!(packages.len(), 1);
    assert_eq!(packages[0].name, "@myapp/web");
}

#[test]
fn no_workspace_config() {
    let root = fixture_root("workspace-cases/none");
    let paths = collect_discovered_paths(&root);
    assert!(detect_workspaces(&root, &paths).is_empty());
}

#[test]
fn bun_lock_detected() {
    let root = fixture_root("workspace-cases/bun-lock");
    let paths = collect_discovered_paths(&root);
    assert!(is_bun_project(&root, &paths));
}

#[test]
fn bunfig_detected() {
    let root = fixture_root("workspace-cases/bunfig");
    let paths = collect_discovered_paths(&root);
    assert!(is_bun_project(&root, &paths));
}

#[test]
fn types_bun_detected() {
    let root = fixture_root("workspace-cases/types-bun");
    let paths = collect_discovered_paths(&root);
    assert!(is_bun_project(&root, &paths));
}
