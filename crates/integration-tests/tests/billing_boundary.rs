//! SOX boundary check: only permitted crates may depend on `gkg-billing`.
//!
//! A passing run in CI is control evidence that the billing crate's dependency
//! surface has not silently expanded. See ADR 013:
//! docs/design-documents/decisions/013_billing_sox_scope.md

use std::path::PathBuf;

use cargo_metadata::MetadataCommand;

const PERMITTED_DEPENDENTS: &[&str] = &["gkg-server"];

#[test]
fn only_permitted_crates_depend_on_gkg_billing() {
    let workspace_root = workspace_root();
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .no_deps()
        .exec()
        .expect("cargo metadata");

    let violations: Vec<String> = metadata
        .workspace_packages()
        .into_iter()
        .filter(|pkg| pkg.name != "gkg-billing")
        .filter(|pkg| pkg.dependencies.iter().any(|dep| dep.name == "gkg-billing"))
        .filter(|pkg| !PERMITTED_DEPENDENTS.contains(&pkg.name.as_str()))
        .map(|pkg| pkg.name.to_string())
        .collect();

    assert!(
        violations.is_empty(),
        "SOX boundary violation: {violations:?} depend on `gkg-billing` \
         but are not in the permitted list {PERMITTED_DEPENDENTS:?}.\n\
         Add to PERMITTED_DEPENDENTS only after SOX review — \
         see docs/design-documents/decisions/013_billing_sox_scope.md"
    );
}

fn workspace_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels up from crates/integration-tests")
        .to_path_buf();
    assert!(
        root.join("Cargo.toml").exists(),
        "workspace Cargo.toml not found at {root:?} — crate may have moved"
    );
    root
}
