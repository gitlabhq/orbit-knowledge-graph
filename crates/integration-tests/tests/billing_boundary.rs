//! SOX boundary check: only permitted crates may depend on `gkg-billing`.
//!
//! A passing run in CI is control evidence that the billing crate's dependency
//! surface has not silently expanded. See ADR 013:
//! docs/design-documents/decisions/013_billing_sox_scope.md

use std::{fs, path::PathBuf};

const PERMITTED_DEPENDENTS: &[&str] = &["gkg-server"];

#[test]
fn only_permitted_crates_depend_on_gkg_billing() {
    let workspace_root = workspace_root();
    let workspace_toml =
        fs::read_to_string(workspace_root.join("Cargo.toml")).expect("workspace Cargo.toml");

    let violations: Vec<String> = workspace_members(&workspace_toml)
        .into_iter()
        .filter_map(|member| {
            let content =
                fs::read_to_string(workspace_root.join(&member).join("Cargo.toml")).ok()?;
            let name = crate_name(&content)?;
            if name == "gkg-billing" {
                return None;
            }
            let depends = content.lines().any(|l| l.trim().starts_with("gkg-billing"));
            (depends && !PERMITTED_DEPENDENTS.contains(&name.as_str())).then_some(name)
        })
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels up from crates/integration-tests")
        .to_path_buf()
}

fn workspace_members(workspace_toml: &str) -> Vec<String> {
    workspace_toml
        .split("members = [")
        .nth(1)
        .expect("workspace members array")
        .split(']')
        .next()
        .expect("closing bracket")
        .lines()
        .filter_map(|line| {
            let s = line.trim().trim_end_matches(',').trim_matches('"');
            s.starts_with("crates/").then_some(s.to_string())
        })
        .collect()
}

fn crate_name(cargo_toml: &str) -> Option<String> {
    cargo_toml.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with("name") && line.contains('=') {
            let val = line.split('=').nth(1)?.trim().trim_matches('"');
            (!val.is_empty()).then_some(val.to_string())
        } else {
            None
        }
    })
}
