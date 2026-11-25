use std::collections::BTreeSet;

use serde::Deserialize;

const MEMBER_PREFIX: &str = "crates/";

#[derive(Deserialize)]
struct Manifest {
    workspace: Workspace,
}

#[derive(Deserialize)]
struct Workspace {
    members: Vec<String>,
}

fn crate_key(member_path: &str) -> String {
    member_path
        .strip_prefix(MEMBER_PREFIX)
        .unwrap_or(member_path)
        .to_string()
}

fn workspace_member_names(manifest_src: &str) -> BTreeSet<String> {
    let manifest: Manifest = toml::from_str(manifest_src)
        .expect("Cargo.toml has a parseable [workspace].members array");
    manifest
        .workspace
        .members
        .iter()
        .map(|m| crate_key(m))
        .collect()
}

fn crate_map_row_names(crate_map_src: &str) -> BTreeSet<String> {
    crate_map_src
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let first_cell = line.strip_prefix('|')?.split('|').next()?.trim();
            let name = first_cell.strip_prefix('`')?.strip_suffix('`')?.trim();
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect()
}

fn drift_report(manifest_src: &str, crate_map_src: &str) -> Option<String> {
    let members = workspace_member_names(manifest_src);
    let documented = crate_map_row_names(crate_map_src);

    let missing: Vec<&String> = members.difference(&documented).collect();
    let extra: Vec<&String> = documented.difference(&members).collect();

    if missing.is_empty() && extra.is_empty() {
        return None;
    }

    let mut report = String::from(
        "crate-map drift: docs/pages/engineering/development/agents-crate-map.md is out of sync with \
         the [workspace].members in Cargo.toml.\n",
    );
    if !missing.is_empty() {
        report.push_str("\n  Workspace members missing a crate-map row (add one):\n");
        for name in &missing {
            report.push_str(&format!("    - {name}\n"));
        }
    }
    if !extra.is_empty() {
        report
            .push_str("\n  Crate-map rows with no matching workspace member (remove or rename):\n");
        for name in &extra {
            report.push_str(&format!("    - {name}\n"));
        }
    }
    Some(report)
}
