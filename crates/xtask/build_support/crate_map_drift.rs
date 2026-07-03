// Pure parsing/diffing logic for the crate-map drift check, shared between
// `build.rs` (which reads the files and `panic!`s on drift) and
// `tests/crate_map_drift.rs` (which exercises the parsers). Both `include!`
// this file, so it deliberately has no `main`, no filesystem, and no env
// access - only string in, sets/report out.

use std::collections::BTreeSet;

use serde::Deserialize;

/// Members are listed as `crates/<...>` paths; the crate map's first column
/// drops this prefix (it writes `gkg-server`, `query-engine/compiler`), so
/// both sides normalize to the path below this prefix before comparing.
const MEMBER_PREFIX: &str = "crates/";

#[derive(Deserialize)]
struct Manifest {
    workspace: Workspace,
}

#[derive(Deserialize)]
struct Workspace {
    members: Vec<String>,
}

/// Normalize a member path to the identifier the crate map uses in its first
/// column: the path below `crates/`. Keeping the full relative path (not just
/// the final segment) means two members that share a leaf name but live under
/// different parents (`a/shared` vs `b/shared`) stay distinct, so one can't
/// silently mask the other's missing row.
fn crate_key(member_path: &str) -> String {
    member_path
        .strip_prefix(MEMBER_PREFIX)
        .unwrap_or(member_path)
        .to_string()
}

/// Extract crate keys from the `[workspace].members` array via a real TOML
/// parse, so a `default-members` key (or comments, or multi-line arrays)
/// can't be mis-read. Glob members (`crates/*`) are not expanded and would
/// surface as a literal `*` key; the workspace uses explicit paths, so this
/// is not a concern today.
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

/// Extract crate keys from the first column of the crate-map Markdown table.
/// Rows look like `| `crate-key` | Role |`; the key is the backtick-wrapped
/// token in the first cell (already written relative to `crates/`). The
/// header (`| Crate | Role |`) and separator (`|---|---|`) rows have no
/// backticked first cell and are skipped.
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

/// Compare the two sources and return a human-readable drift report, or
/// `None` when the crate map is in sync. The report lists both directions:
/// workspace members with no row, and rows with no member.
fn drift_report(manifest_src: &str, crate_map_src: &str) -> Option<String> {
    let members = workspace_member_names(manifest_src);
    let documented = crate_map_row_names(crate_map_src);

    let missing: Vec<&String> = members.difference(&documented).collect();
    let extra: Vec<&String> = documented.difference(&members).collect();

    if missing.is_empty() && extra.is_empty() {
        return None;
    }

    let mut report = String::from(
        "crate-map drift: docs/dev/agents-crate-map.md is out of sync with \
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
    report.push_str(
        "\nUpdate docs/dev/agents-crate-map.md so every [workspace] member has \
         exactly one row (see AGENTS.md doc-sync rules).",
    );
    Some(report)
}
