//! Tests the drift parsers `include!`d from the same file `build.rs` uses.

include!("../build_support/crate_map_drift.rs");

#[test]
fn nested_members_sharing_a_leaf_stay_distinct() {
    let manifest = r#"
[workspace]
members = ["crates/foo/shared", "crates/bar/shared"]
"#;
    let members = workspace_member_names(manifest);
    assert!(members.contains("foo/shared"));
    assert!(members.contains("bar/shared"));
    assert_eq!(members.len(), 2);
}

#[test]
fn default_members_before_members_is_not_mis_read() {
    let manifest = r#"
[workspace]
default-members = ["crates/only-default"]
members = ["crates/real-member"]
"#;
    let members = workspace_member_names(manifest);
    assert_eq!(members, BTreeSet::from(["real-member".to_string()]));
}

#[test]
fn crate_key_strips_only_the_crates_prefix() {
    assert_eq!(crate_key("crates/gkg-server"), "gkg-server");
    assert_eq!(
        crate_key("crates/query-engine/compiler"),
        "query-engine/compiler"
    );
}

#[test]
fn map_rows_keep_the_nested_path_and_skip_header() {
    let md = "\
| Crate | Role |
|---|---|
| `gkg-server` | server |
| `query-engine/compiler` | compiler |
";
    let rows = crate_map_row_names(md);
    assert_eq!(
        rows,
        BTreeSet::from([
            "gkg-server".to_string(),
            "query-engine/compiler".to_string()
        ])
    );
}

#[test]
fn in_sync_sources_report_no_drift() {
    let manifest = r#"
[workspace]
members = ["crates/a", "crates/b/c"]
"#;
    let md = "\
| Crate | Role |
|---|---|
| `a` | role a |
| `b/c` | role c |
";
    assert!(drift_report(manifest, md).is_none());
}

#[test]
fn missing_and_extra_rows_are_both_reported() {
    let manifest = r#"
[workspace]
members = ["crates/present", "crates/undocumented"]
"#;
    let md = "\
| Crate | Role |
|---|---|
| `present` | ok |
| `stale` | no longer exists |
";
    let report = drift_report(manifest, md).expect("drift expected");
    assert!(report.contains("undocumented"));
    assert!(report.contains("stale"));
}
