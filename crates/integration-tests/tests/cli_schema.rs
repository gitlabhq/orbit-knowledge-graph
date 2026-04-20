//! CLI integration tests for `orbit schema` and `orbit debug ddl`.
//!
//! Exercises the schema introspection subcommand (local vs all scope,
//! expanded vs condensed, TOON vs raw JSON) and the renamed DDL subcommand.

use integration_testkit::cli::orbit_cmd;
use serde_json::Value;

fn run(args: &[&str]) -> (String, String, bool) {
    let out = orbit_cmd().args(args).output().expect("spawn orbit");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[test]
fn schema_default_is_local_scope() {
    let (stdout, stderr, ok) = run(&["schema"]);
    assert!(ok, "orbit schema failed: {stderr}");

    for want in ["Directory", "File", "Definition", "ImportedSymbol"] {
        assert!(
            stdout.contains(want),
            "expected local entity {want} in output: {stdout}"
        );
    }
    for forbidden in ["User", "Project", "MergeRequest", "WorkItem", "AUTHORED"] {
        assert!(
            !stdout.contains(forbidden),
            "server-only {forbidden} leaked into local scope: {stdout}"
        );
    }
    for want in ["CONTAINS", "DEFINES", "IMPORTS"] {
        assert!(stdout.contains(want), "missing edge {want}: {stdout}");
    }
}

#[test]
fn schema_default_is_toon_not_json() {
    let (stdout, _, ok) = run(&["schema"]);
    assert!(ok);
    assert!(
        !stdout.trim_start().starts_with('{'),
        "default should be TOON, got JSON: {stdout}"
    );
    assert!(stdout.contains("domains"));
    assert!(stdout.contains("edges"));
}

#[test]
fn schema_expand_file_shows_props() {
    let (stdout, stderr, ok) = run(&["schema", "--expand", "File"]);
    assert!(ok, "stderr: {stderr}");
    assert!(
        stdout.contains("path:string"),
        "missing path:string: {stdout}"
    );
    assert!(stdout.contains("props"), "missing props key: {stdout}");
}

#[test]
fn schema_raw_is_parseable_json() {
    let (stdout, _, ok) = run(&["schema", "--raw"]);
    assert!(ok);
    let v: Value = serde_json::from_str(&stdout).expect("parseable JSON");
    assert!(v["domains"].is_array());
    assert!(v["edges"].is_array());
    let edges: Vec<&str> = v["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    for want in ["CONTAINS", "DEFINES", "IMPORTS"] {
        assert!(edges.contains(&want), "missing {want} edge in {edges:?}");
    }
}

#[test]
fn schema_all_includes_server_entities() {
    let (stdout, _, ok) = run(&["schema", "--all"]);
    assert!(ok);
    assert!(stdout.contains("User"), "--all should include User");
    assert!(stdout.contains("AUTHORED"), "--all should include AUTHORED");
}

#[test]
fn debug_ddl_produces_clickhouse_statements() {
    let (stdout, stderr, ok) = run(&["debug", "ddl"]);
    assert!(ok, "debug ddl failed: {stderr}");
    assert!(
        stdout.contains("CREATE TABLE"),
        "expected DDL output, got: {}",
        &stdout.chars().take(200).collect::<String>()
    );
}

#[test]
fn old_schema_subcommand_no_longer_emits_ddl() {
    let (stdout, _, ok) = run(&["schema"]);
    assert!(ok);
    assert!(
        !stdout.contains("CREATE TABLE"),
        "orbit schema must not emit DDL anymore: {stdout}"
    );
}

#[test]
fn schema_expand_without_value_errors() {
    let (_, stderr, ok) = run(&["schema", "--expand"]);
    assert!(!ok, "--expand without a value should fail");
    assert!(
        stderr.contains("--expand") || stderr.contains("NODE"),
        "stderr should mention the missing NODE value: {stderr}"
    );
}
