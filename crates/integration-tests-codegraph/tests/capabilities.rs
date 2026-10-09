use integration_tests_codegraph::run_incremental_suite;

const SUITE: &str = r#"
name: Fixture capability controls
fixtures:
  - path: main.py
    content: "def run(): pass"
tests:
  - name: enabled by default
    query: "MATCH (d:Definition) RETURN d.name AS name"
    assert:
      - { row: { name: run } }
  - name: unsupported query is not compiled
    supported: false
    query: "not a query"
  - name: supported tests can still be skipped
    supported: true
    skip: true
    query: "not a query"
steps:
  - name: restore capability controls
    snapshot: true
    tests:
      - name: unsupported step query is not compiled
        supported: false
        query: "not a query"
      - name: supported step query runs
        supported: true
        query: "MATCH (d:Definition) RETURN d.name AS name"
        assert:
          - { row: { name: run } }
"#;

#[test]
fn capability_controls_apply_to_initial_and_incremental_queries() {
    run_incremental_suite(SUITE);
}

#[test]
fn enabled_queries_still_fail_the_suite() {
    let invalid = SUITE.replace("{ name: run }", "{ name: missing }");
    assert!(std::panic::catch_unwind(|| run_incremental_suite(&invalid)).is_err());
}

#[test]
fn an_entirely_unsupported_suite_needs_no_fixture() {
    run_incremental_suite(
        "name: Unsupported\ntests:\n  - name: expansion\n    supported: false\n    query: invalid\n",
    );
}
