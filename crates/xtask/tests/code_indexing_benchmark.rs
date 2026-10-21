use std::process::Command;

#[test]
fn incremental_engine_is_a_no_op_before_creating_the_root() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("benchmark");
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(["code-indexing-benchmark", "run", "--cgi", "--root"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("not wired yet"));
    assert!(!root.exists());
}

#[test]
fn benchmark_requires_exactly_one_engine() {
    let missing = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(["code-indexing-benchmark", "smoke"])
        .output()
        .unwrap();
    assert!(!missing.status.success());

    let conflicting = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(["code-indexing-benchmark", "smoke", "--cg", "--cgi"])
        .output()
        .unwrap();
    assert!(!conflicting.status.success());
}
