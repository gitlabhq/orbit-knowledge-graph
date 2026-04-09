//! CLI integration tests.
//!
//! Thin harness that invokes shell test scripts, parses their JSON
//! output, and asserts all sub-tests passed. Each script spawns real
//! `orbit` processes against temp repos and a fresh DuckDB file.
//!
//! Run with: `cargo nextest run --test cli`
//!
//! Requires: `orbit` binary built, `jq` on PATH, `git` on PATH.

use std::path::PathBuf;
use std::process::Command;

use gitalisk_core::repository::testing::local::LocalGitRepository;

#[derive(serde::Deserialize)]
struct TestResult {
    #[allow(dead_code)]
    pass: usize,
    fail: usize,
    tests: Vec<SubTest>,
}

#[derive(serde::Deserialize)]
struct SubTest {
    name: String,
    ok: bool,
    detail: String,
}

fn project_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates
    p.pop(); // root
    p
}

fn orbit_bin() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // debug
    p.push("orbit");
    assert!(p.exists(), "orbit binary not found at {}", p.display());
    p
}

fn lib_path() -> PathBuf {
    let mut p = orbit_bin();
    p.pop();
    p.push("deps");
    p
}

fn run_script(script: &str, extra_args: &[&str]) -> TestResult {
    let root = project_root();
    let script_path = root.join("crates/integration-tests/cli").join(script);
    assert!(
        script_path.exists(),
        "script not found: {}",
        script_path.display()
    );

    let mut cmd = Command::new("bash");
    cmd.arg(&script_path);
    cmd.arg(orbit_bin());
    cmd.args(extra_args);
    // Pass library path so scripts can export it for orbit child processes.
    // On macOS, DYLD_LIBRARY_PATH is stripped by SIP across process
    // boundaries, so scripts must re-set it explicitly.
    cmd.env("ORBIT_LIB_PATH", lib_path());
    // Each test gets its own data dir via the scripts.

    let output = cmd.output().expect("failed to run test script");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let result: TestResult = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("failed to parse script JSON output: {e}\nstdout: {stdout}\nstderr: {stderr}");
    });

    // Print sub-test results for nextest output
    for t in &result.tests {
        let status = if t.ok { "PASS" } else { "FAIL" };
        let detail = if t.detail.is_empty() {
            String::new()
        } else {
            format!(" ({})", t.detail)
        };
        eprintln!("  {status}: {}{detail}", t.name);
    }

    result
}

fn assert_all_passed(result: &TestResult) {
    if result.fail > 0 {
        let failures: Vec<String> = result
            .tests
            .iter()
            .filter(|t| !t.ok)
            .map(|t| format!("{}: {}", t.name, t.detail))
            .collect();
        panic!(
            "{} sub-test(s) failed:\n  {}",
            result.fail,
            failures.join("\n  ")
        );
    }
}

/// Concurrency: concurrent readers, reader during writer, concurrent
/// writers, data integrity, read consistency.
#[test]
fn cli_concurrency() {
    let mut repo = LocalGitRepository::new(None);
    repo.fs
        .create_file("src/main.py", Some("def hello():\n    pass\n"));
    repo.fs
        .create_file("src/utils.py", Some("def read():\n    pass\n"));
    repo.add_all().commit("initial");
    let result = run_script("cli-test-concurrency.sh", &[repo.path.to_str().unwrap()]);
    assert_all_passed(&result);
}

/// Worktree: branch tracking, commit SHAs, branch-specific files,
/// unique IDs per branch, content resolution from correct worktree,
/// cross-branch traversal.
///
/// Creates temp repos internally -- no external repo needed.
#[test]
fn cli_worktree() {
    let result = run_script("cli-test-worktree.sh", &[]);
    assert_all_passed(&result);
}
