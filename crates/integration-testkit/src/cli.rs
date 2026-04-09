//! Harness for running CLI integration test scripts.
//!
//! Shell scripts output JSON `{"pass": N, "fail": N, "tests": [...]}`.
//! The harness invokes a script, parses the output, and asserts all
//! sub-tests passed.

use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;

#[derive(Deserialize)]
pub struct TestResult {
    #[allow(dead_code)]
    pub pass: usize,
    pub fail: usize,
    pub tests: Vec<SubTest>,
}

#[derive(Deserialize)]
pub struct SubTest {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

/// Locate the `orbit` binary relative to the test executable.
pub fn orbit_bin() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // debug
    p.push("orbit");
    assert!(p.exists(), "orbit binary not found at {}", p.display());
    p
}

/// Library path for DuckDB dynamic library (next to the orbit binary).
pub fn lib_path() -> PathBuf {
    let mut p = orbit_bin();
    p.pop();
    p.push("deps");
    p
}

/// Project root (two levels up from `CARGO_MANIFEST_DIR` of
/// integration-testkit).
pub fn project_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates
    p.pop(); // root
    p
}

/// Run a CLI test script and parse its JSON output.
///
/// Scripts live in `crates/integration-tests/cli/`. The orbit binary
/// path and `ORBIT_LIB_PATH` are passed automatically.
pub fn run_script(script: &str, extra_args: &[&str]) -> TestResult {
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
    cmd.env("ORBIT_LIB_PATH", lib_path());

    let output = cmd.output().expect("failed to run test script");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let result: TestResult = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("failed to parse script JSON output: {e}\nstdout: {stdout}\nstderr: {stderr}");
    });

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

/// Panic if any sub-test failed, listing the failures.
pub fn assert_all_passed(result: &TestResult) {
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
