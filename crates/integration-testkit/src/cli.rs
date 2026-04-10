//! CLI integration test harness.
//!
//! Helpers for spawning real `orbit` processes and asserting on their
//! JSON output. Each process gets its own PID and DuckDB connection.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

// ── Binary helpers ──────────────────────────────────────────────

pub fn orbit_bin() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // debug
    p.push("orbit");
    assert!(p.exists(), "orbit not found at {}", p.display());
    p
}

pub fn orbit_cmd() -> Command {
    let bin = orbit_bin();
    let mut lib = bin.clone();
    lib.pop();
    lib.push("deps");

    let mut cmd = Command::new(&bin);
    cmd.env("DYLD_LIBRARY_PATH", &lib);
    cmd.env("LD_LIBRARY_PATH", &lib);
    cmd
}

pub fn orbit_index(repo: &Path, data_dir: &Path) -> bool {
    orbit_cmd()
        .args(["index", repo.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap()
        .status
        .success()
}

pub fn orbit_query(query: &str, data_dir: &Path) -> Value {
    let out = orbit_cmd()
        .args(["query", "--raw", query])
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("invalid JSON")
}

// ── JSON helpers ────────────────────────────────────────────────

pub fn nodes(v: &Value) -> Vec<&Value> {
    v["nodes"].as_array().unwrap().iter().collect()
}

pub fn nodes_where<'a>(v: &'a Value, field: &str, val: &str) -> Vec<&'a Value> {
    nodes(v)
        .into_iter()
        .filter(|n| n[field].as_str() == Some(val))
        .collect()
}

pub fn edge_count(v: &Value) -> usize {
    v["edges"].as_array().map_or(0, |a| a.len())
}

pub fn sorted_node_ids(v: &Value) -> Vec<i64> {
    let mut ids: Vec<i64> = nodes(v).iter().map(|n| n["id"].as_i64().unwrap()).collect();
    ids.sort();
    ids
}

// ── Git helpers ─────────────────────────────────────────────────

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

// ── Repo helpers ────────────────────────────────────────────────

/// Create a git repo at a specific path with Python files.
pub fn init_repo_at(path: &Path, files: &[(&str, &str)]) {
    std::fs::create_dir_all(path).unwrap();
    for (name, content) in files {
        let file_path = path.join(name);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&file_path, content).unwrap();
    }
    git(path, &["init"]);
    git(path, &["config", "user.email", "test@test.com"]);
    git(path, &["config", "user.name", "Test"]);
    git(path, &["add", "-A"]);
    git(path, &["commit", "-m", "initial"]);
}

pub fn create_test_repo() -> gitalisk_core::repository::testing::local::LocalGitRepository {
    let mut repo = gitalisk_core::repository::testing::local::LocalGitRepository::new(None);
    repo.fs.create_file(
        "src/main.py",
        Some(
            "def hello():\n    print('hello')\n\nclass App:\n    def run(self):\n        hello()\n",
        ),
    );
    repo.fs.create_file(
        "src/utils.py",
        Some("import os\n\ndef read_file(path):\n    return open(path).read()\n"),
    );
    repo.add_all().commit("initial");
    repo
}
