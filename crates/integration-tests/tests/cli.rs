//! CLI integration tests.
//!
//! Spawns real `orbit` processes (separate PIDs, separate DuckDB
//! connections) to validate indexing, querying, worktree support,
//! and concurrent access.
//!
//! Run with: `cargo nextest run --test cli`

use std::path::{Path, PathBuf};
use std::process::Command;

use gitalisk_core::repository::testing::local::LocalGitRepository;
use serde_json::Value;

fn orbit_bin() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // debug
    p.push("orbit");
    assert!(p.exists(), "orbit not found at {}", p.display());
    p
}

fn orbit_cmd() -> Command {
    let bin = orbit_bin();
    let mut lib = bin.clone();
    lib.pop();
    lib.push("deps");

    let mut cmd = Command::new(&bin);
    cmd.env("DYLD_LIBRARY_PATH", &lib);
    cmd.env("LD_LIBRARY_PATH", &lib);
    cmd
}

fn orbit_index(repo: &Path, data_dir: &Path) -> bool {
    orbit_cmd()
        .args(["index", repo.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap()
        .status
        .success()
}

fn orbit_query(query: &str, data_dir: &Path) -> Value {
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

fn nodes(v: &Value) -> Vec<&Value> {
    v["nodes"].as_array().unwrap().iter().collect()
}

fn nodes_where<'a>(v: &'a Value, field: &str, val: &str) -> Vec<&'a Value> {
    nodes(v)
        .into_iter()
        .filter(|n| n[field].as_str() == Some(val))
        .collect()
}

fn edge_count(v: &Value) -> usize {
    v["edges"].as_array().map_or(0, |a| a.len())
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {:?} failed", args);
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn create_repo() -> LocalGitRepository {
    let mut repo = LocalGitRepository::new(None);
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

const Q_FILES: &str = r#"{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path","branch","commit_sha","content"]},"limit":50}"#;
const Q_SIMPLE: &str = r#"{"query_type":"search","node":{"id":"f","entity":"File","columns":["id","name","path"]},"limit":50}"#;
const Q_TRAV: &str = r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["id","name"]},{"id":"d","entity":"Definition","columns":["id","name"]}],"relationships":[{"type":"DEFINES","from":"f","to":"d"}],"limit":10}"#;

// ── Worktree test ───────────────────────────────────────────────

#[test]
fn worktree_tracking() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();
    let main_sha = git(&repo.path, &["rev-parse", "HEAD"]);
    let main_branch = git(&repo.path, &["symbolic-ref", "--short", "HEAD"]);

    // Feature worktree
    let wt_feat = repo.workspace_path.join("wt-feat");
    git(
        &repo.path,
        &[
            "worktree",
            "add",
            "-b",
            "feature/tests",
            wt_feat.to_str().unwrap(),
        ],
    );
    std::fs::write(wt_feat.join("src/tests.py"), "def test_hello(): pass\n").unwrap();
    git(&wt_feat, &["add", "-A"]);
    git(&wt_feat, &["commit", "-m", "add tests"]);
    let feat_sha = git(&wt_feat, &["rev-parse", "HEAD"]);

    // Fix worktree (from initial commit)
    let wt_fix = repo.workspace_path.join("wt-fix");
    git(
        &repo.path,
        &[
            "worktree",
            "add",
            "-b",
            "fix/utils",
            wt_fix.to_str().unwrap(),
            &main_sha,
        ],
    );
    std::fs::write(wt_fix.join("src/utils.py"), "def patched(): return True\n").unwrap();
    git(&wt_fix, &["add", "-A"]);
    git(&wt_fix, &["commit", "-m", "patch utils"]);
    let fix_sha = git(&wt_fix, &["rev-parse", "HEAD"]);

    // Index all three
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));
    assert!(orbit_index(&wt_feat, dd));
    assert!(orbit_index(&wt_fix, dd));

    let files = orbit_query(Q_FILES, dd);
    let trav = orbit_query(Q_TRAV, dd);

    // Branch tracking
    assert!(!nodes_where(&files, "branch", &main_branch).is_empty());
    assert!(!nodes_where(&files, "branch", "feature/tests").is_empty());
    assert!(!nodes_where(&files, "branch", "fix/utils").is_empty());

    // Commit SHA tracking
    assert!(!nodes_where(&files, "commit_sha", &main_sha).is_empty());
    assert!(!nodes_where(&files, "commit_sha", &feat_sha).is_empty());
    assert!(!nodes_where(&files, "commit_sha", &fix_sha).is_empty());

    // Branch-specific files
    assert_eq!(nodes_where(&files, "name", "tests.py").len(), 1);
    assert_eq!(nodes_where(&files, "name", "main.py").len(), 3);

    // Content resolves from correct worktree
    let fix_utils: Vec<_> = nodes(&files)
        .into_iter()
        .filter(|n| n["name"] == "utils.py" && n["branch"] == "fix/utils")
        .collect();
    assert!(
        fix_utils[0]["content"]
            .as_str()
            .unwrap()
            .contains("patched")
    );

    let feat_tests: Vec<_> = nodes(&files)
        .into_iter()
        .filter(|n| n["name"] == "tests.py")
        .collect();
    assert!(
        feat_tests[0]["content"]
            .as_str()
            .unwrap()
            .contains("test_hello")
    );

    // Traversal
    assert!(edge_count(&trav) > 0);
}

// ── Concurrency tests ───────────────────────────────────────────

#[test]
fn concurrent_readers() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let children: Vec<_> = (0..5)
        .map(|_| {
            orbit_cmd()
                .args(["query", "--raw", Q_SIMPLE])
                .env("ORBIT_DATA_DIR", data_dir.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();

    let outputs: Vec<String> = children
        .into_iter()
        .map(|c| {
            let out = c.wait_with_output().unwrap();
            assert!(out.status.success());
            String::from_utf8(out.stdout).unwrap()
        })
        .collect();

    // All readers return the same node IDs
    let first: Value = serde_json::from_str(&outputs[0]).unwrap();
    let mut first_ids: Vec<i64> = nodes(&first)
        .iter()
        .map(|n| n["id"].as_i64().unwrap())
        .collect();
    first_ids.sort();

    for output in &outputs[1..] {
        let v: Value = serde_json::from_str(output).unwrap();
        let mut ids: Vec<i64> = nodes(&v)
            .iter()
            .map(|n| n["id"].as_i64().unwrap())
            .collect();
        ids.sort();
        assert_eq!(
            first_ids, ids,
            "concurrent readers returned different results"
        );
    }
}

#[test]
fn reader_during_writer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    // Start indexer in background
    let mut writer = orbit_cmd();
    writer
        .args(["index", repo.path.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir.path());
    let mut child = writer.spawn().unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    // Query while indexer runs
    let result = orbit_query(Q_SIMPLE, data_dir.path());
    assert!(!nodes(&result).is_empty());

    assert!(child.wait().unwrap().success());
}

#[test]
fn concurrent_writers() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let handles: Vec<_> = (0..2)
        .map(|_| {
            let mut cmd = orbit_cmd();
            cmd.args(["index", repo.path.to_str().unwrap()])
                .env("ORBIT_DATA_DIR", data_dir.path());
            cmd.spawn().unwrap()
        })
        .collect();

    let mut succeeded = 0;
    for mut h in handles {
        if h.wait().unwrap().success() {
            succeeded += 1;
        }
    }
    assert!(succeeded > 0, "at least one writer should succeed");

    // Data intact
    let result = orbit_query(Q_SIMPLE, data_dir.path());
    assert!(!nodes(&result).is_empty());
}

#[test]
fn reindex_idempotent() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();

    assert!(orbit_index(&repo.path, data_dir.path()));
    assert!(orbit_index(&repo.path, data_dir.path()));

    let result = orbit_query(Q_SIMPLE, data_dir.path());
    assert_eq!(nodes(&result).len(), 2, "re-index should not duplicate");
}

#[test]
fn sequential_read_consistency() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let baseline = orbit_query(Q_SIMPLE, data_dir.path());
    let mut baseline_ids: Vec<i64> = nodes(&baseline)
        .iter()
        .map(|n| n["id"].as_i64().unwrap())
        .collect();
    baseline_ids.sort();

    for _ in 0..10 {
        let result = orbit_query(Q_SIMPLE, data_dir.path());
        let mut ids: Vec<i64> = nodes(&result)
            .iter()
            .map(|n| n["id"].as_i64().unwrap())
            .collect();
        ids.sort();
        assert_eq!(baseline_ids, ids, "sequential reads should be consistent");
    }
}
