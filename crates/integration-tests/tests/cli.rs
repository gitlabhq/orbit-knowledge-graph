//! CLI integration tests.
//!
//! Spawns real `orbit` processes (separate PIDs, separate DuckDB
//! connections) to validate indexing, querying, worktree support,
//! and concurrent access.
//!
//! Run with: `cargo nextest run --test cli`

use std::collections::HashMap;
use std::sync::LazyLock;

use integration_testkit::cli::{
    create_test_repo, edge_count, git, init_repo_at, nodes, nodes_where, orbit_cmd, orbit_index,
    orbit_query, sorted_node_ids,
};
use serde_json::Value;

// ── Query fixtures ──────────────────────────────────────────────

static QUERIES: LazyLock<HashMap<String, Value>> =
    LazyLock::new(|| serde_json::from_str(include_str!("../fixtures/queries/cli.json")).unwrap());

fn q(name: &str) -> String {
    serde_json::to_string(&QUERIES[name]).unwrap()
}

// ── Worktree ────────────────────────────────────────────────────

#[test]
fn worktree_tracking() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let main_sha = git(&repo.path, &["rev-parse", "HEAD"]);
    let main_branch = git(&repo.path, &["symbolic-ref", "--short", "HEAD"]);

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

    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));
    assert!(orbit_index(&wt_feat, dd));
    assert!(orbit_index(&wt_fix, dd));

    let files = orbit_query(&q("files"), dd);
    let trav = orbit_query(&q("traversal"), dd);

    // Branches
    assert!(!nodes_where(&files, "branch", &main_branch).is_empty());
    assert!(!nodes_where(&files, "branch", "feature/tests").is_empty());
    assert!(!nodes_where(&files, "branch", "fix/utils").is_empty());

    // Commits
    assert!(!nodes_where(&files, "commit_sha", &main_sha).is_empty());
    assert!(!nodes_where(&files, "commit_sha", &feat_sha).is_empty());
    assert!(!nodes_where(&files, "commit_sha", &fix_sha).is_empty());

    // Branch-specific files
    assert_eq!(nodes_where(&files, "name", "tests.py").len(), 1);
    assert_eq!(nodes_where(&files, "name", "main.py").len(), 3);

    // Content from correct worktree
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

// ── Concurrency ─────────────────────────────────────────────────

#[test]
fn concurrent_readers() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let q = q("files_simple");
    let children: Vec<_> = (0..5)
        .map(|_| {
            orbit_cmd()
                .args(["query", "--raw", &q])
                .env("ORBIT_DATA_DIR", data_dir.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();

    let results: Vec<Value> = children
        .into_iter()
        .map(|c| {
            let out = c.wait_with_output().unwrap();
            assert!(out.status.success());
            serde_json::from_slice(&out.stdout).unwrap()
        })
        .collect();

    let baseline = sorted_node_ids(&results[0]);
    for r in &results[1..] {
        assert_eq!(baseline, sorted_node_ids(r));
    }
}

#[test]
fn reader_during_writer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let mut child = orbit_cmd()
        .args(["index", repo.path.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .spawn()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let result = orbit_query(&q("files_simple"), data_dir.path());
    assert!(!nodes(&result).is_empty());

    assert!(child.wait().unwrap().success());
}

#[test]
fn concurrent_writers() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let children: Vec<_> = (0..2)
        .map(|_| {
            orbit_cmd()
                .args(["index", repo.path.to_str().unwrap()])
                .env("ORBIT_DATA_DIR", data_dir.path())
                .spawn()
                .unwrap()
        })
        .collect();

    let mut ok = 0;
    for mut c in children {
        if c.wait().unwrap().success() {
            ok += 1;
        }
    }
    assert!(ok > 0, "at least one writer should succeed");

    let result = orbit_query(&q("files_simple"), data_dir.path());
    assert!(!nodes(&result).is_empty());
}

#[test]
fn reindex_idempotent() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();

    assert!(orbit_index(&repo.path, data_dir.path()));
    assert!(orbit_index(&repo.path, data_dir.path()));

    let result = orbit_query(&q("files_simple"), data_dir.path());
    assert_eq!(nodes(&result).len(), 2);
}

#[test]
fn sequential_read_consistency() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let baseline = sorted_node_ids(&orbit_query(&q("files_simple"), data_dir.path()));
    for _ in 0..10 {
        assert_eq!(
            baseline,
            sorted_node_ids(&orbit_query(&q("files_simple"), data_dir.path()))
        );
    }
}

// ── Nested repos ────────────────────────────────────────────────

#[test]
fn nested_repos_indexed_separately() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();

    let parent = workspace.path().join("parent");
    init_repo_at(&parent, &[("src/app.py", "def app(): pass\n")]);

    let nested = parent.join("libs/utils");
    init_repo_at(&nested, &[("src/helper.py", "def helper(): pass\n")]);

    let dd = data_dir.path();
    assert!(orbit_index(&parent, dd));
    assert!(orbit_index(&nested, dd));

    let files = orbit_query(&q("files"), dd);

    // Both repos indexed, files from each present
    assert!(
        !nodes_where(&files, "name", "app.py").is_empty(),
        "parent repo file missing"
    );
    assert!(
        !nodes_where(&files, "name", "helper.py").is_empty(),
        "nested repo file missing"
    );

    // Different project IDs (different canonical paths)
    let app_id = nodes_where(&files, "name", "app.py")[0]["id"]
        .as_i64()
        .unwrap();
    let helper_id = nodes_where(&files, "name", "helper.py")[0]["id"]
        .as_i64()
        .unwrap();
    assert_ne!(app_id, helper_id);
}

#[test]
fn nested_repo_content_isolation() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();

    let repo_a = workspace.path().join("repo-a");
    init_repo_at(&repo_a, &[("src/main.py", "def version_a(): pass\n")]);

    let repo_b = workspace.path().join("repo-b");
    init_repo_at(&repo_b, &[("src/main.py", "def version_b(): pass\n")]);

    let dd = data_dir.path();
    assert!(orbit_index(&repo_a, dd));
    assert!(orbit_index(&repo_b, dd));

    let files = orbit_query(&q("files"), dd);
    let mains = nodes_where(&files, "name", "main.py");
    assert_eq!(mains.len(), 2, "expected main.py from both repos");

    // Content resolves from the correct repo
    let contents: Vec<&str> = mains
        .iter()
        .map(|n| n["content"].as_str().unwrap())
        .collect();
    assert!(
        contents.iter().any(|c| c.contains("version_a")),
        "repo-a content missing"
    );
    assert!(
        contents.iter().any(|c| c.contains("version_b")),
        "repo-b content missing"
    );
}

#[test]
fn reindex_nested_doesnt_affect_parent() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();

    let parent = workspace.path().join("parent");
    init_repo_at(&parent, &[("src/app.py", "def app(): pass\n")]);

    let nested = parent.join("libs/core");
    init_repo_at(&nested, &[("src/core.py", "def core(): pass\n")]);

    let dd = data_dir.path();
    assert!(orbit_index(&parent, dd));
    assert!(orbit_index(&nested, dd));

    let before = nodes_where(&orbit_query(&q("files_simple"), dd), "name", "app.py").len();

    // Re-index only the nested repo
    std::fs::write(nested.join("src/extra.py"), "def extra(): pass\n").unwrap();
    git(&nested, &["add", "-A"]);
    git(&nested, &["commit", "-m", "add extra"]);
    assert!(orbit_index(&nested, dd));

    let files = orbit_query(&q("files_simple"), dd);
    let after = nodes_where(&files, "name", "app.py").len();

    // Parent repo data unchanged
    assert_eq!(before, after, "parent files should not change");
    // Nested repo has new file
    assert!(
        !nodes_where(&files, "name", "extra.py").is_empty(),
        "new nested file missing"
    );
}
