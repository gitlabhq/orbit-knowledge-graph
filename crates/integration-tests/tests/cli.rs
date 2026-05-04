//! CLI integration tests.
//!
//! Spawns real `orbit` processes (separate PIDs, separate DuckDB
//! connections) to validate indexing, querying, worktree support,
//! and concurrent access.
//!
//! Run with: `cargo nextest run --test cli`

use std::collections::{BTreeSet, HashMap};
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
fn indexes_non_parsable_git_tree_files() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();
    let repo = workspace.path().join("repo");
    init_repo_at(
        &repo,
        &[
            ("src/main.py", "def hello(): pass\n"),
            ("README.md", "# Project\n"),
            ("config/app.yml", "enabled: true\n"),
            ("Dockerfile", "FROM scratch\n"),
            (".gitignore", "target/\n"),
            ("assets/logo.png", "fake png bytes\n"),
            ("docs/only/README.md", "# Nested\n"),
            ("docs/deleted.md", "# Deleted\n"),
        ],
    );
    std::fs::remove_file(repo.join("docs/deleted.md")).unwrap();
    std::fs::create_dir_all(repo.join("notes")).unwrap();
    std::fs::write(repo.join("notes/local.md"), "# Local\n").unwrap();
    std::fs::create_dir_all(repo.join("target")).unwrap();
    std::fs::write(repo.join("target/ignored.md"), "# Ignored\n").unwrap();

    assert!(orbit_index(&repo, data_dir.path()));

    let files = orbit_query(&q("files_simple"), data_dir.path());
    let paths: Vec<_> = nodes(&files)
        .into_iter()
        .filter_map(|node| node["path"].as_str().map(str::to_string))
        .collect();
    let unique_paths: BTreeSet<_> = paths.iter().cloned().collect();
    assert_eq!(
        paths.len(),
        unique_paths.len(),
        "duplicate File nodes: {paths:?}"
    );
    assert_eq!(
        unique_paths,
        BTreeSet::from([
            ".gitignore".to_string(),
            "Dockerfile".to_string(),
            "README.md".to_string(),
            "assets/logo.png".to_string(),
            "config/app.yml".to_string(),
            "docs/only/README.md".to_string(),
            "notes/local.md".to_string(),
            "src/main.py".to_string(),
        ])
    );

    let traversal = serde_json::json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "d", "entity": "Directory", "filters": {"path": "config"}, "columns": ["id", "path"]},
            {"id": "f", "entity": "File", "filters": {"path": "config/app.yml"}, "columns": ["id", "path"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "d", "to": "f"}],
        "limit": 5
    });
    let result = orbit_query(&serde_json::to_string(&traversal).unwrap(), data_dir.path());
    assert_eq!(edge_count(&result), 1);
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
        .as_str()
        .unwrap()
        .to_string();
    let helper_id = nodes_where(&files, "name", "helper.py")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
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

// ── Schema introspection ────────────────────────────────────────

fn run_schema(args: &[&str]) -> (String, String, bool) {
    let out = orbit_cmd().args(args).output().expect("spawn orbit");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[test]
fn schema_default_is_local_scope() {
    let (stdout, stderr, ok) = run_schema(&["schema", "--ontology"]);
    assert!(ok, "orbit schema --ontology failed: {stderr}");

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
    let (stdout, _, ok) = run_schema(&["schema", "--ontology"]);
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
    let (stdout, stderr, ok) = run_schema(&["schema", "--ontology", "--expand", "File"]);
    assert!(ok, "stderr: {stderr}");
    assert!(
        stdout.contains("path:string"),
        "missing path:string: {stdout}"
    );
    assert!(stdout.contains("props"), "missing props key: {stdout}");
}

#[test]
fn schema_raw_is_parseable_json() {
    let (stdout, _, ok) = run_schema(&["schema", "--ontology", "--raw"]);
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
    let (stdout, _, ok) = run_schema(&["schema", "--ontology", "--all"]);
    assert!(ok);
    assert!(stdout.contains("User"), "--all should include User");
    assert!(stdout.contains("AUTHORED"), "--all should include AUTHORED");
}

#[test]
fn debug_ddl_produces_clickhouse_statements() {
    let (stdout, stderr, ok) = run_schema(&["debug", "ddl"]);
    assert!(ok, "debug ddl failed: {stderr}");
    assert!(
        stdout.contains("CREATE TABLE"),
        "expected DDL output, got: {}",
        &stdout.chars().take(200).collect::<String>()
    );
}

#[test]
fn old_schema_subcommand_no_longer_emits_ddl() {
    let (stdout, _, ok) = run_schema(&["schema", "--ontology"]);
    assert!(ok);
    assert!(
        !stdout.contains("CREATE TABLE"),
        "orbit schema must not emit DDL anymore: {stdout}"
    );
}

#[test]
fn schema_expand_without_value_errors() {
    let (_, stderr, ok) = run_schema(&["schema", "--ontology", "--expand"]);
    assert!(!ok, "--expand without a value should fail");
    assert!(
        stderr.contains("--expand") || stderr.contains("NODE"),
        "stderr should mention the missing NODE value: {stderr}"
    );
}

#[test]
fn schema_bare_requires_flag() {
    let (_, stderr, ok) = run_schema(&["schema"]);
    assert!(
        !ok,
        "orbit schema without --ontology or --query should fail"
    );
    assert!(
        stderr.contains("--ontology") || stderr.contains("--query"),
        "stderr should hint at required flags: {stderr}"
    );
}

#[test]
fn schema_query_returns_dsl() {
    let (stdout, stderr, ok) = run_schema(&["schema", "--query"]);
    assert!(ok, "orbit schema --query failed: {stderr}");
    assert!(stdout.contains("query_type"), "should contain query_type");
    assert!(stdout.contains("traversal"), "should contain traversal");
    assert!(
        stdout.contains("NodeSelector"),
        "should contain NodeSelector"
    );
}

#[test]
fn schema_query_raw_is_json() {
    let (stdout, stderr, ok) = run_schema(&["schema", "--query", "--raw"]);
    assert!(ok, "orbit schema --query --raw failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("should be parseable JSON");
    assert!(v.is_object(), "should be a JSON object");
}
