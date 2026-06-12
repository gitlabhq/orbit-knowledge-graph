//! CLI integration tests.
//!
//! Spawns real `orbit` processes (separate PIDs, separate DuckDB
//! connections) to validate indexing, schema introspection, worktree
//! support, and concurrent access — all driven through raw SQL.
//!
//! Run with: `cargo nextest run --test cli`

use std::collections::BTreeSet;

use integration_testkit::cli::{
    create_test_repo, git, init_repo_at, mcp_roundtrip, mcp_tool_call, mcp_tool_text, orbit_cmd,
    orbit_index, orbit_sql, rows, rows_where, sorted_ids,
};
use serde_json::{Value, json};

const FILES_FULL: &str = "SELECT id, name, path, branch, commit_sha FROM gl_file WHERE name IS NOT NULL ORDER BY path LIMIT 50";
const FILES_SIMPLE: &str =
    "SELECT id, name, path FROM gl_file WHERE name IS NOT NULL ORDER BY path LIMIT 50";

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

    let files = orbit_sql(FILES_FULL, dd);

    assert!(!rows_where(&files, "branch", &main_branch).is_empty());
    assert!(!rows_where(&files, "branch", "feature/tests").is_empty());
    assert!(!rows_where(&files, "branch", "fix/utils").is_empty());

    assert!(!rows_where(&files, "commit_sha", &main_sha).is_empty());
    assert!(!rows_where(&files, "commit_sha", &feat_sha).is_empty());
    assert!(!rows_where(&files, "commit_sha", &fix_sha).is_empty());

    assert_eq!(rows_where(&files, "name", "tests.py").len(), 1);
    assert_eq!(rows_where(&files, "name", "main.py").len(), 3);

    let edges = orbit_sql(
        "SELECT source_id, target_id FROM gl_edge WHERE relationship_kind = 'DEFINES' LIMIT 1",
        dd,
    );
    assert!(
        !rows(&edges).is_empty(),
        "expected at least one DEFINES edge"
    );
}

// ── Concurrency ─────────────────────────────────────────────────

#[test]
fn concurrent_readers() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let children: Vec<_> = (0..5)
        .map(|_| {
            orbit_cmd()
                .args(["sql", "-F", "json", FILES_SIMPLE])
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

    let baseline = sorted_ids(&results[0]);
    for r in &results[1..] {
        assert_eq!(baseline, sorted_ids(r));
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

    let result = orbit_sql(FILES_SIMPLE, data_dir.path());
    assert!(!rows(&result).is_empty());

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

    let result = orbit_sql(FILES_SIMPLE, data_dir.path());
    assert!(!rows(&result).is_empty());
}

#[test]
fn reindex_idempotent() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();

    assert!(orbit_index(&repo.path, data_dir.path()));
    assert!(orbit_index(&repo.path, data_dir.path()));

    let result = orbit_sql(FILES_SIMPLE, data_dir.path());
    assert_eq!(rows(&result).len(), 2);
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

    let files = orbit_sql(FILES_SIMPLE, data_dir.path());
    let paths: Vec<_> = rows(&files)
        .into_iter()
        .filter_map(|node| node["path"].as_str().map(str::to_string))
        .collect();
    let unique_paths: BTreeSet<_> = paths.iter().cloned().collect();
    assert_eq!(
        paths.len(),
        unique_paths.len(),
        "duplicate File rows: {paths:?}"
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

    let result = orbit_sql(
        "SELECT count(*) AS c FROM gl_edge e \
         JOIN gl_directory d ON d.id = e.source_id \
         JOIN gl_file f ON f.id = e.target_id \
         WHERE e.relationship_kind = 'CONTAINS' AND d.path = 'config' AND f.path = 'config/app.yml'",
        data_dir.path(),
    );
    assert_eq!(rows(&result)[0]["c"].as_i64(), Some(1));
}

#[test]
fn sequential_read_consistency() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let baseline = sorted_ids(&orbit_sql(FILES_SIMPLE, data_dir.path()));
    for _ in 0..10 {
        assert_eq!(
            baseline,
            sorted_ids(&orbit_sql(FILES_SIMPLE, data_dir.path()))
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

    let files = orbit_sql(FILES_FULL, dd);

    assert!(
        !rows_where(&files, "name", "app.py").is_empty(),
        "parent repo file missing"
    );
    assert!(
        !rows_where(&files, "name", "helper.py").is_empty(),
        "nested repo file missing"
    );

    let app_id = rows_where(&files, "name", "app.py")[0]["id"]
        .as_i64()
        .unwrap();
    let helper_id = rows_where(&files, "name", "helper.py")[0]["id"]
        .as_i64()
        .unwrap();
    assert_ne!(app_id, helper_id);
}

#[test]
fn nested_repos_have_distinct_projects() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();

    let repo_a = workspace.path().join("repo-a");
    init_repo_at(&repo_a, &[("src/main.py", "def version_a(): pass\n")]);

    let repo_b = workspace.path().join("repo-b");
    init_repo_at(&repo_b, &[("src/main.py", "def version_b(): pass\n")]);

    let dd = data_dir.path();
    assert!(orbit_index(&repo_a, dd));
    assert!(orbit_index(&repo_b, dd));

    let mains = orbit_sql(
        "SELECT id, project_id, path FROM gl_file WHERE name = 'main.py' ORDER BY project_id",
        dd,
    );
    let rows = rows(&mains);
    assert_eq!(rows.len(), 2, "expected main.py from both repos");
    assert_ne!(
        rows[0]["project_id"].as_i64(),
        rows[1]["project_id"].as_i64(),
        "nested repos must have distinct project_ids"
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

    let before = rows_where(&orbit_sql(FILES_SIMPLE, dd), "name", "app.py").len();

    std::fs::write(nested.join("src/extra.py"), "def extra(): pass\n").unwrap();
    git(&nested, &["add", "-A"]);
    git(&nested, &["commit", "-m", "add extra"]);
    assert!(orbit_index(&nested, dd));

    let files = orbit_sql(FILES_SIMPLE, dd);
    let after = rows_where(&files, "name", "app.py").len();

    assert_eq!(before, after, "parent files should not change");
    assert!(
        !rows_where(&files, "name", "extra.py").is_empty(),
        "new nested file missing"
    );
}

// ── Schema introspection ────────────────────────────────────────

fn run_cmd(args: &[&str], data_dir: &std::path::Path) -> (String, String, bool) {
    let out = orbit_cmd()
        .args(args)
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .expect("spawn orbit");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[test]
fn schema_lists_duckdb_tables_and_columns() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (stdout, stderr, ok) = run_cmd(&["schema"], data_dir.path());
    assert!(ok, "orbit schema failed: {stderr}");

    for want in [
        "gl_directory",
        "gl_file",
        "gl_definition",
        "gl_imported_symbol",
        "gl_edge",
    ] {
        assert!(stdout.contains(want), "missing table {want}: {stdout}");
    }
    for want in ["id", "name", "path", "project_id"] {
        assert!(stdout.contains(want), "missing column {want}: {stdout}");
    }
}

#[test]
fn schema_raw_is_parseable_json() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (stdout, stderr, ok) = run_cmd(&["schema", "--raw"], data_dir.path());
    assert!(ok, "orbit schema --raw failed: {stderr}");

    let v: Value = serde_json::from_str(&stdout).expect("parseable JSON");
    let tables: BTreeSet<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["table_name"].as_str().unwrap())
        .collect();
    assert!(tables.contains("gl_file"));
    assert!(tables.contains("gl_edge"));
}

#[test]
fn schema_errors_when_db_missing() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let (_, stderr, ok) = run_cmd(&["schema"], data_dir.path());
    assert!(!ok, "schema should fail without an indexed graph");
    assert!(
        stderr.contains("no local graph found"),
        "expected missing-graph error: {stderr}"
    );
}

#[test]
fn schema_scoped_excludes_unrequested_tables() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (stdout, stderr, ok) = run_cmd(&["schema", "gl_file"], data_dir.path());
    assert!(ok, "orbit schema gl_file failed: {stderr}");

    assert!(stdout.contains("gl_file"), "expected gl_file in output");
    assert!(
        !stdout.contains("gl_directory"),
        "gl_directory should not be in scoped output"
    );
    assert!(
        !stdout.contains("gl_edge"),
        "gl_edge should not be in scoped output"
    );
}

#[test]
fn schema_scoped_raw_contains_only_requested_table() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (stdout, stderr, ok) = run_cmd(&["schema", "--raw", "gl_definition"], data_dir.path());
    assert!(ok, "orbit schema --raw gl_definition failed: {stderr}");

    let v: Value = serde_json::from_str(&stdout).expect("parseable JSON");
    let tables: BTreeSet<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["table_name"].as_str().unwrap())
        .collect();

    assert_eq!(tables.len(), 1, "should contain only gl_definition");
    assert!(tables.contains("gl_definition"));
}

#[test]
fn schema_unknown_table_exits_with_error() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (_, stderr, ok) = run_cmd(&["schema", "gl_typo"], data_dir.path());
    assert!(!ok, "schema should fail for unknown table");
    assert!(
        stderr.contains("no table named 'gl_typo'"),
        "expected unknown-table error: {stderr}"
    );
    assert!(
        stderr.contains("Run `orbit schema` to list tables"),
        "expected suggestion to run orbit schema: {stderr}"
    );
}

// ── MCP server ──────────────────────────────────────────────────

#[test]
fn mcp_tools_mirror_cli_surface() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let resps = mcp_roundtrip(
        data_dir.path(),
        &[serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})],
    );

    let tools = resps[0]["result"]["tools"].as_array().unwrap();
    let names: BTreeSet<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        BTreeSet::from(["get_graph_schema", "index", "run_sql"])
    );
    for tool in tools {
        assert!(!tool["description"].as_str().unwrap().is_empty());
    }
}

#[test]
fn mcp_index_then_query() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();

    let resps = mcp_roundtrip(
        data_dir.path(),
        &[
            mcp_tool_call(1, "run_sql", json!({"sql": ["SELECT 1"]})),
            mcp_tool_call(2, "index", json!({"path": repo.path})),
            mcp_tool_call(3, "get_graph_schema", json!({})),
            mcp_tool_call(
                4,
                "run_sql",
                json!({"sql": [
                    "SELECT name FROM gl_file WHERE name IS NOT NULL ORDER BY name",
                    "SELECT COUNT(*) AS n FROM gl_definition",
                ]}),
            ),
        ],
    );

    assert_eq!(resps[0]["result"]["isError"], true);
    assert!(mcp_tool_text(&resps[0]).contains("no local graph found"));

    assert_eq!(resps[1]["result"]["isError"], false);
    let indexed: Value = serde_json::from_str(mcp_tool_text(&resps[1])).unwrap();
    assert_eq!(indexed.as_array().unwrap().len(), 1);

    let schema: Value = serde_json::from_str(mcp_tool_text(&resps[2])).unwrap();
    assert!(rows_where(&schema, "table_name", "gl_file").len() > 1);

    let results: Value = serde_json::from_str(mcp_tool_text(&resps[3])).unwrap();
    let names: Vec<&str> = rows(&results[0])
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["main.py", "utils.py"]);
    assert!(results[1][0]["n"].as_i64().unwrap() > 0);
}

#[test]
fn mcp_bad_sql_is_recoverable_tool_error() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let resps = mcp_roundtrip(
        data_dir.path(),
        &[mcp_tool_call(
            1,
            "run_sql",
            json!({"sql": ["SELECT 1", "SELECT nope FROM does_not_exist"]}),
        )],
    );

    assert_eq!(resps[0]["result"]["isError"], true);
    let msg = mcp_tool_text(&resps[0]);
    assert!(
        msg.contains("statement 1"),
        "missing statement index: {msg}"
    );
    assert!(msg.contains("does_not_exist"), "missing SQL preview: {msg}");
}
