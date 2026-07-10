use std::collections::BTreeSet;

use integration_testkit::cli::{
    create_test_repo, git, init_repo_at, mcp_roundtrip, mcp_tool_call, mcp_tool_text, orbit_cmd,
    orbit_index, orbit_sql, rows, rows_where, sorted_ids,
};
use serde_json::{Value, json};

const FILES_FULL: &str = "SELECT id, name, path, branch, commit_sha FROM gl_file WHERE name IS NOT NULL ORDER BY path LIMIT 50";
const FILES_SIMPLE: &str =
    "SELECT id, name, path FROM gl_file WHERE name IS NOT NULL ORDER BY path LIMIT 50";

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
fn index_db_flag_writes_to_custom_path() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("custom.duckdb");
    let repo = create_test_repo();

    let workspace_dir = tempfile::TempDir::new().unwrap();
    let out = orbit_cmd()
        .args([
            "index",
            repo.path.to_str().unwrap(),
            "--db",
            db_path.to_str().unwrap(),
        ])
        .env("ORBIT_DATA_DIR", workspace_dir.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "index --db failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(db_path.exists(), "custom db file should exist");

    let result = orbit_cmd()
        .args([
            "sql",
            "-F",
            "json",
            "--db",
            db_path.to_str().unwrap(),
            FILES_SIMPLE,
        ])
        .output()
        .unwrap();
    assert!(result.status.success());
    let json: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(rows(&json).len(), 2);
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

#[test]
fn skill_serves_bundled_content() {
    let manifest = orbit_cmd().arg("skill").output().unwrap();
    assert!(manifest.status.success());
    let manifest = String::from_utf8(manifest.stdout).unwrap();
    assert!(manifest.contains("name: orbit-local"));
    assert!(manifest.contains("references/sql.md"));
    assert!(
        manifest.contains("orbit skill references/sql.md"),
        "served manifest must tell binary users the version-matched access path"
    );

    let sql_ref = orbit_cmd()
        .args(["skill", "references/sql.md"])
        .output()
        .unwrap()
        .stdout;
    assert!(
        !String::from_utf8(sql_ref)
            .unwrap()
            .contains("orbit skill <path>"),
        "the discovery hint must be manifest-only, not appended to subfiles"
    );

    for path in ["SKILL.md", "references/sql.md", "references/repo_map.md"] {
        let out = orbit_cmd().args(["skill", path]).output().unwrap();
        assert!(out.status.success(), "`orbit skill {path}` failed");
        assert!(
            !out.stdout.is_empty(),
            "`orbit skill {path}` printed nothing"
        );
    }

    let no_arg = orbit_cmd().arg("skill").output().unwrap().stdout;
    let explicit = orbit_cmd()
        .args(["skill", "SKILL.md"])
        .output()
        .unwrap()
        .stdout;
    assert_eq!(no_arg, explicit, "no-arg must equal `skill SKILL.md`");

    let repo_map_ref = orbit_cmd()
        .args(["skill", "references/repo_map.md"])
        .output()
        .unwrap()
        .stdout;
    assert!(
        String::from_utf8(repo_map_ref)
            .unwrap()
            .contains("orbit repo-map")
    );
}

#[test]
fn skill_rejects_unknown_and_escaping_paths() {
    for path in [
        "references/does-not-exist.md",
        "../Cargo.toml",
        "/etc/passwd",
        "references/../../secret",
    ] {
        let out = orbit_cmd().args(["skill", path]).output().unwrap();
        assert!(
            !out.status.success(),
            "`orbit skill {path}` must exit non-zero"
        );
        assert!(out.stdout.is_empty(), "`orbit skill {path}` leaked stdout");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(
            err.contains("Available files") && err.contains("SKILL.md"),
            "error must list valid paths, got: {err}"
        );
    }
}

fn repo_map(
    repo: &std::path::Path,
    data_dir: &std::path::Path,
    args: &[&str],
) -> std::process::Output {
    let mut cmd = orbit_cmd();
    cmd.arg("repo-map").args(["--repo", repo.to_str().unwrap()]);
    cmd.args(args).env("ORBIT_DATA_DIR", data_dir);
    cmd.output().unwrap()
}

#[test]
fn repo_map_serves_native_subcommands() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    let overview = repo_map(&repo.path, dd, &["overview"]);
    assert!(overview.status.success());
    let text = String::from_utf8(overview.stdout).unwrap();
    assert!(text.contains("REPO MAP"));
    assert!(text.contains("Languages") && text.contains("python"));

    let tree = String::from_utf8(repo_map(&repo.path, dd, &["tree", "src"]).stdout).unwrap();
    assert!(tree.contains("TREE") && tree.contains("App"));

    // `api` extracts a signature line via read_text, so it exercises the
    // relative-glob resolution against the repo-rooted CWD.
    let api = String::from_utf8(repo_map(&repo.path, dd, &["api", "src"]).stdout).unwrap();
    assert!(api.contains("class App") && api.contains("def run"));

    let class = String::from_utf8(repo_map(&repo.path, dd, &["class", "App"]).stdout).unwrap();
    assert!(class.contains("CLASS — App") && class.contains("run"));

    let missing =
        String::from_utf8(repo_map(&repo.path, dd, &["class", "NoSuchThing"]).stdout).unwrap();
    assert!(missing.contains("no class/module/trait named NoSuchThing"));
}

#[test]
fn repo_map_ext_filter_scopes_languages() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    let rust_only =
        String::from_utf8(repo_map(&repo.path, dd, &["--ext", "rs", "overview"]).stdout).unwrap();
    assert!(rust_only.contains("REPO MAP"));
    assert!(
        !rust_only.contains("python"),
        "rs filter must drop python files"
    );
}

#[test]
fn repo_map_extends_and_imports_render_rows() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    let extends = String::from_utf8(repo_map(&repo.path, dd, &["extends", "Base"]).stdout).unwrap();
    assert!(extends.contains("DESCENDANTS — Base"));
    assert!(
        extends.contains("Base") && extends.contains("App"),
        "EXTENDS chain must list the base and its descendant: {extends}"
    );

    let imports =
        String::from_utf8(repo_map(&repo.path, dd, &["imports", "read_file"]).stdout).unwrap();
    assert!(imports.contains("IMPORTERS — pattern 'read_file'"));
    assert!(
        imports.contains("read_file") && imports.contains("src.utils"),
        "IMPORTERS must resolve the imported symbol and its source path: {imports}"
    );
}

#[test]
fn repo_map_subdir_repo_resolves_from_root() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    // Pointing `--repo` at a subdirectory must still anchor at the git
    // top-level so signature extraction (read_text against repo-root-relative
    // paths) works, not silently degrade to bare names.
    let subdir = repo.path.join("src");
    let api = String::from_utf8(repo_map(&subdir, dd, &["api", "src"]).stdout).unwrap();
    assert!(
        api.contains("class App(Base):") && api.contains("def run(self):"),
        "subdir --repo must extract signatures, not bare names: {api}"
    );

    let class = String::from_utf8(repo_map(&subdir, dd, &["class", "App"]).stdout).unwrap();
    assert!(class.contains("CLASS — App") && class.contains("def run(self):"));
}

#[test]
fn repo_map_relative_db_resolves_from_caller_cwd() {
    let repo = create_test_repo();
    let caller = tempfile::TempDir::new().unwrap();

    let index = orbit_cmd()
        .args(["index", repo.path.to_str().unwrap(), "--db", "graph.duckdb"])
        .current_dir(caller.path())
        .output()
        .unwrap();
    assert!(
        index.status.success(),
        "index --db graph.duckdb failed: {}",
        String::from_utf8_lossy(&index.stderr)
    );
    assert!(caller.path().join("graph.duckdb").exists());

    let out = orbit_cmd()
        .args([
            "repo-map",
            "--repo",
            repo.path.to_str().unwrap(),
            "--db",
            "graph.duckdb",
            "overview",
        ])
        .current_dir(caller.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "relative --db must resolve against caller cwd before the repo-root chdir: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8(out.stdout).unwrap().contains("REPO MAP"));
}

#[test]
fn repo_map_reports_unindexed_commit() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    std::fs::write(repo.path.join("src/extra.py"), "def added(): pass\n").unwrap();
    git(&repo.path, &["add", "-A"]);
    git(
        &repo.path,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-m",
            "second",
        ],
    );

    let out = repo_map(&repo.path, dd, &["overview"]);
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("is not indexed") && err.contains("orbit index"));
}

/// Two clones at the same commit SHA indexed into one DB must be completely
/// isolated: each repo-map must see only its own project's data. Without
/// `project_id` scoping, the queries would combine rows from both.
#[test]
fn repo_map_same_sha_isolates_projects() {
    let workspace = tempfile::TempDir::new().unwrap();
    let data_dir = tempfile::TempDir::new().unwrap();
    let dd = data_dir.path();

    let repo_a = workspace.path().join("alpha");
    init_repo_at(
        &repo_a,
        &[
            (
                "src/main.py",
                "class Alpha:\n    def run(self):\n        pass\n",
            ),
            ("src/helper.py", "def alpha_helper():\n    pass\n"),
        ],
    );
    let sha_a = git(&repo_a, &["rev-parse", "HEAD"]);

    let repo_b = workspace.path().join("beta");
    std::process::Command::new("git")
        .args([
            "clone",
            "--local",
            repo_a.to_str().unwrap(),
            repo_b.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let sha_b = git(&repo_b, &["rev-parse", "HEAD"]);
    assert_eq!(sha_a, sha_b, "clones must share the same commit SHA");

    assert!(orbit_index(&repo_a, dd));
    assert!(orbit_index(&repo_b, dd));

    let files = orbit_sql("SELECT project_id, path FROM gl_file ORDER BY path", dd);
    let file_rows = rows(&files);
    let project_ids: BTreeSet<i64> = file_rows
        .iter()
        .filter_map(|r| r["project_id"].as_i64())
        .collect();
    assert_eq!(
        project_ids.len(),
        2,
        "two distinct project_ids must exist: {file_rows:?}"
    );

    // Without project_id scoping, the overview file count would double (4
    // instead of 2) because both projects share the same SHA.
    let count_files = |repo: &std::path::Path, label: &str| -> usize {
        let text = String::from_utf8(repo_map(repo, dd, &["overview"]).stdout).unwrap();
        let files_line = text
            .lines()
            .find(|l| l.contains("python"))
            .unwrap_or_else(|| panic!("{label} overview must contain a python row:\n{text}"));
        files_line
            .split('|')
            .filter_map(|cell| cell.trim().parse().ok())
            .next()
            .unwrap_or_else(|| panic!("{label} python row has no count:\n{files_line}"))
    };
    assert_eq!(count_files(&repo_a, "alpha"), 2);
    assert_eq!(count_files(&repo_b, "beta"), 2);

    let tree_a = String::from_utf8(repo_map(&repo_a, dd, &["tree", "src"]).stdout).unwrap();
    let tree_b = String::from_utf8(repo_map(&repo_b, dd, &["tree", "src"]).stdout).unwrap();
    assert!(tree_a.contains("Alpha"), "alpha tree must contain Alpha");
    assert!(tree_b.contains("Alpha"), "beta tree must contain Alpha");

    let extends_a = String::from_utf8(repo_map(&repo_a, dd, &["extends", "Alpha"]).stdout).unwrap();
    let extends_b = String::from_utf8(repo_map(&repo_b, dd, &["extends", "Alpha"]).stdout).unwrap();
    assert!(
        extends_a.contains("Alpha"),
        "alpha extends must show Alpha: {extends_a}"
    );
    assert!(
        extends_b.contains("Alpha"),
        "beta extends must show Alpha: {extends_b}"
    );

    let imports_a =
        String::from_utf8(repo_map(&repo_a, dd, &["imports", "alpha_helper"]).stdout).unwrap();
    let imports_b =
        String::from_utf8(repo_map(&repo_b, dd, &["imports", "alpha_helper"]).stdout).unwrap();
    assert!(
        imports_a.contains("IMPORTERS"),
        "alpha imports header must appear: {imports_a}"
    );
    assert!(
        imports_b.contains("IMPORTERS"),
        "beta imports header must appear: {imports_b}"
    );
}

/// Preflight must reject a checkout whose project_id is not indexed, even
/// when a different project at the same SHA exists in the DB.
#[test]
fn repo_map_preflight_rejects_unindexed_project_at_same_sha() {
    let workspace = tempfile::TempDir::new().unwrap();
    let data_dir = tempfile::TempDir::new().unwrap();
    let dd = data_dir.path();

    let repo_a = workspace.path().join("indexed");
    init_repo_at(&repo_a, &[("lib.py", "x = 1\n")]);

    let repo_b = workspace.path().join("not-indexed");
    std::process::Command::new("git")
        .args([
            "clone",
            "--local",
            repo_a.to_str().unwrap(),
            repo_b.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(
        git(&repo_a, &["rev-parse", "HEAD"]),
        git(&repo_b, &["rev-parse", "HEAD"]),
    );

    assert!(orbit_index(&repo_a, dd));

    let out = repo_map(&repo_b, dd, &["overview"]);
    assert!(
        !out.status.success(),
        "preflight must fail for the unindexed clone even though the SHA exists"
    );
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("is not indexed"),
        "error must mention unindexed commit: {err}"
    );
}

/// `ORBIT_DATA_DIR` set to a relative path must resolve against the caller's
/// CWD, not the repo root that `repo-map` internally changes to.
#[test]
fn repo_map_relative_orbit_data_dir() {
    let repo = create_test_repo();
    let caller_dir = tempfile::TempDir::new().unwrap();

    let idx = orbit_cmd()
        .args(["index", repo.path.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", "orbit-data")
        .current_dir(caller_dir.path())
        .output()
        .unwrap();
    assert!(
        idx.status.success(),
        "index with relative ORBIT_DATA_DIR failed: {}",
        String::from_utf8_lossy(&idx.stderr)
    );
    assert!(caller_dir.path().join("orbit-data/graph.duckdb").exists());

    let out = orbit_cmd()
        .args([
            "repo-map",
            "--repo",
            repo.path.to_str().unwrap(),
            "overview",
        ])
        .env("ORBIT_DATA_DIR", "orbit-data")
        .current_dir(caller_dir.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "relative ORBIT_DATA_DIR must resolve before the repo-root chdir: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.contains("REPO MAP") && text.contains("Languages"),
        "overview output must contain expected sections: {text}"
    );
}

/// All six subcommands must emit their section header and a separator line,
/// and non-empty subcommands must produce at least one tabular row.
#[test]
fn repo_map_output_shape() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    let overview = String::from_utf8(repo_map(&repo.path, dd, &["overview"]).stdout).unwrap();
    for section in [
        "Languages",
        "Top-level structure",
        "Key abstractions",
        "Most-imported defined symbols",
        "Most-called callables",
    ] {
        assert!(
            overview.contains(section),
            "overview missing section '{section}': {overview}"
        );
    }
    assert!(
        overview.contains("=="),
        "overview missing separator: {overview}"
    );
    assert!(
        overview.lines().any(|l| l.contains("python")),
        "overview must contain at least one data row with 'python': {overview}"
    );

    let tree = String::from_utf8(repo_map(&repo.path, dd, &["tree", "src"]).stdout).unwrap();
    assert!(tree.starts_with("TREE"), "tree must start with header");
    assert!(
        tree.lines().any(|l| l.contains("src/")),
        "tree must list files under src/: {tree}"
    );

    let api = String::from_utf8(repo_map(&repo.path, dd, &["api", "src"]).stdout).unwrap();
    assert!(api.starts_with("API MAP"), "api must start with header");
    assert!(
        api.lines().any(|l| l.contains("[L")),
        "api must contain at least one line-number reference: {api}"
    );

    let class = String::from_utf8(repo_map(&repo.path, dd, &["class", "App"]).stdout).unwrap();
    assert!(class.starts_with("CLASS"), "class must start with header");
    assert!(
        class.contains("Members + signatures"),
        "class must have members section: {class}"
    );

    let extends = String::from_utf8(repo_map(&repo.path, dd, &["extends", "Base"]).stdout).unwrap();
    assert!(
        extends.starts_with("DESCENDANTS"),
        "extends must start with header"
    );
    let depth_rows: Vec<&str> = extends
        .lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| depth"))
        .collect();
    assert!(
        depth_rows.len() >= 2,
        "extends must have at least 2 data rows (Base + App): {extends}"
    );

    let imports =
        String::from_utf8(repo_map(&repo.path, dd, &["imports", "read_file"]).stdout).unwrap();
    assert!(
        imports.starts_with("IMPORTERS"),
        "imports must start with header"
    );
    assert!(
        imports.lines().any(|l| l.contains("read_file")),
        "imports must list the matched symbol: {imports}"
    );
}
