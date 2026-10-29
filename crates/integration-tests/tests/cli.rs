use std::collections::BTreeSet;

#[path = "cli/background_index.rs"]
mod background_index;
#[path = "cli/revisions.rs"]
mod revisions;

use integration_testkit::cli::{
    create_test_repo, git, init_repo_at, orbit_cmd, orbit_index, orbit_sql, rows, rows_where,
    sorted_ids,
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
                .args(["workspace", "sql", "-F", "json", FILES_SIMPLE])
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
fn search_hook_recognizes_the_separate_manifest() {
    use std::io::Write;

    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let invoke = || {
        let mut child = orbit_cmd()
            .args(["hook-guard", "search"])
            .env("ORBIT_DATA_DIR", data_dir.path())
            .env("ORBIT_TELEMETRY_DISABLED", "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        writeln!(
            child.stdin.take().unwrap(),
            "{}",
            json!({
                "cwd": repo.path,
                "tool_name": "Grep",
                "tool_input": {"pattern": "hello"}
            })
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        output.stdout
    };
    assert!(invoke().is_empty());
    assert!(orbit_index(&repo.path, data_dir.path()));
    assert!(!data_dir.path().join("graph.duckdb").exists());
    let output: Value = serde_json::from_slice(&invoke()).unwrap();
    assert_eq!(output["hookSpecificOutput"]["hookEventName"], "PreToolUse");
    assert!(
        output["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap()
            .contains("orbit")
    );
}

#[test]
fn reader_during_writer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let mut child = orbit_cmd()
        .args(["workspace", "index", repo.path.to_str().unwrap()])
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
                .args(["workspace", "index", repo.path.to_str().unwrap()])
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
    assert_eq!(
        ok, 2,
        "both writers should complete under the revision lock"
    );

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
fn incremental_index_follows_the_working_tree() {
    let data = tempfile::TempDir::new().unwrap();
    let repo_dir = tempfile::TempDir::new().unwrap();
    let repo = repo_dir.path();
    let write = |path: &str, text: &str| std::fs::write(repo.join(path), text).unwrap();
    init_repo_at(
        repo,
        &[
            ("Cargo.toml", "[package]\nname = \"demo\"\n"),
            ("lib/greeter.rb", "class Greeter\nend\n"),
            ("src/lib.rs", "pub fn helper() {}\n"),
        ],
    );
    let names = |sql: &str| -> Vec<String> {
        assert!(orbit_index(repo, data.path()));
        let path = repo.to_str().unwrap();
        let mut sql_cmd = orbit_cmd();
        sql_cmd.args(["workspace", "sql", "-F", "json", "--repo", path, sql]);
        let out = sql_cmd.env("ORBIT_DATA_DIR", data.path()).output().unwrap();
        let found: Value = serde_json::from_slice(&out.stdout).unwrap();
        rows(&found)
            .iter()
            .map(|r| r["n"].as_str().unwrap().to_string())
            .collect()
    };
    let definitions = "SELECT fqn AS n FROM gl_definition ORDER BY fqn";
    assert_eq!(names(definitions), ["Greeter", "helper"]);
    write("src/lib.rs", "pub fn helper() {}\npub fn draft() {}\n");
    assert_eq!(names(definitions), ["Greeter", "draft", "helper"]);
    git(repo, &["mv", "src/lib.rs", "src/main.rs"]);
    std::fs::create_dir_all(repo.join("py")).unwrap();
    write("py/mod.py", "def py_fn():\n    pass\n");
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-qm", "rename, add python"]);
    assert_eq!(
        names(definitions),
        ["Greeter", "draft", "helper", "py.mod.py_fn"]
    );
    let files = "SELECT path AS n FROM gl_file ORDER BY path";
    assert_eq!(
        names(files),
        ["Cargo.toml", "lib/greeter.rb", "py/mod.py", "src/main.rs"]
    );
    std::fs::remove_file(repo.join("Cargo.toml")).unwrap();
    assert_eq!(names(files), ["lib/greeter.rb", "py/mod.py", "src/main.rs"]);
}

const MIXED_REPO: &[(&str, &str)] = &[
    (
        "lib/settings.rb",
        "class Settings\n  def self.load\n  end\nend\n",
    ),
    (
        "config/application.rb",
        "require \"rails\"\nrequire_relative \"../lib/settings\"\n\nmodule Demo\n  class Application\n    def boot\n      config.eager_load = true\n      Settings.load\n    end\n  end\nend\n",
    ),
    ("near.py", "def thing():\n    pass\n"),
    ("far.py", "def thing():\n    pass\n"),
    ("middle.py", "from far import *\n"),
    ("hub.py", "from near import *\nfrom middle import *\n"),
    ("app.py", "from hub import *\n\ndef run():\n    thing()\n"),
    ("README.md", "# demo\n"),
];

fn write_file(repo: &std::path::Path, path: &str, text: &str) {
    let path = repo.join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

/// Every row of every graph table, in a stable order.
fn graph_tables(data: &std::path::Path) -> Vec<Value> {
    [
        "gl_directory",
        "gl_file",
        "gl_definition",
        "gl_import",
        "gl_edge",
    ]
    .iter()
    .map(|table| orbit_sql(&format!("SELECT * FROM {table} ORDER BY ALL"), data))
    .collect()
}

fn full_index_tables(repo: &std::path::Path) -> Vec<Value> {
    let full = tempfile::TempDir::new().unwrap();
    assert!(orbit_index(repo, full.path()));
    graph_tables(full.path())
}

#[test]
fn an_incremental_index_matches_a_full_index_of_the_same_tree() {
    let incremental = tempfile::TempDir::new().unwrap();
    let repo_dir = tempfile::TempDir::new().unwrap();
    let repo = repo_dir.path();
    init_repo_at(repo, MIXED_REPO);
    let matches_a_full_index = |step: &str| {
        assert!(orbit_index(repo, incremental.path()));
        assert_eq!(
            graph_tables(incremental.path()),
            full_index_tables(repo),
            "{step}"
        );
    };

    matches_a_full_index("first index");
    write_file(
        repo,
        "lib/settings.rb",
        "class Settings\n  def self.load\n  end\n\n  def self.reload\n  end\nend\n",
    );
    matches_a_full_index("edit a required file");
    write_file(
        repo,
        "lib/tools/runner.rb",
        "class Runner\n  def go\n    Settings.reload\n  end\nend\n",
    );
    matches_a_full_index("add a file in a new directory");
    git(repo, &["mv", "near.py", "close.py"]);
    write_file(
        repo,
        "hub.py",
        "from close import *\nfrom middle import *\n",
    );
    matches_a_full_index("rename a file");
    std::fs::remove_file(repo.join("far.py")).unwrap();
    matches_a_full_index("remove a file");
    std::fs::remove_dir_all(repo.join("lib/tools")).unwrap();
    matches_a_full_index("remove a directory");
}

#[test]
fn a_refresh_matches_a_full_index_when_files_come_and_go() {
    let data = tempfile::TempDir::new().unwrap();
    let repo_dir = tempfile::TempDir::new().unwrap();
    let repo = repo_dir.path();
    init_repo_at(repo, MIXED_REPO);
    assert!(orbit_index(repo, data.path()));
    std::thread::sleep(std::time::Duration::from_millis(1100));

    write_file(repo, "pkg/sub/deep.py", "def deep():\n    run()\n");
    git(repo, &["mv", "near.py", "close.py"]);
    write_file(
        repo,
        "hub.py",
        "from close import *\nfrom middle import *\n",
    );
    std::fs::remove_file(repo.join("far.py")).unwrap();
    let refreshed = orbit_cmd()
        .args(["refresh", repo.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data.path())
        .output()
        .unwrap();

    assert!(
        refreshed.status.success(),
        "{}",
        String::from_utf8_lossy(&refreshed.stderr)
    );
    assert_eq!(graph_tables(data.path()), full_index_tables(repo));
}

#[test]
fn index_db_flag_writes_to_custom_path() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("custom.duckdb");
    let repo = create_test_repo();

    let workspace_dir = tempfile::TempDir::new().unwrap();
    let index = || {
        orbit_cmd()
            .args([
                "workspace",
                "index",
                repo.path.to_str().unwrap(),
                "--db",
                db_path.to_str().unwrap(),
            ])
            .env("ORBIT_DATA_DIR", workspace_dir.path())
            .output()
            .unwrap()
    };
    let out = index();
    assert!(
        out.status.success(),
        "index --db failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(db_path.exists(), "custom db file should exist");

    let result = orbit_cmd()
        .args([
            "workspace",
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

    git(
        &repo.path,
        &["commit", "--allow-empty", "-m", "new revision"],
    );
    let output = index();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("belongs to another checkout revision")
    );
    let client = duckdb_client::DuckDbClient::open(&db_path).unwrap();
    client
        .execute(
            "UPDATE _orbit_meta SET value = 'outdated' WHERE key = 'local_ddl'",
            &[],
        )
        .unwrap();
    drop(client);
    let before = std::fs::read(&db_path).unwrap();
    let output = index();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("belongs to another checkout revision")
    );
    assert_eq!(
        std::fs::read(&db_path).unwrap(),
        before,
        "ownership rejection must preserve an outdated database"
    );
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
fn unindexable_repo_records_error_in_manifest() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = tempfile::TempDir::new().unwrap();
    // A commitless repo is discovered (.git exists) but git_info fails on HEAD.
    git(repo.path(), &["init"]);

    let ok = orbit_cmd()
        .args(["workspace", "index", repo.path().to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .output()
        .unwrap()
        .status
        .success();
    assert!(!ok, "indexing a commitless repo should report failure");

    let manifest = orbit_sql(
        "SELECT CAST(status AS VARCHAR) AS status, error_message FROM _orbit_manifest",
        data_dir.path(),
    );
    let rows = rows(&manifest);
    assert_eq!(rows.len(), 1, "expected one error row, got {rows:?}");
    assert_eq!(rows[0]["status"], "error");
    assert!(
        rows[0]["error_message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "error row must carry a reason: {:?}",
        rows[0]
    );
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

#[test]
fn index_skips_nested_repositories_the_parent_ignores() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();

    let parent = workspace.path().join("parent");
    init_repo_at(
        &parent,
        &[
            (".gitignore", "vendor/\n"),
            ("src/app.py", "def app(): pass\n"),
        ],
    );
    init_repo_at(
        &parent.join("vendor/dependency"),
        &[("dep.py", "def dep(): pass\n")],
    );
    init_repo_at(
        &parent.join("libs/utils"),
        &[("helper.py", "def helper(): pass\n")],
    );

    assert!(orbit_index(&parent, data_dir.path()));

    let indexed = indexed_repo_paths(data_dir.path());
    assert!(
        indexed.iter().any(|path| path.ends_with("/parent")),
        "{indexed:?}"
    );
    assert!(
        indexed.iter().any(|path| path.ends_with("/libs/utils")),
        "{indexed:?}"
    );
    assert!(
        !indexed.iter().any(|path| path.contains("/vendor/")),
        "{indexed:?}"
    );
}

#[test]
fn index_defaults_to_the_current_directory() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();
    let repo = workspace.path().join("repo");
    init_repo_at(&repo, &[("main.py", "def main(): pass\n")]);

    let indexed = orbit_cmd()
        .args(["workspace", "index"])
        .current_dir(&repo)
        .env("ORBIT_DATA_DIR", data_dir.path())
        .status()
        .unwrap();

    assert!(indexed.success());
    let indexed = indexed_repo_paths(data_dir.path());
    assert!(
        indexed.iter().any(|path| path.ends_with("/repo")),
        "{indexed:?}"
    );
}

#[test]
fn first_grep_on_a_new_machine_indexes_the_repository() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();
    let repo = workspace.path().join("repo");
    init_repo_at(&repo, &[("greeter.py", "def greet_visitor(): pass\n")]);

    let out = orbit_cmd()
        .args(["grep", "greet_visitor"])
        .current_dir(&repo)
        .env("ORBIT_DATA_DIR", data_dir.path())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("greeter.py"), "{stdout}");
}

#[test]
fn a_busy_graph_is_reported_in_one_line() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let manifest = orbit_sql("SELECT database_path FROM _orbit_manifest", data_dir.path());
    let graph = std::path::Path::new(manifest[0]["database_path"].as_str().unwrap());
    let _writer = duckdb_client::DuckDbClient::open(graph).unwrap();
    let (_, stderr, ok) = run_cmd(&["workspace", "sql", "--all", "SELECT 1"], data_dir.path());

    assert!(!ok);
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    assert!(stderr.starts_with("The local graph is busy:"), "{stderr}");
    assert!(
        stderr.contains(&format!("(PID {})", std::process::id())),
        "{stderr}"
    );
}

#[test]
fn reset_removes_the_graph_and_keeps_settings() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));
    std::fs::write(data_dir.path().join("graph.duckdb"), b"earlier release").unwrap();
    let (_, stderr, ok) = run_cmd(
        &["config", "set", "telemetry.enabled", "false"],
        data_dir.path(),
    );
    assert!(ok, "{stderr}");

    let (_, stderr, ok) = run_cmd(&["workspace", "reset", "--yes"], data_dir.path());

    assert!(ok, "{stderr}");
    for removed in ["local", "var", "graph.duckdb"] {
        assert!(!data_dir.path().join(removed).exists(), "{removed} remains");
    }
    assert!(data_dir.path().join("settings.json").is_file());
    assert!(orbit_index(&repo.path, data_dir.path()));
    assert_eq!(indexed_repo_paths(data_dir.path()).len(), 1);
}

#[test]
fn reset_dry_run_lists_the_graph_and_removes_nothing() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (_, stderr, ok) = run_cmd(&["workspace", "reset", "--dry-run"], data_dir.path());

    assert!(ok, "{stderr}");
    assert!(stderr.contains("local"), "{stderr}");
    assert_eq!(indexed_repo_paths(data_dir.path()).len(), 1);
}

#[test]
fn reset_waits_for_open_readers_and_gives_up_without_removing() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));
    let manifest = orbit_sql("SELECT database_path FROM _orbit_manifest", data_dir.path());
    let reader = std::fs::File::open(format!(
        "{}.lock",
        manifest[0]["database_path"].as_str().unwrap()
    ))
    .unwrap();
    reader.lock_shared().unwrap();

    let out = orbit_cmd()
        .args(["workspace", "reset", "--yes"])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .env("ORBIT_WAIT_TIMEOUT_MS", "200")
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(75), "{stderr}");
    assert!(stderr.contains("gave up waiting"), "{stderr}");
    assert_eq!(indexed_repo_paths(data_dir.path()).len(), 1);
}

fn indexed_repo_paths(data_dir: &std::path::Path) -> Vec<String> {
    let (stdout, stderr, ok) = run_cmd(&["workspace", "list", "-F", "json"], data_dir);
    assert!(ok, "orbit workspace list failed: {stderr}");
    let listed: Vec<Value> = serde_json::from_str(&stdout).unwrap();
    listed
        .iter()
        .map(|repo| repo["repo_path"].as_str().unwrap().to_string())
        .collect()
}

fn run_cmd(args: &[&str], data_dir: &std::path::Path) -> (String, String, bool) {
    let out = orbit_cmd()
        .args(args)
        .current_dir(data_dir)
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

    let (stdout, stderr, ok) = run_cmd(&["workspace", "schema"], data_dir.path());
    assert!(ok, "orbit schema failed: {stderr}");

    for want in [
        "gl_directory",
        "gl_file",
        "gl_definition",
        "gl_import",
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

    let (stdout, stderr, ok) = run_cmd(&["workspace", "schema", "--raw"], data_dir.path());
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
    let (_, stderr, ok) = run_cmd(&["workspace", "schema"], data_dir.path());
    assert!(!ok, "schema should fail without an indexed graph");
    assert!(
        stderr.contains("no local graph found"),
        "expected missing-graph error: {stderr}"
    );
}

#[test]
fn index_errors_when_path_has_no_git_repository() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let not_a_repo = tempfile::TempDir::new().unwrap();
    std::fs::write(
        not_a_repo.path().join("a.go"),
        "package main\nfunc Alpha() {}\n",
    )
    .unwrap();

    let (_, stderr, ok) = run_cmd(
        &["workspace", "index", not_a_repo.path().to_str().unwrap()],
        data_dir.path(),
    );
    assert!(!ok, "index should fail when the path has no git repository");
    assert!(
        stderr.contains("no git repository found"),
        "expected no-repository error: {stderr}"
    );
}

#[test]
fn index_errors_when_repository_has_no_commits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let empty_repo = tempfile::TempDir::new().unwrap();
    git(empty_repo.path(), &["init", "--quiet"]);

    for mode in [None, Some("--async")] {
        let mut args = vec!["workspace", "index"];
        args.extend(mode);
        args.push(empty_repo.path().to_str().unwrap());
        let (_, stderr, ok) = run_cmd(&args, data_dir.path());
        assert!(!ok, "index should fail when the repository has no commits");
        assert!(
            stderr.contains("has no commits yet"),
            "expected no-commits error: {stderr}"
        );
        assert!(
            !stderr.contains("ambiguous argument"),
            "raw git error should not leak: {stderr}"
        );
    }
}

#[test]
fn index_does_not_report_an_unreadable_repository_as_empty() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let broken = tempfile::TempDir::new().unwrap();
    git(broken.path(), &["init", "--quiet"]);
    git(
        broken.path(),
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "x",
        ],
    );
    std::fs::remove_dir_all(broken.path().join(".git").join("objects")).unwrap();

    let (_, stderr, ok) = run_cmd(
        &["workspace", "index", broken.path().to_str().unwrap()],
        data_dir.path(),
    );
    assert!(!ok, "index should fail on a repository git cannot read");
    assert!(
        !stderr.contains("has no commits yet"),
        "an unreadable repository must not be reported as empty: {stderr}"
    );
}

#[test]
fn schema_scoped_excludes_unrequested_tables() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (stdout, stderr, ok) = run_cmd(&["workspace", "schema", "gl_file"], data_dir.path());
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

    let (stdout, stderr, ok) = run_cmd(
        &["workspace", "schema", "--raw", "gl_definition"],
        data_dir.path(),
    );
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

    let (_, stderr, ok) = run_cmd(&["workspace", "schema", "gl_typo"], data_dir.path());
    assert!(!ok, "schema should fail for unknown table");
    assert!(
        stderr.contains("no table named 'gl_typo'"),
        "expected unknown-table error: {stderr}"
    );
    assert!(
        stderr.contains("Run `orbit workspace schema` to list tables"),
        "expected suggestion to run orbit workspace schema: {stderr}"
    );
}

#[test]
fn workspace_sql_indexes_the_current_repository_on_first_use() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();

    let out = orbit_cmd()
        .args([
            "workspace",
            "sql",
            "-F",
            "json",
            "SELECT COUNT(*) AS n FROM gl_file",
        ])
        .current_dir(&repo.path)
        .env("ORBIT_DATA_DIR", data_dir.path())
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let counts: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(counts, json!([{"n": 2}]));
}

/// `orbit` with no credentials and no glab on PATH, so skills commands never
/// reach a real login or instance.
fn hermetic_skills_cmd(cache: &std::path::Path) -> std::process::Command {
    let mut cmd = orbit_cmd();
    for key in [
        "ORBIT_API_BASE_URL",
        "ORBIT_AUTH_HEADER_NAME",
        "ORBIT_AUTH_HEADER_VALUE",
        "GITLAB_TOKEN",
        "GITLAB_URL",
    ] {
        cmd.env_remove(key);
    }
    cmd.env("PATH", "/nonexistent-orbit-test-path")
        .env("XDG_CACHE_HOME", cache);
    cmd
}

#[test]
fn skills_lists_and_serves_local_content_without_remote_environment() {
    let cache = tempfile::TempDir::new().unwrap();
    let listing = hermetic_skills_cmd(cache.path())
        .arg("skills")
        .output()
        .unwrap();
    assert!(listing.status.success());
    assert_eq!(
        String::from_utf8_lossy(&listing.stderr).trim(),
        "warning: no GitLab credential found; using the embedded local skill"
    );
    let listing = String::from_utf8(listing.stdout).unwrap();
    assert!(listing.starts_with("orbit — "));
    assert!(listing.contains("Orbit CLI"));

    let manifest = hermetic_skills_cmd(cache.path())
        .args(["skills", "get", "orbit"])
        .output()
        .unwrap();
    assert!(manifest.status.success());
    let manifest = String::from_utf8(manifest.stdout).unwrap();
    assert!(manifest.contains("name: orbit-cli"));
    assert!(manifest.contains("references/local/cli.md"));
    assert!(
        manifest.contains("`orbit skills get orbit [path]`"),
        "served manifest must tell binary users the version-matched access path"
    );

    for path in ["SKILL.md", "references/local/cli.md"] {
        let out = hermetic_skills_cmd(cache.path())
            .args(["skills", "get", "orbit", path])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "`orbit skills get orbit {path}` failed"
        );
        assert!(
            !out.stdout.is_empty(),
            "`orbit skills get orbit {path}` printed nothing"
        );
    }

    let canonical = hermetic_skills_cmd(cache.path())
        .args(["skills", "get", "orbit", "references/local/cli.md"])
        .output()
        .unwrap();
    let named_alias = hermetic_skills_cmd(cache.path())
        .args(["skills", "orbit", "references/local/cli.md"])
        .output()
        .unwrap();
    let path_alias = hermetic_skills_cmd(cache.path())
        .args(["skills", "references/local/cli.md"])
        .output()
        .unwrap();
    assert!(canonical.status.success());
    assert_eq!(canonical.stdout, named_alias.stdout);
    assert_eq!(canonical.stdout, path_alias.stdout);
    assert!(
        !String::from_utf8(canonical.stdout)
            .unwrap()
            .contains("skills get orbit <path>"),
        "the discovery hint must be manifest-only, not appended to subfiles"
    );

    let singular_alias = hermetic_skills_cmd(cache.path())
        .args(["skill", "SKILL.md"])
        .output()
        .unwrap();
    assert!(singular_alias.status.success());
}

#[test]
fn skills_help_presents_get_as_the_canonical_command() {
    let skills_help = orbit_cmd().args(["skills", "--help"]).output().unwrap();
    assert!(skills_help.status.success());
    let skills_help = String::from_utf8(skills_help.stdout).unwrap();
    assert!(skills_help.contains("get   Print an instance-matched agent skill file."));
    assert!(!skills_help.contains("NAME_OR_PATH"));

    let get_help = orbit_cmd()
        .args(["skills", "get", "--help"])
        .output()
        .unwrap();
    assert!(get_help.status.success());
    let get_help = String::from_utf8(get_help.stdout).unwrap();
    assert!(get_help.contains(
        "Print a file from an agent skill served by the selected instance. The orbit skill is composed with local CLI guidance; other skills are printed as the server returns them."
    ));
    assert!(get_help.contains("Usage: orbit skills get <NAME> [PATH]"));
}

#[test]
fn skills_reject_unknown_names_and_paths() {
    let cache = tempfile::TempDir::new().unwrap();
    let unknown_name = hermetic_skills_cmd(cache.path())
        .args(["skills", "get", "unknown-name"])
        .output()
        .unwrap();
    assert!(!unknown_name.status.success());
    assert!(unknown_name.stdout.is_empty());
    let error = String::from_utf8(unknown_name.stderr).unwrap();
    assert!(error.contains("could not fetch the \"unknown-name\" skill"));
    assert!(error.contains("no GitLab credential found"));
    assert!(!error.contains("embedded local skill"));

    let path_as_name = hermetic_skills_cmd(cache.path())
        .args(["skills", "get", "references/local/cli.md"])
        .output()
        .unwrap();
    assert!(!path_as_name.status.success());
    assert!(path_as_name.stdout.is_empty());
    let error = String::from_utf8(path_as_name.stderr).unwrap();
    assert!(error.contains("invalid skill name \"references/local/cli.md\""));
    assert!(!error.contains("path shorthand"));

    for path in [
        "references/does-not-exist.md",
        "../Cargo.toml",
        "/etc/passwd",
        "references/local/../../../secret",
    ] {
        let out = hermetic_skills_cmd(cache.path())
            .args(["skills", "get", "orbit", path])
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "`orbit skills get orbit {path}` must exit non-zero"
        );
        assert!(
            out.stdout.is_empty(),
            "`orbit skills get orbit {path}` leaked stdout"
        );
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(
            err.contains("Available files") && err.contains("SKILL.md"),
            "error must list valid paths, got: {err}"
        );
        assert!(err.contains("skills get <name> [path]"));
    }
}

#[test]
fn grep_lists_matching_lines_with_their_definitions() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let dd = data_dir.path();
    assert!(orbit_index(&repo.path, dd));

    let out = orbit_cmd()
        .args(["grep", "open", "--repo"])
        .arg(&repo.path)
        .env("ORBIT_DATA_DIR", dd)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "grep failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout
            .contains("src/utils.py\n3-» Function read_file:3-4\n4:    return open(path).read()\n"),
        "{stdout}"
    );
    let repo_arg = repo.path.to_str().unwrap();
    let fqn = "src.utils.read_file";
    let (by_name, err, ok) = run_cmd(&["context", fqn, "--repo", repo_arg], dd);
    assert!(ok, "{err}");
    assert!(by_name.starts_with(&format!("{fqn}  [")), "{by_name}");
    let (file, err, ok) = run_cmd(&["context", "src/utils.py", "--repo", repo_arg], dd);
    assert!(
        ok && file.starts_with("src/utils.py:1-4\n1|import os\n")
            && file.ends_with("» indexed callers of read_file: run (src/main.py:11)\n"),
        "{err}\n{file}"
    );
    let args = ["context", "src/utils.py", fqn, fqn, "--repo", repo_arg];
    let (context, err, ok) = run_cmd(&args, dd);
    assert!(ok && context.contains(&file), "{err}\n{context}");
    assert_eq!(context.matches("return open(path).read()").count(), 2);
    for (command, query, expected) in [
        (
            "grep",
            "App|read_file|READ_FILE",
            "src/utils.py\n3-» Function read_file:3-4\n3:def read_file(path):\n",
        ),
        ("grep", "App|missing_symbol", " (App 1, missing_symbol 0)\n"),
        ("context", "src/utils.py:3-4", "3|def read_file(path):"),
        (
            "context",
            "src/utils.py:3-4",
            "» indexed callers of read_file: run (src/main.py:11)",
        ),
        (
            "context",
            "src/utils.py:4",
            "4|    return open(path).read()",
        ),
        ("context", "src", "Dir:  src  (2 files,"),
    ] {
        let (out, err, ok) = run_cmd(&[command, query, "--repo", repo_arg], dd);
        assert!(ok && out.contains(expected), "{query}: {err}\n{out}");
    }
    let (out, err, ok) = run_cmd(&["grep", "open", "src/main.py", "--repo", repo_arg], dd);
    assert!(
        ok && out.contains("» 0 matching lines in src/main.py; showing matches in src\n")
            && out.contains("src/utils.py\n"),
        "{err}\n{out}"
    );
    let (out, err, ok) = run_cmd(
        &["grep", "read_file", "lib/utils.py", "--repo", repo_arg],
        dd,
    );
    assert!(
        ok && out.contains("» lib/utils.py not found; searched src/utils.py\n"),
        "{err}\n{out}"
    );
    for missing in ["src.utils.read", "' OR true --", "../outside.py"] {
        let (out, err, ok) = run_cmd(&["context", fqn, missing, "--repo", repo_arg], dd);
        assert!(
            ok && out.starts_with(&format!("» {missing} does not exist\n"))
                && out.contains(&format!("{fqn}  [Function]")),
            "{out}\n{err}"
        );
    }
    let (out, err, ok) = run_cmd(&["context", fqn, "src/utils.py:40", "--repo", repo_arg], dd);
    assert!(!ok && out.is_empty(), "{out}\n{err}");
    for invalid in ["|", "||", " | "] {
        let (out, err, ok) = run_cmd(&["grep", invalid, "--repo", repo_arg], dd);
        assert!(
            !ok && err.contains("no usable search terms"),
            "{out}\n{err}"
        );
    }
}

#[test]
fn context_relationship_order_is_stable_across_overloads() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();
    let repo = workspace.path().join("repo");
    init_repo_at(
        &repo,
        &[
            (
                "src/Target.java",
                "public class Target {\n    public Target() {}\n    public void ping() {}\n}\n",
            ),
            (
                "src/Caller.java",
                concat!(
                    "public class Caller {\n",
                    "    public Caller(Target t) { t.ping(); }\n",
                    "    public Caller(Target t, int n) { t.ping(); }\n",
                    "    public void run(Target t) { t.ping(); }\n",
                    "    public void run(Target t, int n) { t.ping(); }\n",
                    "    public void run(Target t, int n, int m) { t.ping(); }\n",
                    "}\n",
                ),
            ),
        ],
    );
    let dd = data_dir.path();
    assert!(orbit_index(&repo, dd));

    let repo_arg = repo.to_str().unwrap();
    for (fqn, section) in [
        ("Target.ping", "Connections (5 indexed):"),
        ("Target", "Used via members (5 indexed):"),
    ] {
        let args = ["context", fqn, "--repo", repo_arg];
        let (first, stderr, ok) = run_cmd(&args, dd);
        assert!(ok, "context {fqn} failed: {stderr}");
        assert!(
            first.find("public void ping() {}").unwrap() < first.find(section).unwrap(),
            "{first}"
        );
        assert_eq!(first.matches("<-- Caller.Caller ").count(), 2, "{first}");
        assert_eq!(first.matches("<-- Caller.run ").count(), 3, "{first}");
        for _ in 0..10 {
            assert_eq!(first, run_cmd(&args, dd).0, "{fqn} output changed");
        }
    }
    let args = ["context", "Caller.run", "Caller.run", "--repo", repo_arg];
    let (by_name, stderr, ok) = run_cmd(&args, dd);
    assert!(ok, "{stderr}");
    assert_eq!(by_name.matches("|    public void run(").count(), 3);
    let (file, stderr, ok) = run_cmd(&["context", "src/Target.java", "--repo", repo_arg], dd);
    assert!(ok, "{stderr}");
    assert!(
        file.contains("public class Target")
            && file.contains(
                "» indexed callers of ping: Caller (src/Caller.java:2), Caller (:3), run (:4), run (:5), run (:6)"
            ),
        "{file}"
    );
}

#[test]
fn file_context_bounds_connections_and_keeps_full_definition_followups() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let workspace = tempfile::TempDir::new().unwrap();
    let repo = workspace.path().join("repo");
    let models = (0..67)
        .map(|i| format!("def entry_{i}():\n    return {i}\n\n"))
        .collect::<String>();
    let callers = (0..25).map(|i| format!(
        "import dependency_{i}\nfrom models import entry_0\ndef caller_{i}():\n    return entry_0()\n\n"
    )).collect::<String>();
    init_repo_at(
        &repo,
        &[
            ("src/models.py", &models),
            ("src/callers.py", &callers),
            ("tests/callers.py", &callers),
        ],
    );
    let dd = data_dir.path();
    assert!(orbit_index(&repo, dd));
    let repo_arg = repo.to_str().unwrap();
    let (out, err, ok) = run_cmd(&["context", "src/models.py", "--repo", repo_arg], dd);
    assert!(
        ok && out.starts_with("src/models.py:1-200\n"),
        "{err}\n{out}"
    );
    assert!(
        out.contains("» src/models.py has 201 lines; `")
            && out.contains(" context src/models.py:201-201` reads the rest\n"),
        "{out}"
    );
    assert!(
        out.contains(
            "» indexed callers of entry_0: caller_0 (src/callers.py:3), caller_1 (:8), caller_2 (:13), caller_3 (:18), caller_4 (:23), +20 more, 25 in tests\n"
        ),
        "{out}"
    );
    let (body, err, ok) = run_cmd(&["context", "src.models.entry_0", "--repo", repo_arg], dd);
    assert!(ok && body.contains("return 0"), "{err}\n{body}");
    assert_eq!(body.matches("[calls]").count(), 50, "{body}");
    assert!(!body.contains("connections omitted"), "{body}");
}

#[test]
fn piped_index_prints_one_json_document_per_repository() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();

    let out = orbit_cmd()
        .args(["workspace", "index", repo.path.to_str().unwrap()])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let documents: Vec<Value> = serde_json::Deserializer::from_slice(&out.stdout)
        .into_iter()
        .collect::<Result<_, _>>()
        .expect("stdout is a stream of JSON documents");
    assert_eq!(documents.len(), 1);
    let document = &documents[0];
    let repository_name = repo.path.file_name().unwrap().to_str().unwrap();
    assert_eq!(document["repository"], json!(repository_name));
    assert!(document["graph"]["files"].as_u64().unwrap() >= 2);
    assert!(document["graph"]["definitions"].as_u64().unwrap() >= 1);
    assert!(document["processing"].is_object());
}
