use std::path::Path;
use std::process::Output;
use std::time::Duration;

use integration_testkit::cli::{init_repo_at, orbit_cmd};
use serde_json::Value;

const EXIT_WAIT_TIMED_OUT: i32 = 75;

/// Large enough that an index outlasts the few milliseconds the assertions below need.
fn slow_repo(parent: &Path) -> std::path::PathBuf {
    let repo = parent.join("repo");
    let files: Vec<(String, String)> = (0..1500)
        .map(|n| {
            let body = (0..20)
                .map(|m| {
                    format!(
                        "def func_{n}_{m}(x):\n    return func_{n}_{}(x)\n",
                        (m + 1) % 20
                    )
                })
                .collect::<String>();
            (format!("pkg{}/mod_{n}.py", n % 30), body)
        })
        .collect();
    let files: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    init_repo_at(&repo, &files);
    repo
}

fn orbit(args: &[&str], repo: &Path, data_dir: &Path) -> Output {
    orbit_cmd()
        .args(args)
        .current_dir(repo)
        .env("ORBIT_DATA_DIR", data_dir)
        .output()
        .unwrap()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn start_async(repo: &Path, data_dir: &Path) -> Value {
    let output = orbit(&["workspace", "index", "--async"], repo, data_dir);
    assert!(output.status.success(), "{}", stderr_of(&output));
    serde_json::from_slice(&output.stdout).expect("one JSON document")
}

fn manifest_status(repo: &Path, data_dir: &Path) -> String {
    let output = orbit(&["workspace", "list", "-F", "json"], repo, data_dir);
    let rows: Value = serde_json::from_slice(&output.stdout).unwrap_or_default();
    rows[0]["status"].as_str().unwrap_or_default().to_string()
}

fn wait_for_manifest_status(repo: &Path, data_dir: &Path, status: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while manifest_status(repo, data_dir) != status {
        assert!(
            std::time::Instant::now() < deadline,
            "never reached {status}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn file_count(repo: &Path, data_dir: &Path) -> (i64, String) {
    let output = orbit(
        &[
            "workspace",
            "sql",
            "-F",
            "json",
            "SELECT count(*) AS n FROM gl_file",
        ],
        repo,
        data_dir,
    );
    assert!(output.status.success(), "{}", stderr_of(&output));
    let rows: Value = serde_json::from_slice(&output.stdout).unwrap();
    (rows[0]["n"].as_i64().unwrap(), stderr_of(&output))
}

#[test]
fn async_index_returns_while_the_index_runs_and_a_read_waits_for_it() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = slow_repo(scratch.path());

    let started = start_async(&repo, data_dir.path());
    assert_eq!(started["status"], "indexing", "{started}");

    let (files, stderr) = file_count(&repo, data_dir.path());
    assert!(files >= 1500, "{files} files");
    assert!(stderr.contains("waiting for the index of"), "{stderr}");
    assert!(stderr.contains("s ago)"), "{stderr}");
}

#[test]
fn wait_gives_up_with_a_distinct_exit_code_while_an_index_runs() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = slow_repo(scratch.path());
    let started = start_async(&repo, data_dir.path());
    assert_eq!(started["status"], "indexing", "{started}");

    let output = orbit(
        &["workspace", "index", "--wait", "--timeout", "0"],
        &repo,
        data_dir.path(),
    );

    assert_eq!(output.status.code(), Some(EXIT_WAIT_TIMED_OUT));
    assert_eq!(
        stderr_of(&output).lines().count(),
        1,
        "{}",
        stderr_of(&output)
    );
    let waited = orbit(&["workspace", "index", "--wait"], &repo, data_dir.path());
    assert!(waited.status.success(), "{}", stderr_of(&waited));
    assert!(file_count(&repo, data_dir.path()).0 >= 1500);
}

#[test]
fn a_killed_index_is_recovered_by_the_next_read() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = slow_repo(scratch.path());
    let started = start_async(&repo, data_dir.path());
    assert_eq!(started["status"], "indexing", "{started}");

    wait_for_manifest_status(&repo, data_dir.path(), "indexing");
    let killed = std::process::Command::new("kill")
        .args(["-9", &started["pid"].to_string()])
        .status()
        .unwrap();
    assert!(killed.success());

    assert_eq!(manifest_status(&repo, data_dir.path()), "indexing");
    let (files, _) = file_count(&repo, data_dir.path());
    assert!(files >= 1500, "{files} files");
    assert_eq!(manifest_status(&repo, data_dir.path()), "indexed");
}

#[test]
fn first_sql_and_schema_index_the_repository() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, &[("greeter.py", "def greet_visitor(): pass\n")]);

    let schema = orbit(&["workspace", "schema"], &repo, data_dir.path());
    assert!(schema.status.success(), "{}", stderr_of(&schema));
    assert!(String::from_utf8_lossy(&schema.stdout).contains("gl_definition"));
    assert_eq!(file_count(&repo, data_dir.path()).0, 1);

    let other_data_dir = tempfile::tempdir().unwrap();
    assert_eq!(file_count(&repo, other_data_dir.path()).0, 1);
}

fn hold_revision_lock(data_dir: &Path, repo: &Path) -> std::fs::File {
    let indexed = orbit(&["workspace", "index"], repo, data_dir);
    assert!(indexed.status.success(), "{}", stderr_of(&indexed));
    let manifest = orbit(
        &[
            "workspace",
            "sql",
            "-F",
            "json",
            "SELECT database_path FROM _orbit_manifest",
        ],
        repo,
        data_dir,
    );
    let rows: Value = serde_json::from_slice(&manifest.stdout).unwrap();
    let lock = std::fs::File::options()
        .read(true)
        .write(true)
        .open(format!(
            "{}.lock",
            rows[0]["database_path"].as_str().unwrap()
        ))
        .unwrap();
    lock.lock().unwrap();
    lock
}

#[test]
fn a_read_waits_on_the_revision_lock_then_succeeds() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, &[("greeter.py", "def greet_visitor(): pass\n")]);
    let lock = hold_revision_lock(data_dir.path(), &repo);
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(2500));
        lock.unlock().unwrap();
    });

    let (files, stderr) = file_count(&repo, data_dir.path());

    releaser.join().unwrap();
    assert_eq!(files, 1);
    assert!(stderr.contains("waiting for the index of"), "{stderr}");
}

#[test]
fn a_read_that_waits_too_long_exits_with_the_distinct_code() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, &[("greeter.py", "def greet_visitor(): pass\n")]);
    let _lock = hold_revision_lock(data_dir.path(), &repo);

    let output = orbit_cmd()
        .args(["workspace", "sql", "SELECT 1"])
        .current_dir(&repo)
        .env("ORBIT_DATA_DIR", data_dir.path())
        .env("ORBIT_WAIT_TIMEOUT_MS", "300")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(EXIT_WAIT_TIMED_OUT));
    assert!(
        stderr_of(&output).contains("is still indexing (started"),
        "{}",
        stderr_of(&output)
    );
}

#[test]
fn async_index_waits_for_open_readers_so_later_reads_see_the_new_graph() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, &[("a.py", "def a(): pass\n")]);
    let lock = hold_revision_lock_shared(data_dir.path(), &repo);
    std::fs::write(repo.join("b.py"), "def b(): pass\n").unwrap();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        lock.unlock().unwrap();
    });

    let began = std::time::Instant::now();
    let started = start_async(&repo, data_dir.path());
    let waited = began.elapsed();
    let (files, _) = file_count(&repo, data_dir.path());

    releaser.join().unwrap();
    assert_eq!(started["status"], "indexing", "{started}");
    assert!(
        waited >= Duration::from_millis(1000),
        "returned after {waited:?}"
    );
    assert_eq!(files, 2, "a read after --async must not see the old graph");
}

#[test]
fn async_index_of_a_directory_of_repositories_returns_promptly() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let workspace = scratch.path().join("workspace");
    let big = slow_repo(&workspace);
    std::fs::rename(&big, workspace.join("big")).unwrap();
    init_repo_at(&workspace.join("small"), &[("a.py", "def a(): pass\n")]);

    let began = std::time::Instant::now();
    let output = orbit(
        &["workspace", "index", "--async", workspace.to_str().unwrap()],
        scratch.path(),
        data_dir.path(),
    );

    assert!(output.status.success(), "{}", stderr_of(&output));
    assert!(began.elapsed() < Duration::from_secs(10));
}

#[test]
fn a_failed_background_index_leaves_its_error_in_the_log() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let first = scratch.path().join("first");
    let second = scratch.path().join("second");
    init_repo_at(&first, &[("a.py", "def a(): pass\n")]);
    init_repo_at(&second, &[("b.py", "def b(): pass\n")]);
    let custom = scratch.path().join("custom.duckdb");
    let custom = custom.to_str().unwrap();
    let indexed = orbit(
        &["workspace", "index", "--db", custom],
        &first,
        data_dir.path(),
    );
    assert!(indexed.status.success(), "{}", stderr_of(&indexed));

    let output = orbit(
        &["workspace", "index", "--async", "--db", custom],
        &second,
        data_dir.path(),
    );

    let log = data_dir.path().join("var/detached-index.log");
    assert!(!output.status.success());
    assert!(
        stderr_of(&output).contains("detached-index.log"),
        "{}",
        stderr_of(&output)
    );
    assert!(
        std::fs::read_to_string(log)
            .unwrap()
            .contains("belongs to another checkout revision")
    );
}

#[test]
fn an_incomplete_explicit_database_is_recovered_by_the_next_read() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = slow_repo(scratch.path());
    let custom = scratch.path().join("custom.duckdb");
    let custom = custom.to_str().unwrap();
    let output = orbit(
        &["workspace", "index", "--async", "--db", custom],
        &repo,
        data_dir.path(),
    );
    assert!(output.status.success(), "{}", stderr_of(&output));
    let started: Value = serde_json::from_slice(&output.stdout).unwrap();
    wait_for_manifest_status(&repo, data_dir.path(), "indexing");
    std::process::Command::new("kill")
        .args(["-9", &started["pid"].to_string()])
        .status()
        .unwrap();

    let output = orbit(
        &[
            "workspace",
            "sql",
            "-F",
            "json",
            "--db",
            custom,
            "SELECT count(*) AS n FROM gl_file",
        ],
        &repo,
        data_dir.path(),
    );

    assert!(output.status.success(), "{}", stderr_of(&output));
    let rows: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(rows[0]["n"].as_i64().unwrap() >= 1500);
}

#[test]
fn sql_indexes_a_never_indexed_repository_even_when_others_are_indexed() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let indexed = scratch.path().join("indexed");
    let fresh = scratch.path().join("fresh");
    init_repo_at(&indexed, &[("a.py", "def a(): pass\n")]);
    init_repo_at(
        &fresh,
        &[("b.py", "def b(): pass\n"), ("c.py", "def c(): pass\n")],
    );
    let output = orbit(&["workspace", "index"], &indexed, data_dir.path());
    assert!(output.status.success(), "{}", stderr_of(&output));

    let (files, _) = file_count(&fresh, data_dir.path());

    assert_eq!(files, 3);
}

#[test]
fn sql_leaves_a_stale_head_to_refresh() {
    let data_dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, &[("a.py", "def a(): pass\n")]);
    let output = orbit(&["workspace", "index"], &repo, data_dir.path());
    assert!(output.status.success(), "{}", stderr_of(&output));
    std::fs::write(repo.join("b.py"), "def b(): pass\n").unwrap();
    integration_testkit::cli::git(&repo, &["add", "-A"]);
    integration_testkit::cli::git(&repo, &["commit", "-m", "second"]);

    let (files, _) = file_count(&repo, data_dir.path());

    assert_eq!(files, 1, "a new commit alone must not trigger an index");
}

fn hold_revision_lock_shared(data_dir: &Path, repo: &Path) -> std::fs::File {
    let lock = hold_revision_lock(data_dir, repo);
    lock.unlock().unwrap();
    lock.lock_shared().unwrap();
    lock
}
