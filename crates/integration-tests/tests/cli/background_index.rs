use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

use integration_testkit::cli::{git, init_repo_at, orbit_cmd};
use serde_json::Value;

const WAIT_TIMED_OUT: i32 = 75;
const ONE_FILE: &[(&str, &str)] = &[("a.py", "def a(): pass\n")];

/// A repository whose index outlasts the few milliseconds the assertions need.
fn big_repo(parent: &Path, files: usize) -> PathBuf {
    let repo = parent.join("repo");
    let sources: Vec<(String, String)> = (0..files)
        .map(|n| {
            let body: String = (0..20)
                .map(|m| format!("def f_{n}_{m}(x):\n    return f_{n}_{}(x)\n", (m + 1) % 20))
                .collect();
            (format!("pkg{}/m_{n}.py", n % 30), body)
        })
        .collect();
    let sources: Vec<(&str, &str)> = sources.iter().map(|(a, b)| (&a[..], &b[..])).collect();
    init_repo_at(&repo, &sources);
    repo
}

fn orbit(args: &[&str], cwd: &Path, data: &Path) -> Output {
    orbit_cmd()
        .args(args)
        .current_dir(cwd)
        .env("ORBIT_DATA_DIR", data)
        .output()
        .unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn json(output: &Output) -> Value {
    assert!(output.status.success(), "{}", stderr(output));
    serde_json::from_slice(&output.stdout).unwrap()
}

fn start_async(cwd: &Path, data: &Path, extra: &[&str]) -> Value {
    let args = [&["workspace", "index", "--async"], extra].concat();
    json(&orbit(&args, cwd, data))
}

fn files(cwd: &Path, data: &Path) -> (i64, String) {
    let sql = [
        "workspace",
        "sql",
        "-F",
        "json",
        "SELECT count(*) AS n FROM gl_file",
    ];
    let output = orbit(&sql, cwd, data);
    (json(&output)[0]["n"].as_i64().unwrap(), stderr(&output))
}

fn manifest_status(cwd: &Path, data: &Path) -> String {
    let output = orbit(&["workspace", "list", "-F", "json"], cwd, data);
    let rows: Value = serde_json::from_slice(&output.stdout).unwrap_or_default();
    rows[0]["status"].as_str().unwrap_or_default().to_string()
}

fn kill_while_indexing(started: &Value, cwd: &Path, data: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while manifest_status(cwd, data) != "indexing" {
        assert!(Instant::now() < deadline, "the index never started");
        std::thread::sleep(Duration::from_millis(20));
    }
    let pid = started["pid"].to_string();
    assert!(
        std::process::Command::new("kill")
            .args(["-9", &pid])
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(manifest_status(cwd, data), "indexing");
}

/// Indexes `repo`, then takes the revision lock the way an index (exclusive) or a reader would.
fn hold_revision_lock(cwd: &Path, data: &Path, shared: bool) -> std::fs::File {
    assert!(orbit(&["workspace", "index"], cwd, data).status.success());
    let sql = [
        "workspace",
        "sql",
        "-F",
        "json",
        "SELECT database_path FROM _orbit_manifest",
    ];
    let db = json(&orbit(&sql, cwd, data))[0]["database_path"]
        .as_str()
        .unwrap()
        .to_string();
    let lock = std::fs::File::options()
        .read(true)
        .write(true)
        .open(format!("{db}.lock"))
        .unwrap();
    match shared {
        true => lock.lock_shared().unwrap(),
        false => lock.lock().unwrap(),
    }
    lock
}

fn release_after(lock: std::fs::File, delay: Duration) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        std::thread::sleep(delay);
        lock.unlock().unwrap();
    })
}

#[test]
fn async_waits_for_open_readers_so_the_next_read_sees_the_new_graph() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, ONE_FILE);
    let reader = hold_revision_lock(&repo, data.path(), true);
    std::fs::write(repo.join("b.py"), "def b(): pass\n").unwrap();
    let releaser = release_after(reader, Duration::from_millis(1500));

    let began = Instant::now();
    let started = start_async(&repo, data.path(), &[]);
    let waited = began.elapsed();
    let (count, _) = files(&repo, data.path());

    releaser.join().unwrap();
    assert_eq!(started["status"], "indexing");
    assert!(
        waited >= Duration::from_millis(1000),
        "returned after {waited:?}"
    );
    assert_eq!(count, 2, "a read after --async must not see the old graph");
}

#[test]
fn async_that_cannot_get_its_locks_exits_75() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, ONE_FILE);
    let _reader = hold_revision_lock(&repo, data.path(), true);
    std::fs::write(repo.join("b.py"), "def b(): pass\n").unwrap();

    let output = orbit_cmd()
        .args(["workspace", "index", "--async"])
        .current_dir(&repo)
        .env("ORBIT_DATA_DIR", data.path())
        .env("ORBIT_WAIT_TIMEOUT_MS", "500")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(WAIT_TIMED_OUT));
    assert!(
        stderr(&output).contains("keeps waiting; see"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn a_running_index_is_visible_to_async_wait_and_reads() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let repo = big_repo(scratch.path(), 1500);
    let first = start_async(&repo, data.path(), &[]);
    assert_eq!(first["status"], "indexing");

    let second = start_async(&repo, data.path(), &[]);
    let wait = orbit(
        &["workspace", "index", "--wait", "--timeout", "0"],
        &repo,
        data.path(),
    );
    let (count, notice) = files(&repo, data.path());

    assert_eq!(second["status"], "queued");
    assert_eq!(wait.status.code(), Some(WAIT_TIMED_OUT));
    assert!(stderr(&wait).contains("(started"), "{}", stderr(&wait));
    assert!(count >= 1500);
    assert!(notice.contains("waiting for the index of"), "{notice}");
}

#[test]
fn a_read_waits_on_the_revision_lock_then_times_out_with_75() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let repo = scratch.path().join("repo");
    init_repo_at(&repo, ONE_FILE);
    let writer = hold_revision_lock(&repo, data.path(), false);
    let timed_out = orbit_cmd()
        .args(["workspace", "sql", "SELECT 1"])
        .current_dir(&repo)
        .env("ORBIT_DATA_DIR", data.path())
        .env("ORBIT_WAIT_TIMEOUT_MS", "300")
        .output()
        .unwrap();
    let releaser = release_after(writer, Duration::from_millis(2500));

    let (count, notice) = files(&repo, data.path());

    releaser.join().unwrap();
    assert_eq!(timed_out.status.code(), Some(WAIT_TIMED_OUT));
    assert!(
        stderr(&timed_out).contains("(started"),
        "{}",
        stderr(&timed_out)
    );
    assert_eq!(count, 1);
    assert!(notice.contains("waiting for the index of"), "{notice}");
}

#[test]
fn a_killed_index_is_recovered_by_the_next_read_in_the_workspace_and_in_an_explicit_db() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let repo = big_repo(scratch.path(), 1500);
    let custom = scratch.path().join("custom.duckdb");
    let custom = custom.to_str().unwrap();

    let started = start_async(&repo, data.path(), &[]);
    kill_while_indexing(&started, &repo, data.path());
    assert!(files(&repo, data.path()).0 >= 1500);

    let started = start_async(&repo, data.path(), &["--db", custom]);
    kill_while_indexing(&started, &repo, data.path());
    let sql = [
        "workspace",
        "sql",
        "-F",
        "json",
        "--db",
        custom,
        "SELECT count(*) AS n FROM gl_file",
    ];
    assert!(
        json(&orbit(&sql, &repo, data.path()))[0]["n"]
            .as_i64()
            .unwrap()
            >= 1500
    );
}

#[test]
fn sql_and_schema_index_a_never_indexed_repository_but_leave_a_stale_head_to_refresh() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (first, second) = (scratch.path().join("first"), scratch.path().join("second"));
    init_repo_at(&first, ONE_FILE);
    init_repo_at(
        &second,
        &[("b.py", "def b(): pass\n"), ("c.py", "def c(): pass\n")],
    );

    let schema = orbit(&["workspace", "schema"], &first, data.path());
    assert!(String::from_utf8_lossy(&schema.stdout).contains("gl_definition"));
    assert_eq!(files(&first, data.path()).0, 1);
    assert_eq!(
        files(&second, data.path()).0,
        3,
        "other repositories are indexed already"
    );

    std::fs::write(second.join("d.py"), "def d(): pass\n").unwrap();
    git(&second, &["add", "-A"]);
    git(&second, &["commit", "-m", "more"]);
    assert_eq!(
        files(&second, data.path()).0,
        3,
        "a new commit alone must not index"
    );
}

#[test]
fn async_index_of_a_directory_returns_before_the_child_finishes() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let workspace = scratch.path().join("workspace");
    std::fs::rename(big_repo(&workspace, 3000), workspace.join("big")).unwrap();
    init_repo_at(&workspace.join("small"), ONE_FILE);

    let began = Instant::now();
    let output = orbit(
        &["workspace", "index", "--async", "."],
        &workspace,
        data.path(),
    );

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "returned after {:?}",
        began.elapsed()
    );
}

#[test]
fn a_failed_background_index_leaves_its_error_in_the_log() {
    let (data, scratch) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (first, second) = (scratch.path().join("first"), scratch.path().join("second"));
    init_repo_at(&first, ONE_FILE);
    init_repo_at(&second, &[("b.py", "def b(): pass\n")]);
    let custom = scratch.path().join("custom.duckdb");
    let custom = custom.to_str().unwrap();
    assert!(
        orbit(&["workspace", "index", "--db", custom], &first, data.path())
            .status
            .success()
    );

    let output = orbit(
        &["workspace", "index", "--async", "--db", custom],
        &second,
        data.path(),
    );

    let message = stderr(&output);
    let log = message
        .split("see ")
        .nth(1)
        .expect("the error names the log")
        .trim();
    assert!(!output.status.success());
    assert!(
        std::fs::read_to_string(log)
            .unwrap()
            .contains("belongs to another checkout revision")
    );
}
