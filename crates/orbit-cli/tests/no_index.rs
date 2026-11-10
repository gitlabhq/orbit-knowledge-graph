use std::path::Path;
use std::process::{Command, Output};

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args([
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .status()
        .expect("run git");
    assert!(status.success(), "git {args:?} failed");
}

fn repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        repo.path().join("lib.rs"),
        "pub fn parse_widget() {}\npub fn caller() {\n    parse_widget();\n}\n",
    )
    .expect("write source");
    git(repo.path(), &["init", "-q"]);
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "init"]);
    repo
}

fn orbit(repo: &Path, data: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .current_dir(repo)
        .args(args)
        .env("ORBIT_DATA_DIR", data)
        .env("NO_COLOR", "1")
        .output()
        .expect("run orbit")
}

fn assert_nothing_indexed(data: &Path) {
    let stored: Vec<_> = std::fs::read_dir(data)
        .map(|entries| entries.flatten().map(|entry| entry.file_name()).collect())
        .unwrap_or_default();
    assert!(stored.is_empty(), "no-index created {stored:?}");
}

#[test]
fn no_index_fails_without_building_an_index() {
    let (repo, data) = (repo(), tempfile::tempdir().expect("tempdir"));
    for args in [
        &["grep", "--no-index", "parse_widget"][..],
        &["context", "--no-index", "parse_widget"][..],
    ] {
        let output = orbit(repo.path(), data.path(), args);
        assert!(
            !output.status.success(),
            "{args:?} succeeded without an index"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("--no-index does not build one"),
            "{args:?}: {stderr}"
        );
        assert_nothing_indexed(data.path());
    }
}

#[test]
fn no_index_reads_an_existing_index() {
    let (repo, data) = (repo(), tempfile::tempdir().expect("tempdir"));
    let indexed = orbit(repo.path(), data.path(), &["index"]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    for args in [
        &["grep", "--no-index", "parse_widget"][..],
        &["context", "--no-index", "parse_widget"][..],
    ] {
        let output = orbit(repo.path(), data.path(), args);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout.contains("parse_widget"), "{args:?}: {stdout}");
    }
}

/// The nightly search guard (packages/agent-plugins-nightly/search-guard) parses these two outputs.
/// Its fixtures in tests/search-guard-support.ts must change with them.
#[test]
fn grep_and_context_print_the_shapes_the_search_guard_parses() {
    let (repo, data) = (repo(), tempfile::tempdir().expect("tempdir"));
    let indexed = orbit(repo.path(), data.path(), &["index"]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let listed = orbit(
        repo.path(),
        data.path(),
        &["workspace", "list", "--format", "json"],
    );
    let rows: serde_json::Value = serde_json::from_slice(&listed.stdout).expect("json rows");
    let database = rows[0]["database_path"].as_str().expect("database_path");
    assert_eq!(rows[0]["status"], "indexed");
    assert!(rows[0]["repo_path"].is_string() && rows[0]["commit_sha"].is_string());
    assert!(rows[0]["last_indexed_at"].is_string());

    let top = repo.path().to_str().expect("utf-8 path");
    let grep = orbit(
        repo.path(),
        data.path(),
        &["grep", "--no-index", "--db", database, "parse_widget", top],
    );
    let grep = String::from_utf8_lossy(&grep.stdout);
    assert!(
        grep.lines()
            .any(|line| line == "Function parse_widget — lib.rs:1-1"),
        "{grep}"
    );

    let context = orbit(
        repo.path(),
        data.path(),
        &["context", "--no-index", "--db", database, "parse_widget"],
    );
    let context = String::from_utf8_lossy(&context.stdout);
    let lines: Vec<&str> = context.lines().collect();
    let connections = lines
        .iter()
        .position(|line| *line == "Connections (1 indexed):")
        .unwrap_or_else(|| panic!("{context}"));
    assert_eq!(
        lines[connections - 2],
        "parse_widget  [Function]  lib.rs:1-1",
        "{context}"
    );
    assert_eq!(
        lines[connections + 1],
        "  <-- caller  [calls]  (lib.rs:2)",
        "{context}"
    );
}

fn indexed_database(repo: &Path, data: &Path) -> String {
    let indexed = orbit(repo, data, &["index"]);
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let listed = orbit(repo, data, &["workspace", "list", "--format", "json"]);
    let rows: serde_json::Value = serde_json::from_slice(&listed.stdout).expect("json rows");
    rows[0]["database_path"]
        .as_str()
        .expect("database_path")
        .to_string()
}

fn assert_refused(repo: &Path, data: &Path, database: &str) {
    for command in ["grep", "context"] {
        let output = orbit(
            repo,
            data,
            &[command, "--no-index", "--db", database, "parse_widget"],
        );
        assert!(
            !output.status.success(),
            "{command} read a database it should refuse"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("--no-index does not build one"),
            "{command}: {stderr}"
        );
    }
}

#[test]
fn no_index_refuses_a_database_whose_file_is_gone_and_does_not_recreate_it() {
    let (repo, data) = (repo(), tempfile::tempdir().expect("tempdir"));
    let database = indexed_database(repo.path(), data.path());
    std::fs::remove_file(&database).expect("remove database");

    assert_refused(repo.path(), data.path(), &database);
    assert!(
        !Path::new(&database).exists(),
        "--no-index recreated {database}"
    );
}

#[test]
fn no_index_refuses_a_database_with_a_stale_code_index_revision() {
    let (repo, data) = (repo(), tempfile::tempdir().expect("tempdir"));
    let database = indexed_database(repo.path(), data.path());
    let client = duckdb_client::DuckDbClient::open(Path::new(&database)).expect("open database");
    client
        .execute(
            "UPDATE _orbit_meta SET value = '0' WHERE key = 'code_index_revision'",
            &[],
        )
        .expect("age the revision");
    drop(client);

    assert_refused(repo.path(), data.path(), &database);
}
