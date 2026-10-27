use super::{FILES_SIMPLE, run_cmd};
use integration_testkit::cli::{
    create_test_repo, git, mcp_roundtrip, mcp_tool_call, mcp_tool_text, orbit_cmd, orbit_index,
    orbit_sql, rows, rows_where,
};
use serde_json::{Value, json};

#[test]
fn checkout_revision_journey_through_cli_and_mcp() {
    let data = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let first = git(&repo.path, &["rev-parse", "HEAD"]);
    let command = |args: &[&str]| {
        let output = orbit_cmd()
            .args(args)
            .current_dir(&repo.path)
            .env("ORBIT_DATA_DIR", data.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    command(&["workspace", "index", "."]);
    git(&repo.path, &["switch", "-c", "feature"]);
    std::fs::write(
        repo.path.join("feature.py"),
        "def revision_feature(): return 42\n",
    )
    .unwrap();
    git(&repo.path, &["add", "feature.py"]);
    git(&repo.path, &["commit", "-m", "add feature"]);
    let second = git(&repo.path, &["rev-parse", "HEAD"]);
    command(&["workspace", "index", "."]);
    command(&["workspace", "index", "."]);

    let listed: Value =
        serde_json::from_str(&command(&["workspace", "list", "-F", "json"])).unwrap();
    assert_eq!(rows(&listed).len(), 2);
    for commit in [&first, &second] {
        assert_eq!(
            rows_where(&listed, "commit_sha", commit)[0]["status"],
            "indexed"
        );
    }
    let all: Value = serde_json::from_str(&command(&[
        "workspace",
        "sql",
        "-F",
        "json",
        "SELECT commit_sha, count(*) AS files FROM gl_file GROUP BY commit_sha ORDER BY files",
    ]))
    .unwrap();
    assert_eq!(
        all,
        json!([{"commit_sha": first, "files": 2}, {"commit_sha": second, "files": 3}])
    );

    for (commit, files) in [(&second, 3), (&first, 2)] {
        git(&repo.path, &["checkout", "--detach", commit]);
        let scoped: Value = serde_json::from_str(&command(&[
            "workspace",
            "sql",
            "--repo",
            ".",
            "-F",
            "json",
            "SELECT count(*) AS files FROM gl_file",
        ]))
        .unwrap();
        assert_eq!(scoped, json!([{"files": files}]));
        assert!(command(&["grep", "hello"]).contains("src/main.py"));
        assert!(command(&["context", "src/main.py"]).contains("hello"));
    }
    let responses = mcp_roundtrip(
        data.path(),
        &[
            mcp_tool_call(
                1,
                "run_sql",
                json!({"sql": ["SELECT count(*) AS files FROM gl_file"]}),
            ),
            mcp_tool_call(2, "get_graph_schema", json!({})),
        ],
    );
    assert_eq!(
        serde_json::from_str::<Value>(mcp_tool_text(&responses[0])).unwrap(),
        json!([[{"files": 5}]])
    );
    assert!(mcp_tool_text(&responses[1]).contains("gl_file"));
}

fn seed_revision(data_dir: &std::path::Path, name: &str, ddl: &str) -> std::path::PathBuf {
    let path = data_dir.join(format!("{name}.duckdb"));
    let graph = duckdb_client::DuckDbClient::open(&path).unwrap();
    graph.initialize_schema(ddl).unwrap();
    graph.initialize_schema("CREATE TABLE _orbit_meta(key VARCHAR, value VARCHAR); INSERT INTO _orbit_meta VALUES ('indexed_commit', 'test')").unwrap();
    let manifest =
        duckdb_client::DuckDbClient::open(&data_dir.join("local/manifest.duckdb")).unwrap();
    manifest
        .initialize_schema(include_str!(concat!(
            env!("CONFIG_DIR"),
            "/orbit_cli/manifest.sql"
        )))
        .unwrap();
    manifest.execute("INSERT INTO _orbit_manifest (repo_path, project_id, commit_sha, database_path, status) VALUES (?1, 1, 'test', ?2, 'indexed')", &[name.into(), path.to_string_lossy().to_string().into()]).unwrap();
    path
}

#[test]
fn workspace_queries_match_columns_by_name_and_include_tables_from_each_revision() {
    let data = tempfile::TempDir::new().unwrap();
    seed_revision(
        data.path(),
        "a",
        "CREATE TABLE gl_probe(name VARCHAR, path VARCHAR); INSERT INTO gl_probe VALUES ('first', 'first.py')",
    );
    seed_revision(
        data.path(),
        "b",
        "CREATE TABLE gl_probe(path VARCHAR, name VARCHAR, added BIGINT); INSERT INTO gl_probe VALUES ('second.py', 'second', 7); CREATE TABLE gl_new(id BIGINT); INSERT INTO gl_new VALUES (42)",
    );
    assert_eq!(
        orbit_sql("SELECT * FROM gl_probe ORDER BY name", data.path()),
        json!([
            {"name":"first", "path":"first.py"},
            {"name":"second", "path":"second.py", "added":7}
        ])
    );
    assert_eq!(
        orbit_sql("SELECT * FROM gl_new", data.path()),
        json!([{"id":42}])
    );
    let (_, error, ok) = run_cmd(&["workspace", "schema", "--raw"], data.path());
    assert!(ok, "{error}");
}

#[test]
fn workspace_queries_skip_abandoned_and_missing_revisions() {
    let data = tempfile::TempDir::new().unwrap();
    let broken = seed_revision(
        data.path(),
        "a",
        "CREATE TABLE gl_probe(id BIGINT); INSERT INTO gl_probe VALUES (99)",
    );
    seed_revision(
        data.path(),
        "b",
        "CREATE TABLE gl_probe(id BIGINT); INSERT INTO gl_probe VALUES (42)",
    );
    let manifest =
        duckdb_client::DuckDbClient::open(&data.path().join("local/manifest.duckdb")).unwrap();
    manifest
        .execute(
            "UPDATE _orbit_manifest SET status = 'indexing' WHERE repo_path = 'a'",
            &[],
        )
        .unwrap();
    drop(manifest);
    for (change, diagnostic) in [
        (
            Some("DELETE FROM _orbit_meta"),
            "skipping incomplete revision",
        ),
        (
            Some("DROP TABLE _orbit_meta"),
            "skipping incomplete revision",
        ),
        (None, "skipping missing revision"),
    ] {
        match change {
            Some(sql) => {
                let graph = duckdb_client::DuckDbClient::open(&broken).unwrap();
                graph.execute(sql, &[]).unwrap();
            }
            None => {
                std::fs::remove_file(&broken).unwrap();
                std::fs::remove_file(broken.with_extension("duckdb.lock")).unwrap();
            }
        }
        let (output, error, ok) = run_cmd(
            &["workspace", "sql", "-F", "json", "SELECT * FROM gl_probe"],
            data.path(),
        );
        assert!(ok, "{error}");
        assert!(error.contains(diagnostic), "{error}");
        assert_eq!(
            serde_json::from_str::<Value>(&output).unwrap(),
            json!([{"id":42}])
        );
    }
    let responses = mcp_roundtrip(
        data.path(),
        &[
            mcp_tool_call(1, "run_sql", json!({"sql":["SELECT * FROM gl_probe"]})),
            mcp_tool_call(2, "get_graph_schema", json!({})),
        ],
    );
    assert_eq!(
        serde_json::from_str::<Value>(mcp_tool_text(&responses[0])).unwrap(),
        json!([[{"id":42}]])
    );
    assert_ne!(responses[1]["result"]["isError"], true);
    let (_, error, ok) = run_cmd(
        &[
            "workspace",
            "sql",
            "--db",
            broken.to_str().unwrap(),
            "SELECT 1",
        ],
        data.path(),
    );
    assert!(!ok, "explicit missing file must fail: {error}");
    assert!(!broken.with_extension("duckdb.lock").exists());
    let missing = data.path().join("missing/revision.duckdb");
    let manifest =
        duckdb_client::DuckDbClient::open(&data.path().join("local/manifest.duckdb")).unwrap();
    manifest
        .execute(
            "UPDATE _orbit_manifest SET database_path = ?1 WHERE repo_path = 'a'",
            &[missing.to_string_lossy().to_string().into()],
        )
        .unwrap();
    drop(manifest);
    assert_eq!(
        orbit_sql("SELECT * FROM gl_probe", data.path()),
        json!([{"id":42}])
    );
    let (_, error, ok) = run_cmd(
        &[
            "workspace",
            "sql",
            "--db",
            missing.to_str().unwrap(),
            "SELECT 1",
        ],
        data.path(),
    );
    assert!(!ok, "explicit missing file must fail: {error}");
    assert!(!missing.parent().unwrap().exists());
}

fn wait_for_reader(lock: &std::fs::File) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match lock.try_lock() {
            Err(std::fs::TryLockError::WouldBlock) => return,
            Ok(()) => lock.unlock().unwrap(),
            Err(error) => panic!("{error}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "reader did not acquire its lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn revisions_keep_their_data_and_do_not_share_write_locks() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    let first_sha = git(&repo.path, &["rev-parse", "HEAD"]);
    assert!(orbit_index(&repo.path, data_dir.path()));
    let manifest = orbit_sql("SELECT database_path FROM _orbit_manifest", data_dir.path());
    let first_path = std::path::PathBuf::from(manifest[0]["database_path"].as_str().unwrap());
    let first = duckdb_client::DuckDbClient::open(&first_path).unwrap();
    let first_files = first.query_arrow("SELECT count(*) FROM gl_file").unwrap();
    assert!(first.query_arrow("SELECT * FROM _orbit_manifest").is_err());

    std::fs::write(repo.path.join("new.py"), "def second_revision(): pass\n").unwrap();
    git(&repo.path, &["add", "-A"]);
    git(&repo.path, &["commit", "-m", "second revision"]);
    let second_sha = git(&repo.path, &["rev-parse", "HEAD"]);
    assert!(orbit_index(&repo.path, data_dir.path()));

    let (listed, stderr, ok) = run_cmd(&["workspace", "list", "-F", "json"], data_dir.path());
    assert!(ok, "{stderr}");
    let listed: Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(rows(&listed).len(), 2);
    let first_row = rows_where(&listed, "commit_sha", &first_sha)[0];
    let second_row = rows_where(&listed, "commit_sha", &second_sha)[0];
    assert_ne!(first_row["database_path"], second_row["database_path"]);
    assert_eq!(first_row["status"], "indexed");
    assert_eq!(second_row["status"], "indexed");
    assert_eq!(
        duckdb_client::scalar_i64(&first_files),
        duckdb_client::scalar_i64(&first.query_arrow("SELECT count(*) FROM gl_file").unwrap())
    );

    let query_commits = |scoped: bool| {
        let mut command = orbit_cmd();
        command.args([
            "workspace",
            "sql",
            "-F",
            "json",
            "SELECT DISTINCT commit_sha FROM gl_file",
        ]);
        if scoped {
            command.args(["--repo", repo.path.to_str().unwrap()]);
        }
        let output = command
            .current_dir(&repo.path)
            .env("ORBIT_DATA_DIR", data_dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    assert_eq!(query_commits(true), json!([{ "commit_sha": second_sha }]));
    drop(first);

    let mismatch = orbit_cmd()
        .args([
            "workspace",
            "sql",
            "--repo",
            repo.path.to_str().unwrap(),
            "--db",
            first_path.to_str().unwrap(),
            FILES_SIMPLE,
        ])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    assert!(mismatch.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&mismatch.stderr).contains("does not match the checkout revision")
    );

    let manifest_db =
        duckdb_client::DuckDbClient::open_read_only(&data_dir.path().join("local/manifest.duckdb"))
            .unwrap();
    assert!(manifest_db.query_arrow("SELECT * FROM gl_file").is_err());
    drop(manifest_db);
    let all = orbit_sql("SELECT DISTINCT commit_sha FROM gl_file", data_dir.path());
    assert_eq!(rows(&all).len(), 2);
    git(&repo.path, &["checkout", "--detach", &first_sha]);
    assert_eq!(rows(&query_commits(false)).len(), 2);
    assert_eq!(query_commits(true), json!([{ "commit_sha": first_sha }]));
}

#[test]
fn revision_read_locks_block_writers_and_release_when_reader_exits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repo = create_test_repo();
    assert!(orbit_index(&repo.path, data_dir.path()));
    let manifest = orbit_sql("SELECT database_path FROM _orbit_manifest", data_dir.path());
    let path = manifest[0]["database_path"].as_str().unwrap();
    let lock = std::fs::File::options()
        .read(true)
        .write(true)
        .open(format!("{path}.lock"))
        .unwrap();
    let mut reader = orbit_cmd()
        .args([
            "workspace",
            "sql",
            "SELECT sum(sin(i)) FROM range(1000000000000) t(i)",
        ])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_reader(&lock);
    assert!(!orbit_index(&repo.path, data_dir.path()));
    assert_eq!(rows(&orbit_sql(FILES_SIMPLE, data_dir.path())).len(), 2);
    let (_, stderr, ok) = run_cmd(&["workspace", "list", "-F", "json"], data_dir.path());
    assert!(ok, "{stderr}");
    reader.kill().unwrap();
    reader.wait().unwrap();
    lock.try_lock().expect("reader crash releases shared locks");
    lock.unlock().unwrap();
    assert!(orbit_index(&repo.path, data_dir.path()));
}

#[test]
fn busy_revision_fails_the_whole_query_and_releases_prior_locks() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repos = [create_test_repo(), create_test_repo()];
    for repo in &repos {
        assert!(orbit_index(&repo.path, data_dir.path()));
    }
    let manifest = orbit_sql(
        "SELECT database_path FROM _orbit_manifest ORDER BY database_path",
        data_dir.path(),
    );
    let locks: Vec<_> = rows(&manifest)
        .iter()
        .map(|row| {
            std::fs::File::options()
                .read(true)
                .write(true)
                .open(format!("{}.lock", row["database_path"].as_str().unwrap()))
                .unwrap()
        })
        .collect();
    locks[1].lock().unwrap();
    let (stdout, stderr, ok) = run_cmd(
        &["workspace", "sql", "-F", "json", FILES_SIMPLE],
        data_dir.path(),
    );
    assert!(!ok);
    assert!(stdout.is_empty(), "must not return partial rows");
    assert!(stderr.contains("is busy; retry"), "{stderr}");
    locks[0]
        .try_lock()
        .expect("failed reader releases earlier shared locks");
    locks[0].unlock().unwrap();
    let reader = orbit_cmd()
        .args(["workspace", "sql", "-F", "json", FILES_SIMPLE])
        .env("ORBIT_DATA_DIR", data_dir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_reader(&locks[0]);
    let (_, stderr, ok) = run_cmd(&["workspace", "list", "-F", "json"], data_dir.path());
    assert!(
        ok,
        "waiting for a revision must not hold the manifest lock: {stderr}"
    );
    locks[1].unlock().unwrap();
    let output = reader.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        rows(&serde_json::from_slice::<Value>(&output.stdout).unwrap()).len(),
        4
    );
    assert_eq!(rows(&orbit_sql(FILES_SIMPLE, data_dir.path())).len(), 4);
}

#[test]
fn fresh_repositories_index_concurrently_into_separate_databases() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let repos = [create_test_repo(), create_test_repo()];
    let children: Vec<_> = repos
        .iter()
        .map(|repo| {
            orbit_cmd()
                .args(["workspace", "index", repo.path.to_str().unwrap()])
                .env("ORBIT_DATA_DIR", data_dir.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let manifest = orbit_sql(
        "SELECT DISTINCT database_path FROM _orbit_manifest WHERE status = 'indexed'",
        data_dir.path(),
    );
    assert_eq!(rows(&manifest).len(), 2);
}
