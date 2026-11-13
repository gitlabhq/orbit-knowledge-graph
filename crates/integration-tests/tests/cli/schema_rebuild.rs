use std::path::{Path, PathBuf};

use integration_testkit::cli::{create_test_repo, orbit_cmd, orbit_sql};
use serde_json::{Value, json};

const DEFINITIONS: &str = "SELECT count(*) AS definitions FROM gl_definition";

fn run(repo: &Path, data: &Path, args: &[&str]) -> String {
    let output = orbit_cmd()
        .args(args)
        .current_dir(repo)
        .env("ORBIT_DATA_DIR", data)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn revision_database(data: &Path) -> PathBuf {
    let manifest = duckdb_client::DuckDbClient::open(duckdb_client::OpenMode::ReadOnly(
        &data.join("local/manifest.duckdb"),
    ))
    .unwrap();
    let batches = manifest
        .query_arrow_json("SELECT database_path FROM _orbit_manifest", &[])
        .unwrap();
    let paths = duckdb_client::string_column(&batches, "database_path");
    assert_eq!(paths.len(), 1, "{paths:?}");
    PathBuf::from(&paths[0])
}

/// Makes the stored fingerprint differ from the binary's, as an `orbit`
/// upgrade that changes the local schema or the code index revision does.
fn age_stored_fingerprint(database: &Path, key: &str) {
    let graph = duckdb_client::DuckDbClient::open(database).unwrap();
    graph
        .execute(
            "UPDATE _orbit_meta SET value = 'outdated' WHERE key = ?1",
            &[json!(key)],
        )
        .unwrap();
}

#[test]
fn a_schema_rebuild_at_the_same_commit_reindexes_the_whole_repository() {
    let commands: [&[&str]; 3] = [
        &["workspace", "index", "."],
        &["context", "src/main.py"],
        &["grep", "hello"],
    ];
    for key in ["local_ddl", "code_index_revision"] {
        for command in commands {
            let data = tempfile::TempDir::new().unwrap();
            let repo = create_test_repo();
            run(&repo.path, data.path(), &["workspace", "index", "."]);
            let indexed = orbit_sql(DEFINITIONS, data.path());
            assert_ne!(indexed, json!([{"definitions": 0}]));

            age_stored_fingerprint(&revision_database(data.path()), key);
            run(&repo.path, data.path(), command);

            assert_eq!(
                orbit_sql(DEFINITIONS, data.path()),
                indexed,
                "{key} changed, then {command:?}"
            );
            let listed: Value = serde_json::from_str(&run(
                &repo.path,
                data.path(),
                &["workspace", "list", "-F", "json"],
            ))
            .unwrap();
            assert_eq!(listed[0]["status"], "indexed", "{key}, {command:?}");
        }
    }
}
