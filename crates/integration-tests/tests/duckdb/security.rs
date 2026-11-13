use std::collections::BTreeSet;
use std::path::Path;

use duckdb_client::{DuckDbClient, OpenMode, string_column};

const LOCKED_SETTINGS: &[(&str, &str)] = &[
    ("allow_community_extensions", "false"),
    ("allow_persistent_secrets", "false"),
    ("allow_unsigned_extensions", "false"),
    ("autoinstall_known_extensions", "false"),
    ("autoload_known_extensions", "false"),
    ("debug_force_mbedtls_unsafe", "false"),
    ("enable_external_access", "false"),
    ("lock_configuration", "true"),
    ("temp_file_encryption", "false"),
];

const REJECTED_STATEMENTS: &[&str] = &[
    "INSTALL httpfs",
    "LOAD httpfs",
    "SET allow_unsigned_extensions = true",
    "SET enable_external_access = true",
    "SET force_mbedtls_unsafe = true",
    "SET temp_file_encryption = true",
];

const REVIEWED_SECURITY_SETTINGS: &[&str] = &[
    "active_grammar_extensions",
    "allow_community_extensions",
    "allow_extension_repositories",
    "allow_extensions_metadata_mismatch",
    "allow_parser_override_extension",
    "allow_persistent_secrets",
    "allow_unredacted_secrets",
    "allow_unsigned_extensions",
    "allowed_configs",
    "allowed_directories",
    "allowed_paths",
    "autoinstall_extension_repository",
    "autoinstall_known_extensions",
    "autoload_known_extensions",
    "custom_extension_repository",
    "default_secret_storage",
    "enable_external_access",
    "enable_external_file_cache",
    "enable_http_metadata_cache",
    "extension_directories",
    "extension_repository_directory",
    "external_file_cache_local_max_block_size",
    "external_file_cache_remote_max_block_size",
    "external_file_cache_remote_min_block_size",
    "external_file_cache_request_sizing",
    "external_file_cache_spill",
    "external_threads",
    "http_client_pool_capacity",
    "http_proxy",
    "http_proxy_password",
    "http_proxy_username",
    "redact_http_logs",
    "secret_directory",
    "temp_file_encryption",
    "validate_external_file_cache",
];

fn every_open_mode(dir: &Path) -> Vec<(&'static str, DuckDbClient)> {
    let file = dir.join("graph.duckdb");
    drop(DuckDbClient::open(&file).unwrap());
    vec![
        ("ReadWrite", DuckDbClient::open(&file).unwrap()),
        (
            "ReadOnly",
            DuckDbClient::open(OpenMode::ReadOnly(&dir.join("graph.duckdb"))).unwrap(),
        ),
        ("InMemory", DuckDbClient::open(OpenMode::InMemory).unwrap()),
        (
            "InMemoryAttaching",
            DuckDbClient::open(OpenMode::InMemoryAttaching(dir)).unwrap(),
        ),
    ]
}

fn strings(client: &DuckDbClient, sql: &str, params: &[serde_json::Value]) -> Vec<String> {
    string_column(&client.query_arrow_json(sql, params).unwrap(), "value")
}

#[test]
fn every_open_mode_locks_the_security_settings() {
    let dir = tempfile::TempDir::new().unwrap();
    for (mode, client) in every_open_mode(dir.path()) {
        for (setting, expected) in LOCKED_SETTINGS {
            let value = strings(
                &client,
                "SELECT current_setting(?)::VARCHAR AS value",
                &[(*setting).into()],
            );
            assert_eq!(value, [*expected], "{mode}: {setting}");
        }
        for sql in REJECTED_STATEMENTS {
            let error = client.execute(sql, &[]).unwrap_err().to_string();
            assert!(
                [
                    "disabled by configuration",
                    "disabled through configuration",
                    "configuration has been locked"
                ]
                .iter()
                .any(|reason| error.contains(reason)),
                "{mode}: {sql}: {error}"
            );
        }
    }
}

#[test]
fn no_open_mode_can_reach_installed_extensions() {
    let dir = tempfile::TempDir::new().unwrap();
    let extension = dir.path().join("httpfs.duckdb_extension");
    std::fs::write(&extension, b"not an extension").unwrap();
    for (mode, client) in every_open_mode(dir.path()) {
        for sql in [
            "SELECT * FROM duckdb_extensions()".to_string(),
            format!("LOAD '{}'", extension.display()),
        ] {
            let error = client.execute(&sql, &[]).unwrap_err().to_string();
            assert!(
                error.contains("disabled by configuration")
                    || error.contains("disabled through configuration"),
                "{mode}: {sql}: {error}"
            );
        }
    }
}

#[test]
fn security_settings_match_the_reviewed_list() {
    let client = DuckDbClient::open(OpenMode::InMemory).unwrap();
    let found: BTreeSet<String> = strings(
        &client,
        "SELECT name AS value FROM duckdb_settings() WHERE regexp_matches(name, \
         'extension|external|secret|http|(^|_)(s3|ssl|tls)(_|$)|encrypt|mbedtls|crypto|unsigned|allowed_|repository|certificate|proxy')",
        &[],
    )
    .into_iter()
    .collect();
    let reviewed: BTreeSet<String> = REVIEWED_SECURITY_SETTINGS
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        found, reviewed,
        "DuckDB's security-relevant settings changed; review the new ones and update the list"
    );
}
