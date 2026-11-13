//! The `integration.jira` package on a DuckDB connection over a local S3 server, in both
//! location modes: Parquet files under a prefix, and a DuckLake catalog with its data on S3.
//! These run without `--all-features`: the bundled DuckDB build carries a dev version id, so it
//! refuses the official httpfs and ducklake extension files.

use std::path::Path;

use duckdb_client::DuckDbClient;
use integration_testkit::lake::{Lake, Mode};
use integration_testkit::query_scenario::{load_yaml_seed, run_lake_dir, seed_preset};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext};
use orbit_server_config::DuckDbLocation;
use query_data_model::{Catalog, DuckDb};

const SHAPE: &str = "jira";
const CONNECTION: &str = "jira_lake";
const SEED: &str = "jira";
const SCENARIOS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/server/lake/scenarios");
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/presets"
);

async fn scenarios(mode: Mode) {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    load_yaml_seed(&ctx, PRESETS, "data_correctness").await;
    ctx.optimize_if_needed().await;
    run_lake_dir(
        &ctx,
        (SCENARIOS, PRESETS),
        query_data_model::testkit::with_demo(),
        (SHAPE, CONNECTION, SEED),
        mode,
    )
    .await;
}

#[tokio::test]
async fn jira_parquet_scenarios() {
    scenarios(Mode::Parquet).await;
}

#[tokio::test]
async fn jira_ducklake_scenarios() {
    scenarios(Mode::Ducklake).await;
}

fn catalog() -> Catalog<DuckDb> {
    let model = query_data_model::testkit::with_demo()
        .deployed(&query_data_model::testkit::deployments(), SHAPE)
        .unwrap();
    model.physical::<DuckDb>(CONNECTION).unwrap()
}

async fn seeded(mode: Mode, storage_type: impl Fn(&str, &str, &str) -> String) -> Lake {
    let seed = seed_preset(Path::new(PRESETS), SEED);
    Lake::seed(mode, &catalog(), &seed, storage_type).await
}

async fn declared(mode: Mode) -> Lake {
    seeded(mode, |_, _, declared| declared.to_string()).await
}

fn open(lake: &Lake) -> Result<DuckDbClient, String> {
    DuckDbClient::open_s3(CONNECTION, &lake.connection, &catalog())
        .map_err(|error| error.to_string())
}

async fn rejects_another_column_type(mode: Mode) {
    let lake = seeded(mode, |table, column, declared| {
        match (table, column) {
            ("jira_issue", "merge_request_id") => "VARCHAR",
            _ => declared,
        }
        .to_string()
    })
    .await;
    assert_eq!(
        open(&lake).err().unwrap(),
        "connection jira_lake table jira_issue stores merge_request_id as VARCHAR, but the package declares BIGINT",
        "{mode:?}"
    );
}

#[tokio::test]
async fn jira_parquet_rejects_a_column_of_another_type() {
    rejects_another_column_type(Mode::Parquet).await;
}

#[tokio::test]
async fn jira_ducklake_rejects_a_column_of_another_type() {
    rejects_another_column_type(Mode::Ducklake).await;
}

#[tokio::test]
async fn jira_parquet_rejects_a_prefix_without_the_tables() {
    let mut lake = declared(Mode::Parquet).await;
    lake.connection.location = DuckDbLocation::Parquet {
        bucket: integration_testkit::s3::BUCKET.into(),
        prefix: "elsewhere/".into(),
    };
    let error = open(&lake).err().unwrap();
    assert!(
        error.starts_with("connection jira_lake cannot resolve table jira_gitlab_ref: ")
            && error.contains("No files found"),
        "{error}"
    );
}

#[tokio::test]
async fn jira_ducklake_rejects_another_data_path() {
    let mut lake = declared(Mode::Ducklake).await;
    let DuckDbLocation::Ducklake { prefix, .. } = &mut lake.connection.location else {
        unreachable!()
    };
    *prefix = "elsewhere/".into();
    let error = open(&lake).err().unwrap();
    assert!(
        error.starts_with("connection jira_lake cannot attach the DuckLake catalog: ")
            && error.contains("does not match existing data path"),
        "{error}"
    );
}

#[tokio::test]
async fn jira_a_refused_secret_never_shows_in_the_error() {
    let mut lake = declared(Mode::Parquet).await;
    let refused = format!("refused-{}", lake.connection.credentials.secret_access_key);
    lake.connection.credentials.secret_access_key = refused.clone();
    let error = open(&lake).err().unwrap();
    assert!(
        error.starts_with("connection jira_lake cannot resolve table jira_gitlab_ref: ")
            && error.contains("403"),
        "{error}"
    );
    assert!(!error.contains(&refused), "{error}");
    assert!(
        !error.contains(&lake.s3.credentials.secret_access_key),
        "{error}"
    );
}
