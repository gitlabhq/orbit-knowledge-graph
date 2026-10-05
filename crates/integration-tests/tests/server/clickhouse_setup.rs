use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use integration_testkit::TestContext;
use orbit_server::clickhouse_setup;
use orbit_server_config::{AppConfig, ClickHouseConfiguration, ClickHouseSetupConfig};

const GRAPH_DB: &str = "orbit";
const DATALAKE_DB: &str = "datalake";

struct Passwords {
    writer: &'static str,
    reader: &'static str,
    siphon_reader: &'static str,
}

const SPECIAL_CHARACTER_PASSWORDS: Passwords = Passwords {
    writer: r"w'ri?ter\p`ass;--",
    reader: "re?ader'); DROP USER default; --",
    siphon_reader: "siphon ${GRAPH_DB} ?? '",
};

fn setup_config(ctx: &TestContext, passwords: &Passwords) -> AppConfig {
    let mut config = AppConfig::embedded_defaults();
    config.graph = ClickHouseConfiguration {
        database: GRAPH_DB.to_string(),
        username: "gkg_writer".to_string(),
        password: None,
        ..ctx.config.clone()
    };
    config.datalake = ClickHouseConfiguration {
        database: DATALAKE_DB.to_string(),
        ..ctx.config.clone()
    };
    config.clickhouse_setup = ClickHouseSetupConfig {
        admin_username: ctx.config.username.clone(),
        admin_password: ctx.config.password.clone(),
        writer_password: Some(passwords.writer.to_string()),
        reader_password: Some(passwords.reader.to_string()),
        siphon_reader_password: Some(passwords.siphon_reader.to_string()),
    };
    config
}

fn client_as(
    ctx: &TestContext,
    database: &str,
    user: &str,
    password: &str,
) -> ArrowClickHouseClient {
    ClickHouseConfiguration {
        database: database.to_string(),
        username: user.to_string(),
        password: Some(password.to_string()),
        ..ctx.config.clone()
    }
    .build_client()
}

async fn new_context() -> TestContext {
    let ctx = TestContext::new(&[]).await;
    ctx.execute(&format!("CREATE DATABASE `{DATALAKE_DB}`"))
        .await;
    ctx.execute(&format!(
        "CREATE TABLE `{DATALAKE_DB}`.siphon_users (id UInt64) ENGINE = MergeTree ORDER BY id"
    ))
    .await;
    ctx
}

#[tokio::test]
async fn clickhouse_setup_creates_identities_with_the_contract_privileges() {
    let ctx = new_context().await;

    clickhouse_setup::run(&setup_config(&ctx, &SPECIAL_CHARACTER_PASSWORDS))
        .await
        .expect("setup should apply the contract");

    let writer = client_as(
        &ctx,
        GRAPH_DB,
        "gkg_writer",
        SPECIAL_CHARACTER_PASSWORDS.writer,
    );
    writer
        .execute("CREATE TABLE nodes (id UInt64) ENGINE = MergeTree ORDER BY id")
        .await
        .expect("writer creates graph tables");
    writer
        .execute("INSERT INTO nodes VALUES (1)")
        .await
        .expect("writer inserts into the graph");

    let reader = client_as(
        &ctx,
        GRAPH_DB,
        "gkg_reader",
        SPECIAL_CHARACTER_PASSWORDS.reader,
    );
    reader
        .execute("SELECT * FROM nodes")
        .await
        .expect("reader reads the graph");
    assert!(
        reader
            .execute("INSERT INTO nodes VALUES (2)")
            .await
            .is_err(),
        "reader must not write the graph"
    );

    let siphon_reader = client_as(
        &ctx,
        DATALAKE_DB,
        "gkg_siphon_reader",
        SPECIAL_CHARACTER_PASSWORDS.siphon_reader,
    );
    siphon_reader
        .execute("SELECT * FROM siphon_users")
        .await
        .expect("siphon reader reads the data lake");
    assert!(
        siphon_reader
            .execute(&format!("SELECT * FROM `{GRAPH_DB}`.nodes"))
            .await
            .is_err(),
        "siphon reader must not read the graph"
    );

    let admin = client_as(
        &ctx,
        "default",
        &ctx.config.username,
        ctx.config.password.as_deref().unwrap(),
    );
    admin
        .execute("SELECT 1")
        .await
        .expect("no password text runs as SQL");
}

#[tokio::test]
async fn clickhouse_setup_reruns_and_rotates_passwords() {
    let ctx = new_context().await;
    let rotated = Passwords {
        writer: "writer-rotated",
        reader: "reader-rotated",
        siphon_reader: "siphon-rotated",
    };

    clickhouse_setup::run(&setup_config(&ctx, &SPECIAL_CHARACTER_PASSWORDS))
        .await
        .expect("first run");
    clickhouse_setup::run(&setup_config(&ctx, &rotated))
        .await
        .expect("second run");

    let rotated_writer = client_as(&ctx, GRAPH_DB, "gkg_writer", rotated.writer);
    rotated_writer
        .execute("SELECT 1")
        .await
        .expect("rotated password logs in");
    let old_writer = client_as(
        &ctx,
        GRAPH_DB,
        "gkg_writer",
        SPECIAL_CHARACTER_PASSWORDS.writer,
    );
    assert!(
        old_writer.execute("SELECT 1").await.is_err(),
        "old password must stop working"
    );
}
