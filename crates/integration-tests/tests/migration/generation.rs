//! Generated additive migrations must actually apply in ClickHouse and bring a
//! baseline schema to the ontology's desired schema.

use clickhouse_client::FromArrowColumn;
use compiler::ddl::{Codec, ColumnDef, ColumnType, CreateTable, Engine, IndexDef, IndexType};
use compiler::emit_create_table;
use integration_testkit::TestContext;
use migration_framework::generation::{diff_schemas, render_down, render_up};

fn merge_tree() -> Engine {
    Engine {
        name: "MergeTree".into(),
        args: vec![],
    }
}

fn table(name: &str, columns: Vec<ColumnDef>, indexes: Vec<IndexDef>) -> CreateTable {
    CreateTable {
        name: name.into(),
        columns,
        indexes,
        projections: vec![],
        engine: merge_tree(),
        order_by: vec!["id".into()],
        primary_key: None,
        settings: vec![],
    }
}

fn id_index() -> IndexDef {
    IndexDef {
        name: "idx_id".into(),
        expression: "id".into(),
        index_type: IndexType::MinMax,
        granularity: 1,
    }
}

async fn column_names(ctx: &TestContext, table: &str) -> Vec<String> {
    let batches = ctx
        .query(&format!(
            "SELECT name FROM system.columns WHERE table = '{table}' ORDER BY name"
        ))
        .await;
    String::extract_column(&batches, 0).unwrap()
}

async fn index_names(ctx: &TestContext, table: &str) -> Vec<String> {
    let batches = ctx
        .query(&format!(
            "SELECT name FROM system.data_skipping_indices WHERE table = '{table}' ORDER BY name"
        ))
        .await;
    String::extract_column(&batches, 0).unwrap()
}

async fn table_exists(ctx: &TestContext, table: &str) -> bool {
    let batches = ctx
        .query(&format!(
            "SELECT name FROM system.tables WHERE name = '{table}'"
        ))
        .await;
    !String::extract_column(&batches, 0).unwrap().is_empty()
}

async fn column_codec(ctx: &TestContext, table: &str, column: &str) -> String {
    let batches = ctx
        .query(&format!(
            "SELECT compression_codec FROM system.columns \
             WHERE table = '{table}' AND name = '{column}'"
        ))
        .await;
    String::extract_column(&batches, 0)
        .unwrap()
        .into_iter()
        .next()
        .unwrap_or_default()
}

#[tokio::test]
async fn generated_additive_migration_converges_baseline_to_desired() {
    let desired = vec![
        table(
            "mig_user",
            vec![
                ColumnDef::new("id", ColumnType::Int64),
                ColumnDef::new("bio", ColumnType::String).with_default("''"),
            ],
            vec![id_index()],
        ),
        table(
            "mig_issue",
            vec![ColumnDef::new("id", ColumnType::Int64)],
            vec![],
        ),
    ];

    // mig_user is missing the `bio` column and `idx_id` index, and mig_issue
    // does not exist — so the diff must ADD COLUMN, ADD INDEX, and CREATE TABLE.
    let baseline = vec![table(
        "mig_user",
        vec![ColumnDef::new("id", ColumnType::Int64)],
        vec![],
    )];

    let ctx = TestContext::new(&[]).await;
    for table in &baseline {
        ctx.execute(&emit_create_table(table)).await;
    }

    let diff = diff_schemas(&baseline, &desired).expect("drift is additive");
    for statement in render_up(&diff) {
        ctx.execute(&statement).await;
    }

    assert!(
        column_names(&ctx, "mig_user")
            .await
            .contains(&"bio".to_string())
    );
    assert!(
        index_names(&ctx, "mig_user")
            .await
            .contains(&"idx_id".to_string())
    );
    assert!(table_exists(&ctx, "mig_issue").await);
}

#[tokio::test]
async fn generated_in_place_alters_apply_and_revert() {
    let drop_index = IndexDef {
        name: "idx_old".into(),
        ..id_index()
    };

    // Baseline: `id` carries one codec and an index the ontology no longer
    // declares. The diff must be a metadata-only MODIFY COLUMN (codec swap) plus
    // a DROP INDEX, neither of which rewrites row data. A codec→codec swap is
    // chosen because it is cleanly invertible — `MODIFY COLUMN` overrides a codec
    // but does not REMOVE one, so reverting an *added* codec is not symmetric.
    let baseline = vec![table(
        "mig_acct",
        vec![ColumnDef::new("id", ColumnType::Int64).with_codec(vec![Codec::LZ4])],
        vec![drop_index],
    )];
    let desired = vec![table(
        "mig_acct",
        vec![ColumnDef::new("id", ColumnType::Int64).with_codec(vec![Codec::ZSTD(1)])],
        vec![],
    )];

    let ctx = TestContext::new(&[]).await;
    for table in &baseline {
        ctx.execute(&emit_create_table(table)).await;
    }

    let diff = diff_schemas(&baseline, &desired).expect("codec swap + index drop are in place");
    for statement in render_up(&diff) {
        ctx.execute(&statement).await;
    }

    assert!(column_codec(&ctx, "mig_acct", "id").await.contains("ZSTD"));
    assert!(
        !index_names(&ctx, "mig_acct")
            .await
            .contains(&"idx_old".to_string())
    );

    for statement in render_down(&diff) {
        ctx.execute(&statement).await;
    }

    assert!(column_codec(&ctx, "mig_acct", "id").await.contains("LZ4"));
    assert!(
        index_names(&ctx, "mig_acct")
            .await
            .contains(&"idx_old".to_string())
    );
}
