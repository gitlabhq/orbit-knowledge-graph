//! Integration tests for automated column statistics collection.
//!
//! Verifies that AggregatingMergeTree MVs fire on INSERT into node tables
//! and populate the stats tables with correct value frequencies.

use arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use query_engine::compiler::{
    DictionarySource, emit_create_dictionary, generate_statistics_ddl_with_prefix,
};

fn string_col(batch: &RecordBatch, idx: usize) -> Vec<&str> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i)).collect()
}

fn u64_col(batch: &RecordBatch, idx: usize) -> Vec<u64> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..arr.len()).map(|i| arr.value(i)).collect()
}

async fn query_stats(
    ctx: &TestContext,
    stats_table: &str,
    table_name: &str,
    column_name: &str,
    partition_key: &str,
    value_col: &str,
) -> RecordBatch {
    let batches = ctx
        .query(&format!(
            "SELECT {value_col}, uniqMerge(row_count) AS cnt \
             FROM {stats_table} \
             WHERE table_name = '{table_name}' \
               AND column_name = '{column_name}' \
               AND partition_key = '{partition_key}' \
             GROUP BY {value_col} \
             ORDER BY {value_col}"
        ))
        .await;
    assert!(!batches.is_empty(), "expected rows for {column_name}");
    batches.into_iter().next().unwrap()
}

const MR_INSERT_COLS: &str = "id, iid, title, state, source_branch, target_branch, \
     draft, squash, discussion_locked, first_contribution, \
     merge_status, project_id, traversal_path, _version, _deleted";

#[tokio::test]
async fn categorical_stats_mv_tracks_value_frequencies() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} ({MR_INSERT_COLS}) VALUES
         (9001, 1, 'MR Alpha',   'opened', 'feat-a', 'main', false, false, false, false, 'can_be_merged', 1000, '1/100/1000/', 1, false),
         (9002, 2, 'MR Beta',    'opened', 'feat-b', 'main', true,  false, false, false, 'can_be_merged', 1000, '1/100/1000/', 1, false),
         (9003, 3, 'MR Gamma',   'merged', 'feat-c', 'main', false, true,  false, true,  'merged',        1000, '1/100/1000/', 1, false)",
        t("gl_merge_request")
    )).await;
    ctx.optimize_all().await;

    let mr = t("gl_merge_request");
    let stats = t("gkg_column_stats");

    let batch = query_stats(&ctx, &stats, &mr, "state", "1/100/1000/", "value").await;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(string_col(&batch, 0), vec!["merged", "opened"]);
    assert_eq!(u64_col(&batch, 1), vec![1, 2]);

    let batch = query_stats(&ctx, &stats, &mr, "draft", "1/100/1000/", "value").await;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(string_col(&batch, 0), vec!["false", "true"]);
    assert_eq!(u64_col(&batch, 1), vec![2, 1]);
}

#[tokio::test]
async fn histogram_stats_mv_tracks_continuous_columns() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} ({MR_INSERT_COLS}, author_id) VALUES
         (8001, 10, 'MR One',   'opened', 'a', 'main', false, false, false, false, '', 2000, '1/200/', 1, false, 100),
         (8002, 11, 'MR Two',   'opened', 'b', 'main', false, false, false, false, '', 2000, '1/200/', 1, false, 100),
         (8003, 12, 'MR Three', 'merged', 'c', 'main', false, false, false, false, '', 2000, '1/200/', 1, false, 200)",
        t("gl_merge_request")
    )).await;
    ctx.optimize_all().await;

    let mr = t("gl_merge_request");
    let hist = t("gkg_histogram_stats");

    let batch = query_stats(&ctx, &hist, &mr, "author_id", "1/200/", "value").await;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(string_col(&batch, 0), vec!["100", "200"]);
    assert_eq!(u64_col(&batch, 1), vec![2, 1]);
}

#[tokio::test]
async fn token_stats_mv_tracks_text_columns() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} ({MR_INSERT_COLS}) VALUES
         (7001, 20, 'Fix login bug',    'opened', 'fix-login', 'main',    false, false, false, false, '', 3000, '1/300/', 1, false),
         (7002, 21, 'Add dark mode',    'opened', 'feat-dark', 'main',    false, false, false, false, '', 3000, '1/300/', 1, false),
         (7003, 22, 'Refactor auth',    'merged', 'fix-login', 'develop', false, false, false, false, '', 3000, '1/300/', 1, false)",
        t("gl_merge_request")
    )).await;
    ctx.optimize_all().await;

    let mr = t("gl_merge_request");
    let tokens = t("gkg_token_stats");

    let batch = query_stats(&ctx, &tokens, &mr, "source_branch", "1/300/", "token").await;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(string_col(&batch, 0), vec!["feat-dark", "fix-login"]);
    assert_eq!(u64_col(&batch, 1), vec![1, 2]);
}

#[tokio::test]
async fn stats_partitioned_by_traversal_path() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} ({MR_INSERT_COLS}) VALUES
         (6001, 30, 'MR in ns A', 'opened', 'a', 'main', false, false, false, false, '', 4000, '1/400/', 1, false),
         (6002, 31, 'MR in ns B', 'opened', 'b', 'main', false, false, false, false, '', 5000, '1/500/', 1, false),
         (6003, 32, 'MR in ns A', 'merged', 'c', 'main', false, false, false, false, '', 4000, '1/400/', 1, false)",
        t("gl_merge_request")
    )).await;
    ctx.optimize_all().await;

    let mr = t("gl_merge_request");
    let stats = t("gkg_column_stats");

    let batch_a = query_stats(&ctx, &stats, &mr, "state", "1/400/", "value").await;
    assert_eq!(batch_a.num_rows(), 2);

    let batch_b = query_stats(&ctx, &stats, &mr, "state", "1/500/", "value").await;
    assert_eq!(batch_b.num_rows(), 1);
    assert_eq!(string_col(&batch_b, 0), vec!["opened"]);
    assert_eq!(u64_col(&batch_b, 1), vec![1]);
}

#[tokio::test]
async fn stats_dictionary_is_queryable() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    let ontology = ontology::Ontology::load_embedded().unwrap();
    let prefix = integration_testkit::TABLE_PREFIX.as_str();
    if let Some(stats) = generate_statistics_ddl_with_prefix(&ontology, prefix) {
        let source = DictionarySource {
            database: &ctx.config.database,
            user: &ctx.config.username,
            password: ctx.config.password.as_deref(),
        };
        for d in &stats.dictionaries {
            ctx.execute(&emit_create_dictionary(d, &source)).await;
        }
    }

    ctx.execute(&format!(
        "INSERT INTO {} ({MR_INSERT_COLS}) VALUES
         (5001, 40, 'MR One',   'opened', 'a', 'main', false, false, false, false, '', 6000, '1/600/', 1, false),
         (5002, 41, 'MR Two',   'merged', 'b', 'main', false, false, false, false, '', 6000, '1/600/', 1, false),
         (5003, 42, 'MR Three', 'opened', 'c', 'main', false, false, false, false, '', 6000, '1/600/', 1, false)",
        t("gl_merge_request")
    )).await;
    ctx.optimize_all().await;

    let dict_name = t("gkg_column_stats_dict");
    ctx.execute(&format!("SYSTEM RELOAD DICTIONARY {dict_name}"))
        .await;

    let mr = t("gl_merge_request");
    let batches = ctx
        .query(&format!(
            "SELECT dictGet('{dict_name}', 'row_count', \
             ('{mr}', 'state', '1/600/', 'opened')) AS cnt"
        ))
        .await;

    assert!(!batches.is_empty());
    assert_eq!(u64_col(&batches[0], 0), vec![2]);
}
