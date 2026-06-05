//! Integration tests for automated column statistics collection.
//!
//! Verifies that AggregatingMergeTree MVs fire on INSERT into node tables
//! and populate the stats tables with correct value frequencies.

use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use query_engine::compiler::{
    DictionarySource, emit_create_dictionary, generate_statistics_ddl_with_prefix,
};

#[tokio::test]
async fn categorical_stats_mv_tracks_value_frequencies() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, \
         draft, squash, discussion_locked, first_contribution, \
         merge_status, project_id, traversal_path, _version, _deleted) VALUES
         (9001, 1, 'MR Alpha',   'opened', 'feat-a', 'main', false, false, false, false, 'can_be_merged', 1000, '1/100/1000/', 1, false),
         (9002, 2, 'MR Beta',    'opened', 'feat-b', 'main', true,  false, false, false, 'can_be_merged', 1000, '1/100/1000/', 1, false),
         (9003, 3, 'MR Gamma',   'merged', 'feat-c', 'main', false, true,  false, true,  'merged',        1000, '1/100/1000/', 1, false)",
        t("gl_merge_request")
    )).await;

    ctx.optimize_all().await;

    let mr_table = t("gl_merge_request");
    let stats_table = t("gkg_column_stats");

    // state: 2 opened, 1 merged
    let batches = ctx.query(&format!(
        "SELECT value, uniqMerge(row_count) AS cnt \
         FROM {stats_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'state' \
           AND partition_key = '1/100/1000/' \
         GROUP BY value \
         ORDER BY value"
    )).await;

    assert!(!batches.is_empty(), "stats table should have rows for state");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "expected 2 distinct state values");

    let values: Vec<&str> = (0..batch.num_rows())
        .map(|i| batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(i))
        .collect();
    let counts: Vec<u64> = (0..batch.num_rows())
        .map(|i| batch.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap().value(i))
        .collect();

    assert_eq!(values, vec!["merged", "opened"]);
    assert_eq!(counts, vec![1, 2]);

    // draft: 2 false, 1 true
    let draft_batches = ctx.query(&format!(
        "SELECT value, uniqMerge(row_count) AS cnt \
         FROM {stats_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'draft' \
           AND partition_key = '1/100/1000/' \
         GROUP BY value \
         ORDER BY value"
    )).await;

    let db = &draft_batches[0];
    assert_eq!(db.num_rows(), 2);
    let draft_values: Vec<&str> = (0..db.num_rows())
        .map(|i| db.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(i))
        .collect();
    let draft_counts: Vec<u64> = (0..db.num_rows())
        .map(|i| db.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap().value(i))
        .collect();
    assert_eq!(draft_values, vec!["false", "true"]);
    assert_eq!(draft_counts, vec![2, 1]);
}

#[tokio::test]
async fn histogram_stats_mv_tracks_continuous_columns() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, \
         draft, squash, discussion_locked, first_contribution, \
         merge_status, project_id, author_id, traversal_path, _version, _deleted) VALUES
         (8001, 10, 'MR One',   'opened', 'a', 'main', false, false, false, false, '', 2000, 100, '1/200/', 1, false),
         (8002, 11, 'MR Two',   'opened', 'b', 'main', false, false, false, false, '', 2000, 100, '1/200/', 1, false),
         (8003, 12, 'MR Three', 'merged', 'c', 'main', false, false, false, false, '', 2000, 200, '1/200/', 1, false)",
        t("gl_merge_request")
    )).await;

    ctx.optimize_all().await;

    let mr_table = t("gl_merge_request");
    let hist_table = t("gkg_histogram_stats");

    // author_id histogram: value "100" has 2 rows, value "200" has 1 row
    let batches = ctx.query(&format!(
        "SELECT value, uniqMerge(row_count) AS cnt \
         FROM {hist_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'author_id' \
           AND partition_key = '1/200/' \
         GROUP BY value \
         ORDER BY value"
    )).await;

    assert!(!batches.is_empty(), "histogram table should have rows for author_id");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "expected 2 distinct author_id values");

    let values: Vec<&str> = (0..batch.num_rows())
        .map(|i| batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(i))
        .collect();
    let counts: Vec<u64> = (0..batch.num_rows())
        .map(|i| batch.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap().value(i))
        .collect();

    assert_eq!(values, vec!["100", "200"]);
    assert_eq!(counts, vec![2, 1]);
}

#[tokio::test]
async fn token_stats_mv_tracks_text_columns() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, \
         draft, squash, discussion_locked, first_contribution, \
         merge_status, project_id, traversal_path, _version, _deleted) VALUES
         (7001, 20, 'Fix login bug',    'opened', 'fix-login', 'main',    false, false, false, false, '', 3000, '1/300/', 1, false),
         (7002, 21, 'Add dark mode',    'opened', 'feat-dark', 'main',    false, false, false, false, '', 3000, '1/300/', 1, false),
         (7003, 22, 'Refactor auth',    'merged', 'fix-login', 'develop', false, false, false, false, '', 3000, '1/300/', 1, false)",
        t("gl_merge_request")
    )).await;

    ctx.optimize_all().await;

    let mr_table = t("gl_merge_request");
    let token_table = t("gkg_token_stats");

    // source_branch tokens: "fix-login" appears twice, "feat-dark" once
    let batches = ctx.query(&format!(
        "SELECT token, uniqMerge(row_count) AS cnt \
         FROM {token_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'source_branch' \
           AND partition_key = '1/300/' \
         GROUP BY token \
         ORDER BY token"
    )).await;

    assert!(!batches.is_empty(), "token table should have rows for source_branch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "expected 2 distinct source_branch tokens");

    let tokens: Vec<&str> = (0..batch.num_rows())
        .map(|i| batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(i))
        .collect();
    let counts: Vec<u64> = (0..batch.num_rows())
        .map(|i| batch.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap().value(i))
        .collect();

    assert_eq!(tokens, vec!["feat-dark", "fix-login"]);
    assert_eq!(counts, vec![1, 2]);
}

#[tokio::test]
async fn stats_partitioned_by_traversal_path() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, \
         draft, squash, discussion_locked, first_contribution, \
         merge_status, project_id, traversal_path, _version, _deleted) VALUES
         (6001, 30, 'MR in ns A', 'opened', 'a', 'main', false, false, false, false, '', 4000, '1/400/', 1, false),
         (6002, 31, 'MR in ns B', 'opened', 'b', 'main', false, false, false, false, '', 5000, '1/500/', 1, false),
         (6003, 32, 'MR in ns A', 'merged', 'c', 'main', false, false, false, false, '', 4000, '1/400/', 1, false)",
        t("gl_merge_request")
    )).await;

    ctx.optimize_all().await;

    let mr_table = t("gl_merge_request");
    let stats_table = t("gkg_column_stats");

    // Namespace 1/400/: 1 opened + 1 merged = 2 rows
    let ns_a = ctx.query(&format!(
        "SELECT value, uniqMerge(row_count) AS cnt \
         FROM {stats_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'state' \
           AND partition_key = '1/400/' \
         GROUP BY value \
         ORDER BY value"
    )).await;

    let batch_a = &ns_a[0];
    assert_eq!(batch_a.num_rows(), 2);

    // Namespace 1/500/: 1 opened only
    let ns_b = ctx.query(&format!(
        "SELECT value, uniqMerge(row_count) AS cnt \
         FROM {stats_table} \
         WHERE table_name = '{mr_table}' \
           AND column_name = 'state' \
           AND partition_key = '1/500/' \
         GROUP BY value \
         ORDER BY value"
    )).await;

    let batch_b = &ns_b[0];
    assert_eq!(batch_b.num_rows(), 1);
    let val = batch_b.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(0);
    let cnt = batch_b.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap().value(0);
    assert_eq!(val, "opened");
    assert_eq!(cnt, 1);
}

#[tokio::test]
async fn stats_dictionary_is_queryable() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    // Create the statistics dictionary (testkit skips dictionaries by default).
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
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, \
         draft, squash, discussion_locked, first_contribution, \
         merge_status, project_id, traversal_path, _version, _deleted) VALUES
         (5001, 40, 'MR One',   'opened', 'a', 'main', false, false, false, false, '', 6000, '1/600/', 1, false),
         (5002, 41, 'MR Two',   'merged', 'b', 'main', false, false, false, false, '', 6000, '1/600/', 1, false),
         (5003, 42, 'MR Three', 'opened', 'c', 'main', false, false, false, false, '', 6000, '1/600/', 1, false)",
        t("gl_merge_request")
    )).await;

    ctx.optimize_all().await;

    // Force dictionary reload so it picks up the fresh stats.
    let dict_name = t("gkg_column_stats_dict");
    ctx.execute(&format!("SYSTEM RELOAD DICTIONARY {dict_name}")).await;

    // Query the dictionary via dictGet.
    let mr_table = t("gl_merge_request");
    let batches = ctx.query(&format!(
        "SELECT dictGet('{dict_name}', 'row_count', \
         ('{mr_table}', 'state', '1/600/', 'opened')) AS cnt"
    )).await;

    assert!(!batches.is_empty(), "dictGet must return a result");
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 2, "dictGet should return 2 opened MRs");
}
