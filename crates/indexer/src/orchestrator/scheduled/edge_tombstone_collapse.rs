use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use orbit_utils::clickhouse::render_arrow_sql_literal;
use tracing::warn;

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{EdgeTombstoneCollapseConfig, ScheduleConfiguration};

const TASK_NAME: &str = "maintenance.edge_tombstone_collapse";
const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";
const TRAVERSAL_PATH: &str = "traversal_path";
const STATEMENT_TIMEOUT_SECS: u64 = 7200;
const DISCOVERY_OVERLAP_SECS: i64 = 300;
const SCOPES_PER_SELECT: usize = 50;
const DELETE_STATEMENT_BYTES: usize = 256 * 1024 / 10 * 9;

struct SweptTable {
    name: String,
    sort_key: Vec<String>,
}

pub struct EdgeTombstoneCollapse {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    code_checkpoint_table: String,
    tables: Vec<SweptTable>,
    metrics: ScheduledTaskMetrics,
    config: EdgeTombstoneCollapseConfig,
}

impl EdgeTombstoneCollapse {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: EdgeTombstoneCollapseConfig,
    ) -> Self {
        Self {
            graph,
            checkpoint_store,
            code_checkpoint_table: prefixed_table_name(
                CODE_INDEXING_CHECKPOINT_TABLE,
                *SCHEMA_VERSION,
            ),
            tables: swept_edge_tables(ontology),
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for EdgeTombstoneCollapse {
    fn name(&self) -> &str {
        TASK_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let result = self.collapse().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(TASK_NAME, outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl EdgeTombstoneCollapse {
    async fn collapse(&self) -> Result<(), TaskError> {
        let now = Utc::now();
        let cursor = self
            .checkpoint_store
            .load(TASK_NAME)
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark);
        let lookback = TimeDelta::seconds(self.config.lookback_secs as i64);
        let scopes = self
            .discover_scopes(window_start(cursor, now, lookback), now)
            .await?;

        let mut drained = true;
        for table in &self.tables {
            drained &= self.collapse_table(table, &scopes, now).await?;
        }

        if drained {
            self.checkpoint_store
                .save_completed(TASK_NAME, &now, WriteDurability::Durable)
                .await
                .map_err(TaskError::new)?;
        } else {
            warn!(
                task = TASK_NAME,
                scopes = scopes.len(),
                budget = self.config.max_keys_per_run,
                "key budget reached; the cursor stays until the remainder drains"
            );
        }
        Ok(())
    }

    async fn discover_scopes(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Vec<String>, TaskError> {
        let batches = self
            .graph
            .query(&discover_scopes_sql(&self.code_checkpoint_table))
            .param(
                "window_start",
                window_start.format(TIMESTAMP_FORMAT).to_string(),
            )
            .param(
                "window_end",
                window_end.format(TIMESTAMP_FORMAT).to_string(),
            )
            .with_setting("max_execution_time", STATEMENT_TIMEOUT_SECS.to_string())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        render_key_literals(&batches, &[TRAVERSAL_PATH.to_string()])
    }

    async fn collapse_table(
        &self,
        table: &SweptTable,
        scopes: &[String],
        now: DateTime<Utc>,
    ) -> Result<bool, TaskError> {
        let started = Instant::now();
        let mut remaining = self.config.max_keys_per_run;
        let mut drained = true;
        for chunk in scopes.chunks(SCOPES_PER_SELECT) {
            let batches = self
                .graph
                .query(&select_tombstoned_keys_sql(
                    table,
                    &chunk.join(", "),
                    remaining + 1,
                ))
                .with_setting("optimize_aggregation_in_order", "1")
                .with_setting("max_execution_time", STATEMENT_TIMEOUT_SECS.to_string())
                .fetch_arrow()
                .await
                .map_err(TaskError::new)?;
            let keys = render_key_literals(&batches, &table.sort_key)?;
            let over_budget = keys.len() > remaining;
            let to_delete = &keys[..keys.len().min(remaining)];
            self.delete_keys(table, to_delete, now).await?;
            remaining -= to_delete.len();
            if over_budget {
                drained = false;
                break;
            }
        }
        self.metrics
            .record_query_duration(&table.name, started.elapsed().as_secs_f64());
        Ok(drained)
    }

    async fn delete_keys(
        &self,
        table: &SweptTable,
        keys: &[String],
        window_end: DateTime<Utc>,
    ) -> Result<(), TaskError> {
        let window_end = window_end.format(TIMESTAMP_FORMAT).to_string();
        for statement in build_delete_statements(table, keys, &window_end, DELETE_STATEMENT_BYTES) {
            self.graph
                .query(&statement)
                .execute()
                .await
                .map_err(TaskError::new)?;
        }
        Ok(())
    }
}

fn window_start(
    cursor: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    lookback: TimeDelta,
) -> DateTime<Utc> {
    match cursor {
        Some(cursor) => cursor - TimeDelta::seconds(DISCOVERY_OVERLAP_SECS),
        None => now - lookback,
    }
}

fn swept_edge_tables(ontology: &ontology::Ontology) -> Vec<SweptTable> {
    ontology
        .edge_tables()
        .into_iter()
        .filter_map(|table| {
            Some(SweptTable {
                name: prefixed_table_name(table, *SCHEMA_VERSION),
                sort_key: ontology.sort_key_for_table(table)?.to_vec(),
            })
        })
        .collect()
}

fn discover_scopes_sql(code_checkpoint: &str) -> String {
    format!(
        "SELECT DISTINCT {TRAVERSAL_PATH} FROM {code_checkpoint} FINAL \
         WHERE indexed_at > {{window_start:String}} AND indexed_at <= {{window_end:String}} \
           AND _deleted = false \
         ORDER BY {TRAVERSAL_PATH}"
    )
}

fn select_tombstoned_keys_sql(table: &SweptTable, scope_list: &str, limit: usize) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "SELECT {keys} FROM {table} WHERE {TRAVERSAL_PATH} IN ({scope_list}) \
         GROUP BY {keys} HAVING argMax(_deleted, _version) LIMIT {limit}",
        table = table.name,
    )
}

fn build_delete_statements(
    table: &SweptTable,
    keys: &[String],
    window_end: &str,
    budget: usize,
) -> Vec<String> {
    let columns = key_columns(table);
    let fixed = delete_statement(&table.name, &columns, "", window_end).len();
    let mut statements = Vec::new();
    let mut chunk: Vec<&str> = Vec::new();
    let mut chunk_bytes = 0usize;
    for key in keys {
        let added = key.len() + ", ".len();
        if !chunk.is_empty() && fixed + chunk_bytes + added > budget {
            statements.push(delete_statement(
                &table.name,
                &columns,
                &chunk.join(", "),
                window_end,
            ));
            chunk.clear();
            chunk_bytes = 0;
        }
        chunk_bytes += added;
        chunk.push(key);
    }
    if !chunk.is_empty() {
        statements.push(delete_statement(
            &table.name,
            &columns,
            &chunk.join(", "),
            window_end,
        ));
    }
    statements
}

fn key_columns(table: &SweptTable) -> String {
    match table.sort_key.as_slice() {
        [single] => single.clone(),
        many => format!("({})", many.join(", ")),
    }
}

fn delete_statement(table: &str, columns: &str, key_list: &str, window_end: &str) -> String {
    format!(
        "DELETE FROM {table} WHERE {columns} IN ({key_list}) AND _version <= '{window_end}' \
         SETTINGS lightweight_deletes_sync = 0, max_execution_time = {STATEMENT_TIMEOUT_SECS}"
    )
}

fn render_key_literals(
    batches: &[RecordBatch],
    columns: &[String],
) -> Result<Vec<String>, TaskError> {
    let mut literals = Vec::new();
    for batch in batches {
        let arrays = plain_columns(batch, columns)?;
        for row in 0..batch.num_rows() {
            let values = arrays
                .iter()
                .map(|array| render_arrow_sql_literal(array, row).map_err(TaskError::new))
                .collect::<Result<Vec<_>, _>>()?;
            literals.push(match values.as_slice() {
                [single] => single.clone(),
                many => format!("({})", many.join(", ")),
            });
        }
    }
    Ok(literals)
}

fn plain_columns(batch: &RecordBatch, columns: &[String]) -> Result<Vec<ArrayRef>, TaskError> {
    columns
        .iter()
        .map(|column| {
            let array = batch
                .column_by_name(column)
                .ok_or_else(|| TaskError::new(format!("sort key column '{column}' missing")))?;
            match array.data_type() {
                DataType::Dictionary(_, value_type) => compute::cast(array, value_type)
                    .map_err(|e| TaskError::new(format!("cast '{column}': {e}"))),
                _ => Ok(Arc::clone(array)),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int64Array, StringArray};
    use arrow::datatypes::{Field, Int32Type, Schema};

    fn edge_table() -> SweptTable {
        SweptTable {
            name: "v99_gl_edge".to_string(),
            sort_key: ["relationship_kind", "source_id", "target_id"]
                .map(str::to_string)
                .to_vec(),
        }
    }

    #[test]
    fn sweeps_every_edge_table_and_no_node_table() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let swept: Vec<String> = swept_edge_tables(&ontology)
            .into_iter()
            .map(|t| t.name)
            .collect();
        for edge_table in ontology.edge_tables() {
            assert!(swept.contains(&prefixed_table_name(edge_table, *SCHEMA_VERSION)));
        }
        for node in ontology.nodes() {
            assert!(!swept.contains(&prefixed_table_name(
                &node.destination_table,
                *SCHEMA_VERSION
            )));
        }
        assert_eq!(swept.len(), ontology.edge_tables().len());
    }

    #[test]
    fn every_edge_sort_key_leads_with_traversal_path_and_renders_as_string_or_int64() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        for table in ontology.edge_tables() {
            let config = ontology
                .edge_table_config(table)
                .expect("edge table config");
            assert_eq!(
                config.sort_key.first().map(String::as_str),
                Some(TRAVERSAL_PATH)
            );
            for key in &config.sort_key {
                let column = config
                    .columns
                    .iter()
                    .find(|c| &c.name == key)
                    .unwrap_or_else(|| panic!("{table}.{key} has no column"));
                assert!(
                    matches!(
                        column.data_type,
                        ontology::DataType::String | ontology::DataType::Int
                    ),
                    "{table}.{key} is {:?}",
                    column.data_type
                );
            }
        }
    }

    #[test]
    fn window_starts_behind_the_cursor_or_at_the_lookback_floor() {
        let now = DateTime::parse_from_rfc3339("2026-08-29T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let lookback = TimeDelta::hours(1);
        assert_eq!(window_start(None, now, lookback), now - lookback);

        let stale = now - TimeDelta::days(30);
        assert_eq!(
            window_start(Some(stale), now, lookback),
            stale - TimeDelta::seconds(DISCOVERY_OVERLAP_SECS)
        );
    }

    #[test]
    fn discovery_reads_reindexed_scopes_from_the_code_checkpoint() {
        let sql = discover_scopes_sql("v99_code_indexing_checkpoint");
        assert!(
            sql.starts_with(
                "SELECT DISTINCT traversal_path FROM v99_code_indexing_checkpoint FINAL"
            ),
            "{sql}"
        );
        assert!(
            sql.contains(
                "indexed_at > {window_start:String} AND indexed_at <= {window_end:String}"
            ),
            "{sql}"
        );
        assert!(sql.contains("_deleted = false"), "{sql}");
    }

    #[test]
    fn key_select_is_scoped_by_traversal_path_and_keeps_only_newest_tombstones() {
        let sql = select_tombstoned_keys_sql(&edge_table(), "'1/100/', '1/200/'", 501);
        assert_eq!(
            sql,
            "SELECT relationship_kind, source_id, target_id FROM v99_gl_edge \
             WHERE traversal_path IN ('1/100/', '1/200/') \
             GROUP BY relationship_kind, source_id, target_id \
             HAVING argMax(_deleted, _version) LIMIT 501"
        );
    }

    #[test]
    fn delete_is_a_flat_literal_bounded_to_the_snapshot() {
        let stmts = build_delete_statements(
            &edge_table(),
            &[
                "('MENTIONS', 1, 2)".to_string(),
                "('CONTAINS', 3, 4)".to_string(),
            ],
            "2020-01-01 00:00:00.000000",
            1_000_000,
        );
        assert_eq!(
            stmts,
            vec![
                "DELETE FROM v99_gl_edge WHERE (relationship_kind, source_id, target_id) IN \
                 (('MENTIONS', 1, 2), ('CONTAINS', 3, 4)) AND _version <= '2020-01-01 00:00:00.000000' \
                 SETTINGS lightweight_deletes_sync = 0, max_execution_time = 7200"
            ]
        );
    }

    #[test]
    fn key_lists_pack_into_multiple_statements_under_the_byte_budget() {
        let keys: Vec<String> = (0..100)
            .map(|i| format!("('MENTIONS', {i}, {i})"))
            .collect();
        let budget = 400;
        let stmts =
            build_delete_statements(&edge_table(), &keys, "2020-01-01 00:00:00.000000", budget);
        assert!(stmts.len() > 1);
        let overshoot = "('MENTIONS', 99, 99), ".len();
        for sql in &stmts {
            assert!(sql.len() <= budget + overshoot, "{}: {sql}", sql.len());
        }
        let total: usize = stmts.iter().map(|s| s.matches("('MENTIONS'").count()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn dictionary_and_int_sort_keys_render_as_literals() {
        let kinds: DictionaryArray<Int32Type> = vec!["MENTIONS", "CONTAINS"].into_iter().collect();
        let schema = Schema::new(vec![
            Field::new("relationship_kind", kinds.data_type().clone(), false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("target_id", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(kinds),
                Arc::new(Int64Array::from(vec![1_i64, 3])),
                Arc::new(Int64Array::from(vec![2_i64, 4])),
            ],
        )
        .unwrap();

        let keys = render_key_literals(&[batch], &edge_table().sort_key).unwrap();
        assert_eq!(keys, vec!["('MENTIONS', 1, 2)", "('CONTAINS', 3, 4)"]);
    }

    #[test]
    fn single_column_keys_render_bare_for_a_scope_list() {
        let schema = Schema::new(vec![Field::new(TRAVERSAL_PATH, DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec!["1/100/", "1/2'00/"]))],
        )
        .unwrap();
        let scopes = render_key_literals(&[batch], &[TRAVERSAL_PATH.to_string()]).unwrap();
        assert_eq!(scopes, vec!["'1/100/'", "'1/2''00/'"]);
    }

    #[test]
    fn missing_sort_key_column_is_an_error() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let err = render_key_literals(&[batch], &["not_a_column".to_string()]).unwrap_err();
        assert!(err.to_string().contains("not_a_column"));
    }
}
