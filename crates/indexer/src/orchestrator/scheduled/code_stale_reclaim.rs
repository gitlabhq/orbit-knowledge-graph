use std::time::Instant;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use tracing::{info, warn};

use crate::clickhouse::{ArrowClickHouseClient, insert_overrides};
use crate::durability::WriteDurability;
use crate::modules::code::STALE_SNAPSHOTS_TABLE;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, table_prefix};
use orbit_server_config::{CodeStaleReclaimConfig, ScheduleConfiguration};

const TASK_NAME: &str = "maintenance.code_stale_reclaim";
const MARKS_TABLE: &str = "code_stale_reclaim_marks";
const EPOCH: &str = "1970-01-01 00:00:00.000000";
const STATEMENT_TIMEOUT_SECS: u64 = 600;
const MAX_QUERY_SIZE_BYTES: u64 = 64 * 1024 * 1024;

fn logical_table_name(table: &str) -> &str {
    table
        .strip_prefix(table_prefix(*SCHEMA_VERSION).as_str())
        .unwrap_or(table)
}

pub struct CodeStaleReclaim {
    graph: ArrowClickHouseClient,
    scoped_tables: Vec<String>,
    shared_edge_tables: Vec<String>,
    metrics: ScheduledTaskMetrics,
    config: CodeStaleReclaimConfig,
}

impl CodeStaleReclaim {
    pub fn new(
        graph: ArrowClickHouseClient,
        table_names: &CodeTableNames,
        metrics: ScheduledTaskMetrics,
        config: CodeStaleReclaimConfig,
    ) -> Self {
        let mut scoped_tables: Vec<String> = table_names
            .node_tables()
            .iter()
            .map(|table| table.to_string())
            .collect();
        let mut shared_edge_tables = Vec::new();
        for table in table_names.edge_table_names() {
            if table.contains("code_edge") {
                scoped_tables.push(table.to_string());
            } else {
                shared_edge_tables.push(table.to_string());
            }
        }
        Self {
            graph,
            scoped_tables,
            shared_edge_tables,
            metrics,
            config,
        }
    }

    async fn string_rows(&self, sql: &str) -> Result<Vec<Vec<String>>, TaskError> {
        let batches = self
            .graph
            .query(sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let mut rows = Vec::new();
        for batch in batches {
            let columns: Vec<&StringArray> = batch
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| TaskError::new("expected String columns"))
                })
                .collect::<Result<_, _>>()?;
            for row in 0..batch.num_rows() {
                rows.push(
                    columns
                        .iter()
                        .map(|column| column.value(row).to_string())
                        .collect(),
                );
            }
        }
        Ok(rows)
    }

    async fn unfinished_mutations(&self, table: &str) -> Result<u64, TaskError> {
        let rows = self
            .string_rows(&format!(
                "SELECT toString(count()) FROM system.mutations \
                 WHERE database = currentDatabase() AND table = '{table}' AND is_done = 0"
            ))
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(|count| count.parse().ok())
            .unwrap_or(0))
    }

    async fn reclaimed_through(&self, table: &str) -> Result<String, TaskError> {
        let logical = logical_table_name(table);
        let rows = self
            .string_rows(&format!(
                "SELECT toString(reclaimed_through) FROM {MARKS_TABLE} FINAL \
                 WHERE table_name = '{logical}'"
            ))
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or_else(|| EPOCH.to_string()))
    }

    async fn pending_tuples(
        &self,
        table: &str,
        scoped: bool,
    ) -> Result<(Vec<String>, Option<String>), TaskError> {
        let mark = self.reclaimed_through(table).await?;
        let key = if scoped {
            "(traversal_path, project_id, branch, stale_version)"
        } else {
            "(traversal_path, stale_version)"
        };
        let limit = self.config.max_scopes_per_statement;
        let settle = self.config.settle_secs;
        let rows = self
            .string_rows(&format!(
                "SELECT toString({key}) AS tuple, toString(max(retired_at)) AS retired \
                 FROM {STALE_SNAPSHOTS_TABLE} FINAL \
                 WHERE retired_at > toDateTime64('{mark}', 6, 'UTC') \
                   AND retired_at <= now64(6) - INTERVAL {settle} SECOND \
                 GROUP BY {key} ORDER BY retired ASC LIMIT {limit} WITH TIES"
            ))
            .await?;
        let through = rows.iter().map(|row| row[1].clone()).max();
        let tuples = rows
            .into_iter()
            .map(|row| row.into_iter().next().unwrap_or_default())
            .collect();
        Ok((tuples, through))
    }

    async fn reclaim_table(&self, table: &str, scoped: bool) -> Result<usize, TaskError> {
        let unfinished = self.unfinished_mutations(table).await?;
        if unfinished > self.config.max_unfinished_mutations {
            warn!(
                table,
                unfinished, "skipping stale snapshot reclaim: mutation backlog"
            );
            self.metrics.record_requests_skipped(TASK_NAME, 1);
            return Ok(0);
        }
        let (tuples, through) = self.pending_tuples(table, scoped).await?;
        let Some(through) = through else {
            return Ok(0);
        };
        let predicate = if scoped {
            format!(
                "(traversal_path, project_id, branch, _version) IN ({})",
                tuples.join(", ")
            )
        } else {
            format!(
                "(traversal_path, _version) IN ({}) AND source_kind IN ({})",
                tuples.join(", "),
                CodeTableNames::node_kinds_sql_list()
            )
        };
        let sql = format!(
            "DELETE FROM {table} WHERE {predicate} \
             SETTINGS lightweight_deletes_sync = 0, max_execution_time = {STATEMENT_TIMEOUT_SECS}"
        );
        let started = Instant::now();
        self.graph
            .query(&sql)
            .with_setting("max_query_size", MAX_QUERY_SIZE_BYTES.to_string())
            .execute()
            .await
            .map_err(TaskError::new)?;
        let elapsed = started.elapsed().as_secs_f64();
        self.metrics.record_query_duration(table, elapsed);
        let mut mark = self.graph.query(&format!(
            "INSERT INTO {MARKS_TABLE} (table_name, reclaimed_through) \
             VALUES ({{table_name:String}}, {{reclaimed_through:String}})"
        ));
        for (name, value) in insert_overrides(WriteDurability::Durable) {
            mark = mark.with_setting(*name, *value);
        }
        mark.param("table_name", logical_table_name(table))
            .param("reclaimed_through", &through)
            .execute()
            .await
            .map_err(TaskError::new)?;
        info!(
            table,
            scopes = tuples.len(),
            statement_bytes = sql.len(),
            submit_ms = (elapsed * 1000.0) as u64,
            reclaimed_through = %through,
            "submitted stale snapshot reclaim"
        );
        Ok(tuples.len())
    }
}

#[async_trait]
impl ScheduledTask for CodeStaleReclaim {
    fn name(&self) -> &str {
        TASK_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let mut failed = 0usize;
        let mut scopes = 0usize;
        for (tables, scoped) in [
            (&self.scoped_tables, true),
            (&self.shared_edge_tables, false),
        ] {
            for table in tables {
                match self.reclaim_table(table, scoped).await {
                    Ok(count) => scopes += count,
                    Err(error) => {
                        failed += 1;
                        self.metrics.record_error(TASK_NAME, "reclaim");
                        warn!(table = table.as_str(), %error, "stale snapshot reclaim failed");
                    }
                }
            }
        }
        let outcome = if failed == 0 { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        info!(scopes, failed, "stale snapshot reclaim pass complete");
        if failed > 0 {
            return Err(TaskError::new(format!("{failed} tables failed")));
        }
        Ok(())
    }
}
