use std::collections::BTreeMap;
use std::time::Instant;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use tracing::{info, warn};

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, STALE_ROWS_TABLE, insert_overrides};
use crate::durability::WriteDurability;
use crate::modules::code::STALE_SNAPSHOTS_TABLE;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, StaleReclaimConfig};

const TASK_NAME: &str = "maintenance.stale_reclaim";
const MARKS_TABLE: &str = "stale_reclaim_marks";
const EPOCH: &str = "1970-01-01 00:00:00.000000";
const ROWS_PHASE: &str = "rows";
const TOMBSTONES_PHASE: &str = "tombstones";
const CODE_LEDGER_KEY: &str = "toString((traversal_path, project_id, branch, stale_version))";
const VERSION_LITERAL_BYTES: usize = 32;
const STATEMENT_TIMEOUT_SECS: u64 = 600;
const MAX_QUERY_SIZE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
enum CodeScope {
    Project,
    Shared,
}

impl CodeScope {
    fn scope_columns(self) -> &'static str {
        match self {
            CodeScope::Project => "(traversal_path, project_id, branch)",
            CodeScope::Shared => "tuple(traversal_path)",
        }
    }

    fn group_by(self) -> &'static str {
        match self {
            CodeScope::Project => "traversal_path, project_id, branch",
            CodeScope::Shared => "traversal_path",
        }
    }

    fn scope_filter(self, scopes: &str) -> String {
        match self {
            CodeScope::Project => format!("(traversal_path, project_id, branch) IN ({scopes})"),
            CodeScope::Shared => format!(
                "traversal_path IN ({scopes}) AND source_kind IN ({})",
                CodeTableNames::node_kinds_sql_list()
            ),
        }
    }

    fn predicate(self, tuples: &str) -> String {
        match self {
            CodeScope::Project => {
                format!("(traversal_path, project_id, branch, _version) IN ({tuples})")
            }
            CodeScope::Shared => format!(
                "(traversal_path, _version) IN ({tuples}) AND source_kind IN ({})",
                CodeTableNames::node_kinds_sql_list()
            ),
        }
    }
}

struct ReclaimTable {
    name: String,
    logical: String,
    sort_key: Vec<String>,
    code_scope: Option<CodeScope>,
}

#[derive(Clone, Default)]
struct Cursor {
    at: String,
    key: String,
}

struct LedgerRow {
    cursor: Cursor,
    scope: String,
    bound: String,
}

#[derive(Default)]
struct Versions {
    live: Vec<String>,
    dead: Vec<String>,
}

struct Part {
    ledger: &'static str,
    phase: &'static str,
    predicate: String,
    keys: usize,
    cursor: Option<Cursor>,
}

pub struct StaleReclaim {
    graph: ArrowClickHouseClient,
    tables: Vec<ReclaimTable>,
    metrics: ScheduledTaskMetrics,
    config: StaleReclaimConfig,
}

fn split_versions(list: &str) -> Vec<String> {
    list.split('|')
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect()
}

fn version_tuples(scope: &str, versions: &[String]) -> Vec<String> {
    let prefix = scope.trim_end_matches(')');
    versions
        .iter()
        .map(|version| format!("{prefix},'{version}')"))
        .collect()
}

fn tuples_bytes(tuples: &[String]) -> usize {
    tuples.iter().map(|tuple| tuple.len() + 2).sum()
}

impl StaleReclaim {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        code_tables: &CodeTableNames,
        metrics: ScheduledTaskMetrics,
        config: StaleReclaimConfig,
    ) -> Self {
        let mut tables: Vec<ReclaimTable> = ontology
            .nodes()
            .map(|node| node.destination_table.as_str())
            .chain(ontology.edge_tables())
            .filter_map(|logical| {
                let sort_key = ontology.sort_key_for_table(logical)?.to_vec();
                let name = prefixed_table_name(logical, *SCHEMA_VERSION);
                let code_scope = if code_tables.node_tables().contains(&name.as_str()) {
                    Some(CodeScope::Project)
                } else if code_tables.edge_table_names().contains(&name.as_str()) {
                    Some(if name.contains("code_edge") {
                        CodeScope::Project
                    } else {
                        CodeScope::Shared
                    })
                } else {
                    None
                };
                Some(ReclaimTable {
                    name,
                    logical: logical.to_string(),
                    sort_key,
                    code_scope,
                })
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            graph,
            tables,
            metrics,
            config,
        }
    }

    async fn rows(&self, query: ArrowQuery) -> Result<Vec<Vec<String>>, TaskError> {
        let batches = query.fetch_arrow().await.map_err(TaskError::new)?;
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
            .rows(self.graph.query(&format!(
                "SELECT toString(count()) FROM system.mutations \
                 WHERE database = currentDatabase() AND table = '{table}' AND is_done = 0"
            )))
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(|count| count.parse().ok())
            .unwrap_or(0))
    }

    async fn mark(&self, ledger: &str, phase: &str, logical: &str) -> Result<Cursor, TaskError> {
        let rows = self
            .rows(
                self.graph
                    .query(&format!(
                        "SELECT toString(reclaimed_through), reclaimed_through_key \
                         FROM {MARKS_TABLE} FINAL \
                         WHERE ledger = {{ledger:String}} AND phase = {{phase:String}} \
                           AND table_name = {{table_name:String}}"
                    ))
                    .param("ledger", ledger)
                    .param("phase", phase)
                    .param("table_name", logical),
            )
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .map(|row| Cursor {
                at: row[0].clone(),
                key: row[1].clone(),
            })
            .unwrap_or_else(|| Cursor {
                at: EPOCH.to_string(),
                key: String::new(),
            }))
    }

    async fn save_mark(&self, part: &Part, logical: &str) -> Result<(), TaskError> {
        let Some(cursor) = &part.cursor else {
            return Ok(());
        };
        let mut mark = self.graph.query(&format!(
            "INSERT INTO {MARKS_TABLE} (ledger, phase, table_name, reclaimed_through, reclaimed_through_key) \
             VALUES ({{ledger:String}}, {{phase:String}}, {{table_name:String}}, {{reclaimed_through:String}}, {{reclaimed_through_key:String}})"
        ));
        for (name, value) in insert_overrides(WriteDurability::Durable) {
            mark = mark.with_setting(*name, *value);
        }
        mark.param("ledger", part.ledger)
            .param("phase", part.phase)
            .param("table_name", logical)
            .param("reclaimed_through", &cursor.at)
            .param("reclaimed_through_key", &cursor.key)
            .execute()
            .await
            .map_err(TaskError::new)
    }

    fn ledger_query(&self, sql: &str, from: &Cursor, to: Option<&Cursor>) -> ArrowQuery {
        let mut query = self
            .graph
            .query(sql)
            .param("from_at", &from.at)
            .param("from_key", &from.key);
        if let Some(to) = to {
            query = query.param("to_at", &to.at).param("to_key", &to.key);
        }
        query
    }

    fn window(&self, to: Option<&Cursor>, key: &str) -> String {
        let mut sql = format!(
            "(retired_at, {key}) > (toDateTime64({{from_at:String}}, 6, 'UTC'), {{from_key:String}}) \
             AND retired_at <= now64(6) - INTERVAL {} SECOND",
            self.config.settle_secs
        );
        if to.is_some() {
            sql.push_str(&format!(
                " AND (retired_at, {key}) <= (toDateTime64({{to_at:String}}, 6, 'UTC'), {{to_key:String}})"
            ));
        }
        sql
    }

    async fn pending_snapshots(
        &self,
        scope: CodeScope,
        from: &Cursor,
        to: Option<&Cursor>,
    ) -> Result<Vec<LedgerRow>, TaskError> {
        let sql = format!(
            "SELECT toString({}) AS scope, toString(stale_version) AS bound, \
                    toString(retired_at) AS at, {CODE_LEDGER_KEY} AS key \
             FROM {STALE_SNAPSHOTS_TABLE} FINAL \
             WHERE {} \
             ORDER BY retired_at, key LIMIT {}",
            scope.scope_columns(),
            self.window(to, CODE_LEDGER_KEY),
            self.config.max_keys_per_statement
        );
        Ok(self
            .rows(self.ledger_query(&sql, from, to))
            .await?
            .into_iter()
            .map(|row| LedgerRow {
                cursor: Cursor {
                    at: row[2].clone(),
                    key: row[3].clone(),
                },
                scope: row[0].clone(),
                bound: row[1].clone(),
            })
            .collect())
    }

    async fn pending_rows(
        &self,
        logical: &str,
        from: &Cursor,
        to: Option<&Cursor>,
    ) -> Result<Vec<LedgerRow>, TaskError> {
        let sql = format!(
            "SELECT row_key AS scope, toString(retired_at) AS at \
             FROM {STALE_ROWS_TABLE} \
             WHERE table_name = {{table_name:String}} AND {} \
             ORDER BY retired_at, row_key LIMIT {}",
            self.window(to, "row_key"),
            self.config.max_keys_per_statement
        );
        Ok(self
            .rows(
                self.ledger_query(&sql, from, to)
                    .param("table_name", logical),
            )
            .await?
            .into_iter()
            .map(|row| LedgerRow {
                cursor: Cursor {
                    at: row[1].clone(),
                    key: row[0].clone(),
                },
                scope: row[0].clone(),
                bound: String::new(),
            })
            .collect())
    }

    async fn versions(&self, sql: String) -> Result<BTreeMap<String, Versions>, TaskError> {
        let rows = self
            .rows(
                self.graph
                    .query(&sql)
                    .with_setting("max_query_size", MAX_QUERY_SIZE_BYTES.to_string())
                    .with_setting("max_execution_time", STATEMENT_TIMEOUT_SECS.to_string()),
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row[0].clone(),
                    Versions {
                        live: split_versions(&row[1]),
                        dead: split_versions(&row[2]),
                    },
                )
            })
            .collect())
    }

    async fn code_part(
        &self,
        table: &ReclaimTable,
        scope: CodeScope,
        phase: &'static str,
        from: &Cursor,
        to: Option<&Cursor>,
        budget: &mut usize,
    ) -> Result<Option<Part>, TaskError> {
        let ledger = self.pending_snapshots(scope, from, to).await?;
        let Some(max_bound) = ledger.iter().map(|row| row.bound.as_str()).max() else {
            return Ok(None);
        };
        let scopes = ledger
            .iter()
            .map(|row| row.scope.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let versions = self
            .versions(format!(
                "SELECT toString({}) AS scope, \
                        arrayStringConcat(arrayMap(v -> toString(v), groupUniqArrayIf(_version, NOT _deleted)), '|') AS live, \
                        arrayStringConcat(arrayMap(v -> toString(v), groupUniqArrayIf(_version, _deleted)), '|') AS dead \
                 FROM {} WHERE {} AND _version <= toDateTime64('{max_bound}', 6, 'UTC') \
                 GROUP BY {}",
                scope.scope_columns(),
                table.name,
                scope.scope_filter(&scopes),
                scope.group_by()
            ))
            .await?;
        let mut tuples = Vec::new();
        let mut cursor = None;
        let mut ready = true;
        for row in &ledger {
            let found = versions.get(&row.scope);
            let live: Vec<String> = found
                .map(|v| {
                    v.live
                        .iter()
                        .filter(|x| **x <= row.bound)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            let dead: Vec<String> = found
                .map(|v| {
                    v.dead
                        .iter()
                        .filter(|x| **x <= row.bound)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            let mut selected = match (phase, live.is_empty()) {
                (ROWS_PHASE, _) => version_tuples(&row.scope, &live),
                (_, false) => {
                    ready = false;
                    version_tuples(&row.scope, &live)
                }
                (_, true) => version_tuples(&row.scope, &dead),
            };
            if tuples_bytes(&tuples) + tuples_bytes(&selected) > *budget {
                break;
            }
            tuples.append(&mut selected);
            cursor = Some(row.cursor.clone());
        }
        if !ready {
            cursor = None;
        }
        *budget = budget.saturating_sub(tuples_bytes(&tuples));
        let predicate = if tuples.is_empty() {
            String::new()
        } else {
            scope.predicate(&tuples.join(", "))
        };
        Ok(Some(Part {
            ledger: STALE_SNAPSHOTS_TABLE,
            phase,
            predicate,
            keys: tuples.len(),
            cursor,
        }))
    }

    async fn rows_part(
        &self,
        table: &ReclaimTable,
        phase: &'static str,
        from: &Cursor,
        to: Option<&Cursor>,
        budget: &mut usize,
    ) -> Result<Option<Part>, TaskError> {
        let ledger = self.pending_rows(&table.logical, from, to).await?;
        let mut taken = 0usize;
        let mut estimate = 0usize;
        for row in &ledger {
            estimate += 2 * (row.scope.len() + VERSION_LITERAL_BYTES + 3);
            if estimate > *budget {
                break;
            }
            taken += 1;
        }
        if taken == 0 {
            return Ok(None);
        }
        let ledger = &ledger[..taken];
        let keys = ledger
            .iter()
            .map(|row| row.scope.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let key_columns = table.sort_key.join(", ");
        let verified = self
            .versions(format!(
                "SELECT toString(tuple({key_columns})) AS key, \
                        arrayStringConcat(arrayMap(v -> toString(v), groupUniqArrayIf(_version, NOT _deleted)), '|') AS live, \
                        arrayStringConcat(arrayMap(v -> toString(v), groupUniqArrayIf(_version, _deleted)), '|') AS dead \
                 FROM {} WHERE ({key_columns}) IN ({keys}) \
                 GROUP BY {key_columns} \
                 HAVING maxIf(_version, _deleted) > maxIf(_version, NOT _deleted)",
                table.name
            ))
            .await?;
        let mut tuples = Vec::new();
        let mut ready = true;
        for (key, versions) in &verified {
            match (phase, versions.live.is_empty()) {
                (ROWS_PHASE, _) => tuples.extend(version_tuples(key, &versions.live)),
                (_, false) => {
                    ready = false;
                    tuples.extend(version_tuples(key, &versions.live));
                }
                (_, true) => tuples.extend(version_tuples(key, &versions.dead)),
            }
        }
        *budget = budget.saturating_sub(tuples_bytes(&tuples));
        let predicate = if tuples.is_empty() {
            String::new()
        } else {
            format!("({key_columns}, _version) IN ({})", tuples.join(", "))
        };
        Ok(Some(Part {
            ledger: STALE_ROWS_TABLE,
            phase,
            predicate,
            keys: verified.len(),
            cursor: ready.then(|| ledger[taken - 1].cursor.clone()),
        }))
    }

    async fn reclaim_table(&self, table: &ReclaimTable) -> Result<usize, TaskError> {
        let unfinished = self.unfinished_mutations(&table.name).await?;
        if unfinished > self.config.max_unfinished_mutations {
            warn!(
                table = table.name,
                unfinished, "skipping stale reclaim: mutation backlog"
            );
            self.metrics.record_requests_skipped(TASK_NAME, 1);
            return Ok(0);
        }
        let mut budget = self.config.max_statement_bytes;
        let mut parts = Vec::new();
        if let Some(scope) = table.code_scope {
            let rows_mark = self
                .mark(STALE_SNAPSHOTS_TABLE, ROWS_PHASE, &table.logical)
                .await?;
            let tombstones_mark = self
                .mark(STALE_SNAPSHOTS_TABLE, TOMBSTONES_PHASE, &table.logical)
                .await?;
            parts.extend(
                self.code_part(
                    table,
                    scope,
                    TOMBSTONES_PHASE,
                    &tombstones_mark,
                    Some(&rows_mark),
                    &mut budget,
                )
                .await?,
            );
            parts.extend(
                self.code_part(table, scope, ROWS_PHASE, &rows_mark, None, &mut budget)
                    .await?,
            );
        }
        let rows_mark = self
            .mark(STALE_ROWS_TABLE, ROWS_PHASE, &table.logical)
            .await?;
        let tombstones_mark = self
            .mark(STALE_ROWS_TABLE, TOMBSTONES_PHASE, &table.logical)
            .await?;
        parts.extend(
            self.rows_part(
                table,
                TOMBSTONES_PHASE,
                &tombstones_mark,
                Some(&rows_mark),
                &mut budget,
            )
            .await?,
        );
        parts.extend(
            self.rows_part(table, ROWS_PHASE, &rows_mark, None, &mut budget)
                .await?,
        );
        if parts.is_empty() {
            return Ok(0);
        }
        let predicates: Vec<String> = parts
            .iter()
            .filter(|part| !part.predicate.is_empty())
            .map(|part| format!("({})", part.predicate))
            .collect();
        let keys: usize = parts.iter().map(|part| part.keys).sum();
        if !predicates.is_empty() {
            let sql = format!(
                "DELETE FROM {} WHERE {} \
                 SETTINGS lightweight_deletes_sync = 0, max_execution_time = {STATEMENT_TIMEOUT_SECS}",
                table.name,
                predicates.join(" OR ")
            );
            let started = Instant::now();
            self.graph
                .query(&sql)
                .with_setting("max_query_size", MAX_QUERY_SIZE_BYTES.to_string())
                .execute()
                .await
                .map_err(TaskError::new)?;
            let elapsed = started.elapsed().as_secs_f64();
            self.metrics.record_query_duration(&table.name, elapsed);
            info!(
                table = table.name,
                keys,
                statement_bytes = sql.len(),
                submit_ms = (elapsed * 1000.0) as u64,
                "submitted stale reclaim"
            );
        }
        for part in &parts {
            self.save_mark(part, &table.logical).await?;
        }
        Ok(keys)
    }
}

#[async_trait]
impl ScheduledTask for StaleReclaim {
    fn name(&self) -> &str {
        TASK_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let mut failed = 0usize;
        let mut keys = 0usize;
        for table in &self.tables {
            match self.reclaim_table(table).await {
                Ok(count) => keys += count,
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(TASK_NAME, "reclaim");
                    warn!(table = table.name, %error, "stale reclaim failed");
                }
            }
        }
        let outcome = if failed == 0 { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        info!(keys, failed, "stale reclaim pass complete");
        if failed > 0 {
            return Err(TaskError::new(format!("{failed} tables failed")));
        }
        Ok(())
    }
}
