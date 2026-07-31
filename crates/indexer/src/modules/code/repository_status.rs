use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};
use thiserror::Error;

use super::config::CodeTableNames;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};

/// Indexing lifecycle of a project x branch. The string is the only value
/// written to `gl_repository.status`, so the column is enum-bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoStatus {
    /// A start row exists but no terminal row has landed. Survives a crash as
    /// evidence that indexing was attempted for this branch.
    Indexing,
    /// Parsed and streamed to the sink (including the indexed-empty terminal case).
    Indexed,
    /// The attempt reached a terminal failure; see `fail_reason`.
    Failed,
}

impl RepoStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Indexing => "indexing",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }
}

/// Why a terminal indexing attempt failed. The string is the only value
/// written to `gl_repository.fail_reason`, so the column is enum-bounded.
/// Empty unless `status` is `failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoFailReason {
    /// Exceeded the wall-clock job timeout.
    Timeout,
    /// A transient failure (e.g. archive fetch) that NATS will redeliver.
    Transient,
    /// A deterministic failure that dead-lettered the task (e.g. fatal parse error).
    Permanent,
}

impl RepoFailReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        }
    }
}

/// One row of per-branch indexing state, latest-wins on `_version`. Its `id`
/// is shared with the matching Branch node, so a query joins the two by id.
#[derive(Debug, Clone)]
pub struct RepositoryStatusRecord {
    pub traversal_path: String,
    pub project_id: i64,
    pub branch: String,
    pub status: RepoStatus,
    pub fail_reason: Option<RepoFailReason>,
    pub last_task_id: i64,
    pub last_commit: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl RepositoryStatusRecord {
    fn duration_ms(&self) -> i64 {
        match self.completed_at {
            Some(done) => done
                .signed_duration_since(self.started_at)
                .num_milliseconds()
                .max(0),
            None => 0,
        }
    }
}

/// Derived ID for a project x branch. Shares the hash with the Branch node so a
/// Repository and its Branch always carry the same id.
pub fn repository_id(project_id: i64, branch: &str) -> i64 {
    let mut hasher = FxHasher::default();
    project_id.hash(&mut hasher);
    branch.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

#[derive(Debug, Error)]
#[error("query failed: {0}")]
pub struct RepositoryStatusError(String);

#[async_trait]
pub trait RepositoryStatusStore: Send + Sync {
    async fn record(&self, record: &RepositoryStatusRecord) -> Result<(), RepositoryStatusError>;
}

pub struct ClickHouseRepositoryStatusStore {
    client: Arc<ArrowClickHouseClient>,
    table: String,
}

impl ClickHouseRepositoryStatusStore {
    pub fn new(client: Arc<ArrowClickHouseClient>, table_names: &CodeTableNames) -> Self {
        Self {
            client,
            table: table_names.repository.clone(),
        }
    }
}

#[async_trait]
impl RepositoryStatusStore for ClickHouseRepositoryStatusStore {
    async fn record(&self, record: &RepositoryStatusRecord) -> Result<(), RepositoryStatusError> {
        let started_at = record.started_at.format(TIMESTAMP_FORMAT).to_string();
        // An empty string parses to NULL, so the start row leaves completed_at
        // null without a Nullable bind param.
        let completed_at = record
            .completed_at
            .map(|ts| ts.format(TIMESTAMP_FORMAT).to_string())
            .unwrap_or_default();

        self.client
            .insert_query(&format!(
                r#"
                INSERT INTO {table}
                (id, traversal_path, project_id, branch, status, fail_reason, last_task_id, last_commit, started_at, completed_at, duration_ms)
                VALUES ({{id:Int64}}, {{traversal_path:String}}, {{project_id:Int64}}, {{branch:String}}, {{status:String}}, {{fail_reason:String}}, {{last_task_id:Int64}}, {{last_commit:String}}, {{started_at:String}}, parseDateTime64BestEffortOrNull({{completed_at:String}}, 6), {{duration_ms:Int64}})
            "#,
                table = self.table
            ))
            .param("id", repository_id(record.project_id, &record.branch))
            .param("traversal_path", &record.traversal_path)
            .param("project_id", record.project_id)
            .param("branch", &record.branch)
            .param("status", record.status.as_str())
            .param("fail_reason", record.fail_reason.map(RepoFailReason::as_str).unwrap_or_default())
            .param("last_task_id", record.last_task_id)
            .param("last_commit", record.last_commit.as_deref().unwrap_or_default())
            .param("started_at", started_at)
            .param("completed_at", completed_at)
            .param("duration_ms", record.duration_ms())
            .execute()
            .await
            .map_err(|e| RepositoryStatusError(e.to_string()))
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    #[derive(Default)]
    pub struct MockRepositoryStatusStore {
        records: Mutex<HashMap<(String, i64, String), RepositoryStatusRecord>>,
    }

    impl MockRepositoryStatusStore {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn get(
            &self,
            traversal_path: &str,
            project_id: i64,
            branch: &str,
        ) -> Option<RepositoryStatusRecord> {
            self.records
                .lock()
                .get(&(traversal_path.to_string(), project_id, branch.to_string()))
                .cloned()
        }
    }

    #[async_trait]
    impl RepositoryStatusStore for MockRepositoryStatusStore {
        async fn record(
            &self,
            record: &RepositoryStatusRecord,
        ) -> Result<(), RepositoryStatusError> {
            self.records.lock().insert(
                (
                    record.traversal_path.clone(),
                    record.project_id,
                    record.branch.clone(),
                ),
                record.clone(),
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_id_is_deterministic_and_non_negative() {
        for (project_id, branch) in [(1_i64, "main"), (42, "feature/x"), (i64::MAX, "main")] {
            let id = repository_id(project_id, branch);
            assert!(id >= 0, "repository_id({project_id}, {branch:?}) = {id}");
            assert_eq!(id, repository_id(project_id, branch));
        }
    }

    #[test]
    fn status_labels_are_stable() {
        assert_eq!(RepoStatus::Indexing.as_str(), "indexing");
        assert_eq!(RepoStatus::Indexed.as_str(), "indexed");
        assert_eq!(RepoStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn fail_reason_labels_are_stable() {
        assert_eq!(RepoFailReason::Timeout.as_str(), "timeout");
        assert_eq!(RepoFailReason::Transient.as_str(), "transient");
        assert_eq!(RepoFailReason::Permanent.as_str(), "permanent");
    }

    #[test]
    fn duration_is_zero_while_indexing() {
        let started_at = Utc::now();
        let indexing = RepositoryStatusRecord {
            traversal_path: "1/2/".to_string(),
            project_id: 2,
            branch: "main".to_string(),
            status: RepoStatus::Indexing,
            fail_reason: None,
            last_task_id: 1,
            last_commit: None,
            started_at,
            completed_at: None,
        };
        assert_eq!(indexing.duration_ms(), 0);

        let done = RepositoryStatusRecord {
            completed_at: Some(started_at + chrono::Duration::milliseconds(250)),
            ..indexing
        };
        assert_eq!(done.duration_ms(), 250);
    }
}
