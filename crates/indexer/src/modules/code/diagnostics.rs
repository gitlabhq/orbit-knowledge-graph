use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use code_graph::v2::FileReason;
use thiserror::Error;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};

const BRANCH_EVENTS_TABLE: &str = "code_indexing_branch_events";
const FILE_EVENTS_TABLE: &str = "code_indexing_file_events";

/// Status of a code-indexing branch attempt. `as_str` is the single producer
/// for the wire value stored in `code_indexing_branch_events.status`; the
/// label-stability test guards it so a rename cannot silently break history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchStatus {
    Indexing,
    Indexed,
    Failed,
}

impl BranchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Indexing => "indexing",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }
}

/// Why a branch attempt failed. Written to `fail_reason` only for
/// [`BranchStatus::Failed`]; empty otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchFailReason {
    Timeout,
    Transient,
    Permanent,
}

impl BranchFailReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BranchEvent {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub task_id: i64,
    pub status: BranchStatus,
    pub fail_reason: Option<BranchFailReason>,
    pub commit: Option<String>,
    pub started_at: DateTime<Utc>,
    pub duration_ms: i64,
}

/// Identifies the task a batch of file events belongs to.
#[derive(Debug, Clone)]
pub struct TaskRef {
    pub project_id: i64,
    pub branch: String,
    pub task_id: i64,
}

#[derive(Debug, Error)]
pub enum DiagnosticsError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("arrow batch build failed: {0}")]
    Arrow(String),
}

/// Records per-branch and per-file code-indexing outcomes to the unversioned
/// diagnostics tables. Every write is best-effort at the call site: a failure
/// here must never fail the indexing task.
#[async_trait]
pub trait DiagnosticsStore: Send + Sync {
    async fn record_branch_event(&self, event: &BranchEvent) -> Result<(), DiagnosticsError>;

    async fn record_file_events(
        &self,
        task: &TaskRef,
        reasons: &[(String, FileReason)],
    ) -> Result<(), DiagnosticsError>;
}

pub struct ClickHouseDiagnosticsStore {
    client: Arc<ArrowClickHouseClient>,
}

impl ClickHouseDiagnosticsStore {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    fn file_events_schema() -> Schema {
        Schema::new(vec![
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("task_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("reason", DataType::Utf8, false),
            Field::new(
                "occurred_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ])
    }

    fn build_file_events_batch(
        task: &TaskRef,
        reasons: &[(String, FileReason)],
        occurred_at: DateTime<Utc>,
    ) -> Result<RecordBatch, DiagnosticsError> {
        let occurred_micros = occurred_at.timestamp_micros();
        let mut project_id = Int64Builder::new();
        let mut branch = StringBuilder::new();
        let mut task_id = Int64Builder::new();
        let mut path = StringBuilder::new();
        let mut reason = StringBuilder::new();
        let mut occurred = TimestampMicrosecondBuilder::new().with_timezone("UTC");

        for (file_path, file_reason) in reasons {
            project_id.append_value(task.project_id);
            branch.append_value(&task.branch);
            task_id.append_value(task.task_id);
            path.append_value(file_path);
            reason.append_value(file_reason.to_string());
            occurred.append_value(occurred_micros);
        }

        RecordBatch::try_new(
            Arc::new(Self::file_events_schema()),
            vec![
                Arc::new(project_id.finish()),
                Arc::new(branch.finish()),
                Arc::new(task_id.finish()),
                Arc::new(path.finish()),
                Arc::new(reason.finish()),
                Arc::new(occurred.finish()),
            ],
        )
        .map_err(|e| DiagnosticsError::Arrow(e.to_string()))
    }
}

#[async_trait]
impl DiagnosticsStore for ClickHouseDiagnosticsStore {
    async fn record_branch_event(&self, event: &BranchEvent) -> Result<(), DiagnosticsError> {
        let started_at = event.started_at.format(TIMESTAMP_FORMAT).to_string();
        let fail_reason = event
            .fail_reason
            .map(BranchFailReason::as_str)
            .unwrap_or("");

        self.client
            .insert_query(&format!(
                r#"
                INSERT INTO {BRANCH_EVENTS_TABLE}
                (project_id, branch, traversal_path, task_id, status, fail_reason, commit, started_at, duration_ms)
                VALUES ({{project_id:Int64}}, {{branch:String}}, {{traversal_path:String}}, {{task_id:Int64}}, {{status:String}}, {{fail_reason:String}}, {{commit:String}}, {{started_at:String}}, {{duration_ms:Int64}})
            "#
            ))
            .param("project_id", event.project_id)
            .param("branch", &event.branch)
            .param("traversal_path", &event.traversal_path)
            .param("task_id", event.task_id)
            .param("status", event.status.as_str())
            .param("fail_reason", fail_reason)
            .param("commit", event.commit.as_deref().unwrap_or_default())
            .param("started_at", started_at)
            .param("duration_ms", event.duration_ms)
            .execute()
            .await
            .map_err(|e| DiagnosticsError::Query(e.to_string()))
    }

    async fn record_file_events(
        &self,
        task: &TaskRef,
        reasons: &[(String, FileReason)],
    ) -> Result<(), DiagnosticsError> {
        if reasons.is_empty() {
            return Ok(());
        }
        let batch = Self::build_file_events_batch(task, reasons, Utc::now())?;
        self.client
            .insert_arrow(FILE_EVENTS_TABLE, &[batch])
            .await
            .map_err(|e| DiagnosticsError::Query(e.to_string()))
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    pub struct MockDiagnosticsStore {
        pub branch_events: Mutex<Vec<BranchEvent>>,
    }

    impl MockDiagnosticsStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl DiagnosticsStore for MockDiagnosticsStore {
        async fn record_branch_event(&self, event: &BranchEvent) -> Result<(), DiagnosticsError> {
            self.branch_events.lock().push(event.clone());
            Ok(())
        }

        async fn record_file_events(
            &self,
            _task: &TaskRef,
            _reasons: &[(String, FileReason)],
        ) -> Result<(), DiagnosticsError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph::v2::{FileFault, FileSkip};

    #[test]
    fn branch_status_labels_are_stable() {
        assert_eq!(BranchStatus::Indexing.as_str(), "indexing");
        assert_eq!(BranchStatus::Indexed.as_str(), "indexed");
        assert_eq!(BranchStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn branch_fail_reason_labels_are_stable() {
        assert_eq!(BranchFailReason::Timeout.as_str(), "timeout");
        assert_eq!(BranchFailReason::Transient.as_str(), "transient");
        assert_eq!(BranchFailReason::Permanent.as_str(), "permanent");
    }

    #[test]
    fn file_events_batch_matches_schema_and_reason_strings() {
        let task = TaskRef {
            project_id: 7,
            branch: "main".to_string(),
            task_id: 42,
        };
        let reasons = vec![
            (
                "a.rb".to_string(),
                FileReason::Fault(FileFault::InvalidUtf8),
            ),
            ("b.rb".to_string(), FileReason::Skip(FileSkip::Oversize)),
        ];

        let batch =
            ClickHouseDiagnosticsStore::build_file_events_batch(&task, &reasons, Utc::now())
                .expect("batch builds");

        assert_eq!(
            batch.schema().as_ref(),
            &ClickHouseDiagnosticsStore::file_events_schema()
        );
        assert_eq!(batch.num_rows(), 2);

        let reason_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("reason column is Utf8");
        assert_eq!(reason_col.value(0), "fault_invalid_utf8");
        assert_eq!(reason_col.value(1), "skip_oversize");
    }
}
