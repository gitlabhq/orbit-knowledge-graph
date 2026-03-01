use crate::clickhouse::ArrowClickHouseClient;
use async_trait::async_trait;
use thiserror::Error;

use clickhouse_client::FromArrowColumn;

#[derive(Debug, Error)]
pub enum PushEventStoreError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("invalid data type: {0}")]
    InvalidType(String),
}

#[derive(Debug, Clone)]
pub struct LatestPushEvent {
    pub event_id: i64,
    pub commit_sha: String,
}

#[async_trait]
pub trait PushEventStore: Send + Sync {
    async fn latest_push_on_branch(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<LatestPushEvent>, PushEventStoreError>;
}

const LATEST_PUSH_ON_BRANCH_QUERY: &str = r#"
SELECT event_id, commit_to
FROM push_event_branch_latest
WHERE project_id = {project_id:Int64}
  AND ref = {branch:String}
  AND _siphon_deleted = false
ORDER BY event_id DESC
LIMIT 1
"#;

pub struct ClickHousePushEventStore {
    client: ArrowClickHouseClient,
}

impl ClickHousePushEventStore {
    pub fn new(client: ArrowClickHouseClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl PushEventStore for ClickHousePushEventStore {
    async fn latest_push_on_branch(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<LatestPushEvent>, PushEventStoreError> {
        let batches = self
            .client
            .query(LATEST_PUSH_ON_BRANCH_QUERY)
            .param("project_id", project_id)
            .param("branch", branch)
            .fetch_arrow()
            .await
            .map_err(|e| PushEventStoreError::Query(e.to_string()))?;

        let event_ids = i64::extract_column(&batches, 0)
            .map_err(|e| PushEventStoreError::InvalidType(e.to_string()))?;
        let commit_shas = String::extract_column(&batches, 1)
            .map_err(|e| PushEventStoreError::InvalidType(e.to_string()))?;

        let Some(event_id) = event_ids.first() else {
            return Ok(None);
        };

        let commit_sha = commit_shas
            .first()
            .ok_or_else(|| PushEventStoreError::InvalidType("missing commit_to column".into()))?;

        Ok(Some(LatestPushEvent {
            event_id: *event_id,
            commit_sha: commit_sha.clone(),
        }))
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    pub struct MockPushEventStore {
        pub events: Mutex<HashMap<(i64, String), LatestPushEvent>>,
    }

    impl MockPushEventStore {
        pub fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
            }
        }

        pub fn add_push_event(
            &self,
            project_id: i64,
            branch: &str,
            event_id: i64,
            commit_sha: &str,
        ) {
            self.events.lock().insert(
                (project_id, branch.to_string()),
                LatestPushEvent {
                    event_id,
                    commit_sha: commit_sha.to_string(),
                },
            );
        }
    }

    impl Default for MockPushEventStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl PushEventStore for MockPushEventStore {
        async fn latest_push_on_branch(
            &self,
            project_id: i64,
            branch: &str,
        ) -> Result<Option<LatestPushEvent>, PushEventStoreError> {
            let events = self.events.lock();
            Ok(events.get(&(project_id, branch.to_string())).cloned())
        }
    }
}
