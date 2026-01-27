use std::sync::Arc;

use crate::indexer::modules::INDEXER_TOPIC;
use crate::indexer::modules::sdlc::datalake::{Datalake, DatalakeClient, DatalakeQuery, ParamValue, QueryParams};
use crate::indexer::modules::sdlc::watermark_store::{
    ClickHouseWatermarkStore, WatermarkClient, WatermarkError, WatermarkStore,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use etl_engine::destination::BatchWriter;
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;

const SUBJECT: &str = "sdlc.user.indexing.requested";

const USER_QUERY: &str = r#"
SELECT
    id,
    username,
    email,
    name,
    first_name,
    last_name,
    state,
    public_email,
    preferred_language,
    last_activity_on,
    private_profile,
    admin,
    auditor,
    external,
    user_type,
    created_at,
    updated_at
FROM siphon_users
WHERE _siphon_replicated_at > {last_watermark:String}
  AND _siphon_replicated_at <= {watermark:String}
"#;

const TRANSFORMATION_SQL: &str = r#"
SELECT
    id, username, email, name, first_name, last_name, state,
    public_email, preferred_language,
    CAST(last_activity_on AS VARCHAR) AS last_activity_on,
    private_profile,
    admin AS is_admin,
    auditor AS is_auditor,
    external AS is_external,
    CASE user_type
        WHEN 0 THEN 'human'
        WHEN 1 THEN 'support_bot'
        WHEN 2 THEN 'alert_bot'
        WHEN 3 THEN 'visual_review_bot'
        WHEN 4 THEN 'service_user'
        WHEN 5 THEN 'ghost'
        WHEN 6 THEN 'project_bot'
        WHEN 7 THEN 'security_bot'
        WHEN 8 THEN 'automation_bot'
        WHEN 9 THEN 'security_policy_bot'
        WHEN 10 THEN 'admin_bot'
        WHEN 11 THEN 'service_account'
        WHEN 12 THEN 'placeholder'
        WHEN 13 THEN 'duo_code_review_bot'
        WHEN 14 THEN 'import_user'
        ELSE 'unknown'
    END AS user_type,
    CAST(created_at AS VARCHAR) AS created_at,
    CAST(updated_at AS VARCHAR) AS updated_at
FROM source_data
"#;

#[derive(Debug, Deserialize, Serialize)]
pub struct UserHandlerPayload {
    watermark: DateTime<Utc>,
}

impl Event for UserHandlerPayload {
    fn topic() -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }
}

fn build_query_params(last_watermark: &DateTime<Utc>, watermark: &DateTime<Utc>) -> QueryParams {
    QueryParams::from(vec![
        (
            "last_watermark",
            ParamValue::from(last_watermark.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        ),
        (
            "watermark",
            ParamValue::from(watermark.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        ),
    ])
}

pub struct UserHandler {
    watermark_store: Box<dyn WatermarkStore>,
    datalake: Box<dyn DatalakeQuery>,
}

impl UserHandler {
    pub fn new(datalake_client: DatalakeClient, watermark_client: WatermarkClient) -> Self {
        Self {
            watermark_store: Box::new(ClickHouseWatermarkStore::new(watermark_client)),
            datalake: Box::new(Datalake::new(datalake_client)),
        }
    }


    /// Transforms and writes a batch in a contained scope.
    /// The input batch and output batch are released as soon as possible.
    async fn transform_and_write_batch(
        batch: RecordBatch,
        writer: &dyn BatchWriter,
    ) -> Result<(), HandlerError> {
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| HandlerError::Processing(format!("failed to create mem table: {e}")))?;

        session
            .register_table("source_data", Arc::new(mem_table))
            .map_err(|e| HandlerError::Processing(format!("failed to register table: {e}")))?;

        let dataframe = session
            .sql(TRANSFORMATION_SQL)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to execute sql: {e}")))?;

        let results = dataframe
            .collect()
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to collect results: {e}")))?;

        let transformed = results
            .into_iter()
            .next()
            .ok_or_else(|| HandlerError::Processing("no result from transformation".to_string()))?;

        writer
            .write_batch(&[transformed])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write users: {e}")))?;

        Ok(())
    }
}

#[async_trait]
impl Handler for UserHandler {
    fn name(&self) -> &str {
        "user-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: UserHandlerPayload = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let last_watermark = match self.watermark_store.get_users_watermark().await {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let writer = context
            .destination
            .new_batch_writer("users")
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to create users writer: {e}")))?;

        let mut stream = self
            .datalake
            .query_arrow(
                USER_QUERY,
                Some(build_query_params(&last_watermark, &payload.watermark)),
            )
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to query users: {e}")))?;

        while let Some(result) = stream.next().await {
            let source_batch = result
                .map_err(|e| HandlerError::Processing(format!("failed to read batch: {e}")))?;
            if source_batch.num_rows() == 0 {
                continue;
            }

            Self::transform_and_write_batch(source_batch, writer.as_ref()).await?;
        }

        self.watermark_store
            .set_users_watermark(&payload.watermark)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to update watermark: {e}")))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::indexer::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use etl_engine::testkit::{
        MockDestination, MockMetricCollector, MockNatsServices, TestEnvelopeFactory,
    };
    use futures::stream;

    struct MockWatermarkStore {
        watermark_was_set: Arc<AtomicBool>,
        expected_watermark: DateTime<Utc>,
    }

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_users_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_users_watermark(
            &self,
            watermark: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            assert_eq!(watermark, &self.expected_watermark);
            self.watermark_was_set.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn get_namespaces_watermark(
            &self,
            _namespace_id: i64,
        ) -> Result<DateTime<Utc>, WatermarkError> {
            unimplemented!()
        }

        async fn set_namespaces_watermark(
            &self,
            _namespace_id: i64,
            _watermark: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            unimplemented!()
        }
    }

    struct MockDatalake;

    #[async_trait]
    impl DatalakeQuery for MockDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Option<QueryParams>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn handle_sets_watermark_after_processing() {
        let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let watermark_was_set = Arc::new(AtomicBool::new(false));

        let handler = UserHandler {
            watermark_store: Box::new(MockWatermarkStore {
                watermark_was_set: watermark_was_set.clone(),
                expected_watermark: watermark,
            }),
            datalake: Box::new(MockDatalake),
        };

        let payload = serde_json::json!({ "watermark": watermark.to_rfc3339() }).to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockMetricCollector::new()),
            Arc::new(MockNatsServices::new()),
        );

        handler
            .handle(context, envelope)
            .await
            .expect("handler should succeed");

        assert!(
            watermark_was_set.load(Ordering::SeqCst),
            "set_users_watermark should have been called"
        );
    }
}
