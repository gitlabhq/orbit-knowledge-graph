use std::sync::Arc;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use etl_engine::clickhouse::ArrowClickHouseClient;
use thiserror::Error;

pub(crate) const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

#[derive(Debug, Error)]
pub(crate) enum WatermarkError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("no data returned")]
    NoData,

    #[error("invalid timestamp type")]
    InvalidType,

    #[error("invalid timestamp value")]
    InvalidTimestamp,
}

#[async_trait]
pub(crate) trait WatermarkStore: Send + Sync {
    async fn get_users_watermark(&self) -> Result<DateTime<Utc>, WatermarkError>;
    async fn set_users_watermark(&self, watermark: &DateTime<Utc>) -> Result<(), WatermarkError>;

    async fn get_namespace_watermark(
        &self,
        namespace_id: i64,
    ) -> Result<DateTime<Utc>, WatermarkError>;

    async fn set_namespace_watermark(
        &self,
        namespace_id: i64,
        watermark: &DateTime<Utc>,
    ) -> Result<(), WatermarkError>;
}

pub(crate) type WatermarkClient = Arc<ArrowClickHouseClient>;

pub(crate) struct ClickHouseWatermarkStore {
    client: WatermarkClient,
}

impl ClickHouseWatermarkStore {
    pub fn new(client: WatermarkClient) -> Self {
        Self { client }
    }

    fn extract_timestamp(batches: Vec<RecordBatch>) -> Result<DateTime<Utc>, WatermarkError> {
        let batch = batches.into_iter().next().ok_or(WatermarkError::NoData)?;

        if batch.num_rows() == 0 {
            return Err(WatermarkError::NoData);
        }

        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or(WatermarkError::InvalidType)?;

        if timestamps.is_null(0) {
            return Err(WatermarkError::NoData);
        }

        let micros = timestamps.value(0);
        Utc.timestamp_micros(micros)
            .single()
            .ok_or(WatermarkError::InvalidTimestamp)
    }
}

#[async_trait]
impl WatermarkStore for ClickHouseWatermarkStore {
    async fn get_users_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
        let query = "SELECT argMax(watermark, _version) as watermark FROM user_indexing_watermark";
        let batches = self
            .client
            .query_arrow(query)
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Self::extract_timestamp(batches)
    }

    async fn set_users_watermark(&self, watermark: &DateTime<Utc>) -> Result<(), WatermarkError> {
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query("INSERT INTO user_indexing_watermark (watermark) VALUES ({watermark:String})")
            .param("watermark", formatted_watermark)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }

    async fn get_namespace_watermark(
        &self,
        namespace_id: i64,
    ) -> Result<DateTime<Utc>, WatermarkError> {
        let query = "SELECT argMax(watermark, _version) as watermark FROM namespace_indexing_watermark WHERE namespace = {namespace:Int64}";

        let batches = self
            .client
            .query(query)
            .param("namespace", namespace_id)
            .fetch_arrow()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Self::extract_timestamp(batches)
    }

    async fn set_namespace_watermark(
        &self,
        namespace_id: i64,
        watermark: &DateTime<Utc>,
    ) -> Result<(), WatermarkError> {
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query("INSERT INTO namespace_indexing_watermark (namespace, watermark) VALUES ({namespace:Int64}, {watermark:String})")
            .param("namespace", namespace_id)
            .param("watermark", formatted_watermark)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }
}
