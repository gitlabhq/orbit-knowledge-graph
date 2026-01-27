use std::sync::Arc;

use arrow::array::{Array, TimestampMillisecondArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use etl_engine::clickhouse::ArrowClickHouseClient;
use thiserror::Error;

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

impl From<clickhouse::error::Error> for WatermarkError {
    fn from(err: clickhouse::error::Error) -> Self {
        WatermarkError::Query(err.to_string())
    }
}

#[async_trait]
pub(crate) trait WatermarkStore: Send + Sync {
    async fn get_users_watermark(&self) -> Result<DateTime<Utc>, WatermarkError>;
    async fn set_users_watermark(&self, watermark: &DateTime<Utc>) -> Result<(), WatermarkError>;
}

pub(crate) type WatermarkClient = Arc<ArrowClickHouseClient>;

pub(crate) struct ClickHouseWatermarkStore {
    client: WatermarkClient,
}

impl ClickHouseWatermarkStore {
    pub fn new(client: WatermarkClient) -> Self {
        Self { client }
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

        let batch = batches.into_iter().next().ok_or(WatermarkError::NoData)?;

        if batch.num_rows() == 0 {
            return Err(WatermarkError::NoData);
        }

        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or(WatermarkError::InvalidType)?;

        if timestamps.is_null(0) {
            return Err(WatermarkError::NoData);
        }

        let millis = timestamps.value(0);
        Utc.timestamp_millis_opt(millis)
            .single()
            .ok_or(WatermarkError::InvalidTimestamp)
    }

    async fn set_users_watermark(&self, watermark: &DateTime<Utc>) -> Result<(), WatermarkError> {
        let formatted_watermark = watermark.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        self.client
            .query("INSERT INTO user_indexing_watermark (watermark) VALUES ({watermark:String})")
            .param("watermark", formatted_watermark)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }
}
