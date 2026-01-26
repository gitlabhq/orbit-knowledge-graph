use std::sync::Arc;

use arrow::array::{Array, TimestampMillisecondArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use clickhouse_arrow::prelude::{ParamValue, QueryParams};
use clickhouse_arrow::{ArrowFormat, Client};
use futures::StreamExt;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum WatermarkError {
    #[error("query failed: {0}")]
    Query(#[from] clickhouse_arrow::Error),

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
}

pub(crate) type WatermarkClient = Arc<Client<ArrowFormat>>;

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
        let mut stream = self.client.query(query, None).await?;

        let Some(result) = stream.next().await else {
            return Err(WatermarkError::NoData);
        };

        let batch = result?;
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
        let query = "INSERT INTO user_indexing_watermark (watermark) VALUES ({watermark:String})";

        let params = QueryParams::from(vec![(
            "watermark",
            ParamValue::from(watermark.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        )]);

        self.client
            .execute_params(query, Some(params), None)
            .await?;
        Ok(())
    }
}
