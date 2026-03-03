use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::handler::HandlerError;
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;

use super::cursor_paginator::{CursorValue, deserialize_cursor, serialize_cursor};

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

#[derive(Debug, Clone)]
pub(crate) struct InProgressCursor {
    pub cursor_values: Vec<CursorValue>,
    pub upper_watermark: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct WatermarkState {
    pub watermark: DateTime<Utc>,
    pub in_progress: Option<InProgressCursor>,
}

#[async_trait]
pub(crate) trait CursorReporter: Send + Sync {
    async fn on_page_complete(&self, cursor_values: &[CursorValue]) -> Result<(), HandlerError>;
}

#[async_trait]
pub(crate) trait WatermarkStore: Send + Sync {
    async fn get_namespace_state(
        &self,
        namespace_id: i64,
        entity: &str,
    ) -> Result<WatermarkState, WatermarkError>;

    async fn save_namespace_cursor(
        &self,
        namespace_id: i64,
        entity: &str,
        cursor: &InProgressCursor,
    ) -> Result<(), WatermarkError>;

    async fn complete_namespace_watermark(
        &self,
        namespace_id: i64,
        entity: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), WatermarkError>;

    async fn get_global_state(&self) -> Result<WatermarkState, WatermarkError>;

    async fn save_global_cursor(
        &self,
        cursor: &InProgressCursor,
    ) -> Result<(), WatermarkError>;

    async fn complete_global_watermark(
        &self,
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

    fn extract_watermark_state(
        batches: Vec<RecordBatch>,
    ) -> Result<WatermarkState, WatermarkError> {
        let batch = batches.into_iter().next().ok_or(WatermarkError::NoData)?;

        if batch.num_rows() == 0 {
            return Err(WatermarkError::NoData);
        }

        let watermark_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or(WatermarkError::InvalidType)?;

        if watermark_col.is_null(0) {
            return Err(WatermarkError::NoData);
        }

        let watermark = Utc
            .timestamp_micros(watermark_col.value(0))
            .single()
            .ok_or(WatermarkError::InvalidTimestamp)?;

        let upper_watermark_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>();

        let cursor_col = batch.column(2).as_any().downcast_ref::<StringArray>();

        let in_progress = match (upper_watermark_col, cursor_col) {
            (Some(uw_col), Some(c_col)) if !uw_col.is_null(0) && !c_col.is_null(0) => {
                let upper_wm = Utc
                    .timestamp_micros(uw_col.value(0))
                    .single()
                    .ok_or(WatermarkError::InvalidTimestamp)?;
                let cursor_json = c_col.value(0);
                let cursor_values = deserialize_cursor(cursor_json)
                    .map_err(|e| WatermarkError::Query(format!("invalid cursor JSON: {e}")))?;
                Some(InProgressCursor {
                    cursor_values,
                    upper_watermark: upper_wm,
                })
            }
            _ => None,
        };

        Ok(WatermarkState {
            watermark,
            in_progress,
        })
    }
}

#[async_trait]
impl WatermarkStore for ClickHouseWatermarkStore {
    async fn get_namespace_state(
        &self,
        namespace_id: i64,
        entity: &str,
    ) -> Result<WatermarkState, WatermarkError> {
        let query = "\
            SELECT \
                argMax(watermark, _version) as watermark, \
                argMax(upper_watermark, _version) as upper_watermark, \
                argMax(cursor_json, _version) as cursor_json \
            FROM namespace_indexing_watermark \
            WHERE namespace = {namespace:Int64} AND entity = {entity:String}";

        let batches = self
            .client
            .query(query)
            .param("namespace", namespace_id)
            .param("entity", entity)
            .fetch_arrow()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Self::extract_watermark_state(batches)
    }

    async fn save_namespace_cursor(
        &self,
        namespace_id: i64,
        entity: &str,
        cursor: &InProgressCursor,
    ) -> Result<(), WatermarkError> {
        let formatted_upper = cursor.upper_watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json = serialize_cursor(&cursor.cursor_values);

        self.client
            .query(
                "\
                INSERT INTO namespace_indexing_watermark \
                (namespace, entity, watermark, upper_watermark, cursor_json) \
                SELECT \
                    {namespace:Int64}, \
                    {entity:String}, \
                    argMax(watermark, _version), \
                    {upper_watermark:String}, \
                    {cursor_json:String} \
                FROM namespace_indexing_watermark \
                WHERE namespace = {namespace:Int64} AND entity = {entity:String}",
            )
            .param("namespace", namespace_id)
            .param("entity", entity)
            .param("upper_watermark", formatted_upper)
            .param("cursor_json", cursor_json)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }

    async fn complete_namespace_watermark(
        &self,
        namespace_id: i64,
        entity: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), WatermarkError> {
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query(
                "\
                INSERT INTO namespace_indexing_watermark \
                (namespace, entity, watermark, upper_watermark, cursor_json) \
                VALUES ({namespace:Int64}, {entity:String}, {watermark:String}, NULL, NULL)",
            )
            .param("namespace", namespace_id)
            .param("entity", entity)
            .param("watermark", formatted_watermark)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }

    async fn get_global_state(&self) -> Result<WatermarkState, WatermarkError> {
        let query = "\
            SELECT \
                argMax(watermark, _version) as watermark, \
                argMax(upper_watermark, _version) as upper_watermark, \
                argMax(cursor_json, _version) as cursor_json \
            FROM global_indexing_watermark";

        let batches = self
            .client
            .query_arrow(query)
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Self::extract_watermark_state(batches)
    }

    async fn save_global_cursor(
        &self,
        cursor: &InProgressCursor,
    ) -> Result<(), WatermarkError> {
        let formatted_upper = cursor.upper_watermark.format(TIMESTAMP_FORMAT).to_string();
        let cursor_json = serialize_cursor(&cursor.cursor_values);

        self.client
            .query(
                "\
                INSERT INTO global_indexing_watermark \
                (watermark, upper_watermark, cursor_json) \
                SELECT \
                    argMax(watermark, _version), \
                    {upper_watermark:String}, \
                    {cursor_json:String} \
                FROM global_indexing_watermark",
            )
            .param("upper_watermark", formatted_upper)
            .param("cursor_json", cursor_json)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }

    async fn complete_global_watermark(
        &self,
        watermark: &DateTime<Utc>,
    ) -> Result<(), WatermarkError> {
        let formatted_watermark = watermark.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query(
                "\
                INSERT INTO global_indexing_watermark \
                (watermark, upper_watermark, cursor_json) \
                VALUES ({watermark:String}, NULL, NULL)",
            )
            .param("watermark", formatted_watermark)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }
}
