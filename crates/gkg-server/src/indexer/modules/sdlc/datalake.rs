use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_arrow::{ArrowFormat, Client};
pub(crate) use clickhouse_arrow::prelude::{ParamValue, QueryParams};
use futures::StreamExt;
use futures::stream::BoxStream;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DatalakeError {
    #[error("query failed: {0}")]
    Query(#[from] clickhouse_arrow::Error),
}

pub(crate) type RecordBatchStream<'a> = BoxStream<'a, Result<RecordBatch, DatalakeError>>;

#[async_trait]
pub(crate) trait DatalakeQuery: Send + Sync {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Option<QueryParams>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError>;
}

pub(crate) type DatalakeClient = Arc<Client<ArrowFormat>>;

pub(crate) struct Datalake {
    client: DatalakeClient,
}

impl Datalake {
    pub fn new(client: DatalakeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Option<QueryParams>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        let stream = self.client.query_params(sql, params, None).await?;
        let mapped = stream.map(|result| result.map_err(DatalakeError::from));
        Ok(Box::pin(mapped))
    }
}
