//! The executors the demo registers: ClickHouse through the server's own executor, and the
//! remote connections over HTTP to the loopback fakes. Each records a sample of what it returned.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use async_trait::async_trait;
use compiler::dag::Request;
use query_data_model::catalog::Row;
use query_data_model::implementations::orbit_code::RequestField;
use query_engine::shared::dag::{Executed, Executor, ExecutorError, from_rows};
use serde::Serialize;
use serde_json::Value;

use crate::client::post;
use crate::fakes::{orbit_next, slack};

const SAMPLE: usize = 5;

fn failed(error: anyhow::Error) -> ExecutorError {
    ExecutorError(format!("{error:#}"))
}

fn unexpected(request: &Request) -> ExecutorError {
    ExecutorError(format!("this connection runs no {}", kind(request)))
}

pub fn kind(request: &Request) -> &'static str {
    match request {
        Request::Query(_) => "query",
        Request::Hydration(_) => "hydration",
        Request::Lookup(_) => "scope lookup",
        Request::Code(_) => "structural search",
        Request::Live(_) => "extraction",
        Request::Engine { .. } => "engine join",
    }
}

/// Orbit Next's code indexing web servers.
pub struct CodeHttp(pub SocketAddr);

#[async_trait]
impl Executor for CodeHttp {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Code(search) = request else {
            return Err(unexpected(request));
        };
        let rows: Vec<Row> = post(self.0, orbit_next::PATH, &[], search)
            .await
            .map_err(failed)?;
        let fields: Vec<String> = search
            .fields
            .iter()
            .map(|field| match field {
                RequestField::Path => "path".to_string(),
                RequestField::Line => "line".to_string(),
                RequestField::Capture(name)
                | RequestField::Repository(name)
                | RequestField::Branch(name) => name.clone(),
            })
            .collect();
        batch(&rows, &fields)
    }
}

/// The Data Extraction Service, signed in to Slack with the caller's handle.
pub struct LiveHttp {
    pub address: SocketAddr,
    pub handle: String,
}

#[async_trait]
impl Executor for LiveHttp {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Live(call) = request else {
            return Err(unexpected(request));
        };
        let bearer = format!("Bearer {}", self.handle);
        let rows: Vec<Row> = post(
            self.address,
            slack::PATH,
            &[("Authorization", &bearer)],
            call,
        )
        .await
        .map_err(failed)?;
        batch(&rows, &call.fields)
    }
}

fn batch(rows: &[Row], fields: &[String]) -> Result<Executed, ExecutorError> {
    Ok(Executed {
        batches: vec![from_rows(rows, fields)?],
        log: Vec::new(),
    })
}

/// What one executor call returned, before the stage's row checks.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    pub request: &'static str,
    pub count: usize,
    pub rows: Vec<Row>,
}

pub type Samples = Arc<Mutex<Vec<Sample>>>;

/// Records a sample of every result the wrapped executor returns.
pub struct Sampled {
    pub inner: Arc<dyn Executor>,
    pub samples: Samples,
}

#[async_trait]
impl Executor for Sampled {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let executed = self.inner.execute(request).await?;
        let sample = Sample {
            request: kind(request),
            count: executed.batches.iter().map(RecordBatch::num_rows).sum(),
            rows: json_rows(&executed.batches, SAMPLE),
        };
        self.samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(sample);
        Ok(executed)
    }
}

/// Up to `limit` rows of Arrow batches as JSON, each value as Arrow displays it.
pub fn json_rows(batches: &[RecordBatch], limit: usize) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            if rows.len() == limit {
                return rows;
            }
            let mut row = Row::new();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                let value = match column.is_null(index) {
                    true => Value::Null,
                    false => array_value_to_string(column, index)
                        .map(Value::from)
                        .unwrap_or_default(),
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(row);
        }
    }
    rows
}
