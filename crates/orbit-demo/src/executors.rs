//! The executors the demo registers: ClickHouse through the server's own executor, and the
//! remote connections over HTTP to the loopback fakes. Each records a sample of what it returned.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use async_trait::async_trait;
use compiler::dag::{Contract, Handle, Output, Request};
use query_data_model::Model;
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
        Request::Code(_) => "structural search",
        Request::Live(_) => "extraction",
        Request::Combine { .. } => "combine",
    }
}

/// Orbit Next's code indexing web servers.
pub struct CodeHttp(pub SocketAddr);

#[async_trait]
impl Executor for CodeHttp {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
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
                RequestField::Line => "line".to_string(),
                RequestField::Column(name)
                | RequestField::Capture(name)
                | RequestField::Repository(name)
                | RequestField::Branch(name) => name.clone(),
            })
            .collect();
        batch(&rows, &fields, contract)
    }
}

/// The Data Extraction Service, signed in to Slack with the caller's handle.
pub struct LiveHttp {
    pub address: SocketAddr,
    pub handle: String,
}

#[async_trait]
impl Executor for LiveHttp {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
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
        batch(&rows, &call.fields, contract)
    }
}

/// A remote stage's rows in the order and types of the stage's contract.
fn batch(
    rows: &[Row],
    fields: &[String],
    contract: Option<&Contract>,
) -> Result<Executed, ExecutorError> {
    let contract = contract
        .ok_or_else(|| ExecutorError("a remote stage request carries a contract".into()))?;
    Ok(Executed {
        batches: vec![from_rows(rows, fields, contract)?],
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
    pub model: Arc<Model>,
}

#[async_trait]
impl Executor for Sampled {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        let executed = self.inner.execute(request, contract).await?;
        let sample = Sample {
            request: kind(request),
            count: executed.batches.iter().map(RecordBatch::num_rows).sum(),
            rows: named(json_rows(&executed.batches, SAMPLE), contract, &self.model),
        };
        self.samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(sample);
        Ok(executed)
    }
}

/// A stage's rows come back named by contract position; these name each column after the
/// binding, property, edge field or match part it holds.
fn named(rows: Vec<Row>, contract: Option<&Contract>, model: &Model) -> Vec<Row> {
    let Some(contract) = contract else {
        return rows;
    };
    let names: Vec<String> = contract
        .columns
        .iter()
        .map(|column| match &column.output {
            Output::Node {
                binding,
                handle: Handle::Property(property),
            } => {
                format!("{binding}.{}", model.graph().property(*property).name)
            }
            Output::Node {
                binding,
                handle: Handle::Identity(0),
            } => format!("{binding}.id"),
            Output::Node {
                binding,
                handle: Handle::Identity(part),
            } => format!("{binding}.id.{part}"),
            Output::Edge { step, field } => {
                format!("edge{step}.{}", format!("{field:?}").to_lowercase())
            }
            Output::Path { step } => format!("path{step}"),
            Output::Line => "match.line".into(),
            Output::Capture(name) => format!("match.{name}"),
        })
        .collect();
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(position, value)| {
                    let name = position
                        .parse::<usize>()
                        .ok()
                        .and_then(|at| names.get(at))
                        .cloned();
                    (name.unwrap_or(position), value)
                })
                .collect()
        })
        .collect()
}

// Arrow names the contract's UTC zone, which it can only display with chrono-tz, so a UTC
// timestamp displays zone-less with a Z.
fn display(column: &ArrayRef, index: usize) -> Value {
    if let DataType::Timestamp(TimeUnit::Microsecond, Some(_)) = column.data_type() {
        let utc = column
            .as_primitive::<TimestampMicrosecondType>()
            .clone()
            .with_timezone_opt(None::<String>);
        return array_value_to_string(&utc, index)
            .map_or(Value::Null, |text| Value::from(text + "Z"));
    }
    array_value_to_string(column, index).map_or(Value::Null, Value::from)
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
                    false => display(column, index),
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(row);
        }
    }
    rows
}
