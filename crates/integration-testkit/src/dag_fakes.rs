//! Executors over the seeded fakes, so DAG scenarios run code and live stages end to end the
//! way `SeededColumnResolver` fakes Gitaly. Only tests register them; the server registers none.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use compiler::dag::Request;
use compiler::orbit_authorization::Key;
use compiler::remote::{Resolves, RowCheck};
use query_data_model::catalog::Row;
use query_data_model::implementations::live::DataExtraction;
use query_data_model::implementations::orbit_code::{CodeSearch, RequestField};
use query_engine::shared::dag::{Executed, Executor, ExecutorError, RowChecks, from_rows};
use serde_json::{Value, json};

use crate::seeded_remote::{SeededCodeSearch, SeededDataExtraction, seeded_namespaces};

fn unexpected(request: &Request) -> ExecutorError {
    ExecutorError(format!("this fake runs no {request:?}"))
}

fn batch(rows: &[Row], fields: &[String]) -> Result<Executed, ExecutorError> {
    Ok(Executed {
        batches: vec![from_rows(rows, fields)?],
        log: Vec::new(),
    })
}

/// Orbit Next's code indexing web servers, over `code_search.yaml`.
pub struct CodeExecutor(pub SeededCodeSearch);

#[async_trait]
impl Executor for CodeExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Code(search) = request else {
            return Err(unexpected(request));
        };
        let rows = self
            .0
            .search(search)
            .map_err(|error| ExecutorError(error.0))?;
        let fields: Vec<String> = search
            .fields
            .iter()
            .map(|field| match field {
                RequestField::Path => "path".to_string(),
                RequestField::Line => "line".to_string(),
                RequestField::Capture(name) | RequestField::Repository(name) => name.clone(),
            })
            .collect();
        batch(&rows, &fields)
    }
}

/// The Data Extraction Service over `data_extraction.yaml`, signed in as one user.
pub struct LiveExecutor {
    pub service: SeededDataExtraction,
    pub user: String,
}

#[async_trait]
impl Executor for LiveExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Live(call) = request else {
            return Err(unexpected(request));
        };
        let rows = self
            .service
            .extract(&self.user, call)
            .map_err(|error| ExecutorError(error.0))?;
        batch(&rows, &call.fields)
    }
}

/// The graph connection in tests that assert plans without a container: each query returns the
/// seeded rows, and each scope lookup the seeded namespaces.
pub struct SeededGraph(pub Vec<Row>);

#[async_trait]
impl Executor for SeededGraph {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        match request {
            Request::Query(_) if self.0.is_empty() => Ok(Executed::default()),
            Request::Query(_) => {
                let mut fields: Vec<String> = Vec::new();
                for key in self.0.iter().flat_map(|row| row.keys()) {
                    if !fields.contains(key) {
                        fields.push(key.clone());
                    }
                }
                batch(&self.0, &fields)
            }
            Request::Lookup(lookups) => {
                let seeded = seeded_namespaces();
                let mut batches = Vec::new();
                for lookup in lookups {
                    let row = |id: Value, path: &str| {
                        json!({compiler::remote::KEY: id, lookup.scope.as_str(): path})
                            .as_object()
                            .cloned()
                            .unwrap_or_default()
                    };
                    let rows: Vec<Row> = match lookup.resolves {
                        Resolves::Repositories => seeded
                            .repositories
                            .iter()
                            .map(|(id, path)| row(Value::from(*id), path.as_str()))
                            .collect(),
                        Resolves::Workspaces => seeded
                            .workspaces
                            .iter()
                            .map(|(id, path)| row(Value::from(id.as_str()), path.as_str()))
                            .collect(),
                    };
                    let fields = [compiler::remote::KEY.to_string(), lookup.scope.clone()];
                    batches.extend(batch(&rows, &fields)?.batches);
                }
                Ok(Executed {
                    batches,
                    log: Vec::new(),
                })
            }
            other => Err(unexpected(other)),
        }
    }
}

/// A Rails stand-in that allows the listed keys of every row check.
pub struct AllowedKeys(pub BTreeSet<Key>);

#[async_trait]
impl RowChecks for AllowedKeys {
    async fn allowed(&self, _: &RowCheck, keys: Vec<Key>) -> Result<BTreeSet<Key>, ExecutorError> {
        Ok(keys
            .into_iter()
            .filter(|key| self.0.contains(key))
            .collect())
    }
}

pub fn seeded_code() -> Arc<dyn Executor> {
    Arc::new(CodeExecutor(SeededCodeSearch::from_seed_file()))
}

pub fn seeded_live(user: &str) -> Arc<dyn Executor> {
    Arc::new(LiveExecutor {
        service: SeededDataExtraction::from_seed_file(),
        user: user.to_string(),
    })
}
