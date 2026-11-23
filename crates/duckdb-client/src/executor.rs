use std::sync::{Mutex, MutexGuard, PoisonError};

use arrow::record_batch::RecordBatch;
use compiler::ParameterizedQuery;
use pipeline::{PipelineError, PipelineObserver, QueryPipelineContext};
use shared::QueryExecutor;

use crate::DuckDbClient;

pub struct DuckDbExecutor {
    client: Mutex<DuckDbClient>,
}

impl DuckDbExecutor {
    pub fn new(client: DuckDbClient) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    pub fn client(&self) -> MutexGuard<'_, DuckDbClient> {
        self.client.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl QueryExecutor for DuckDbExecutor {
    async fn run(
        &self,
        query: &ParameterizedQuery,
        _ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Vec<RecordBatch>, PipelineError> {
        let mut bound: Vec<_> = query
            .params
            .iter()
            .filter_map(|(name, param)| {
                Some((name.strip_prefix('p')?.parse::<usize>().ok()?, &param.value))
            })
            .collect();
        bound.sort_by_key(|(position, _)| *position);
        let values: Vec<_> = bound.into_iter().map(|(_, value)| value.clone()).collect();
        self.client()
            .query_arrow_json(&query.sql, &values)
            .map_err(|error| PipelineError::Execution(format!("{error}\n{}", query.render())))
    }
}
