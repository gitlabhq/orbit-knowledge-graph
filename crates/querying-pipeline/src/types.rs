use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use query_engine::{CompiledQueryContext, ResultContext, SecurityContext};
use querying_types::{QueryResult, ResourceAuthorization};
use serde_json::Value;

use crate::error::PipelineError;

/// Type-erased extension map for pipeline context.
/// Server-specific stages insert concrete types (e.g. ClickHouse client, gRPC streams),
/// and retrieve them by type. Local pipelines leave this empty.
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(val));
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
    }

    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut())
    }
}

pub struct QueryPipelineContext {
    pub query_json: String,
    pub compiled: Option<Arc<CompiledQueryContext>>,
    pub ontology: Arc<Ontology>,
    pub security_context: Option<SecurityContext>,
    pub extensions: Extensions,
}

impl QueryPipelineContext {
    pub fn compiled(&self) -> Result<&Arc<CompiledQueryContext>, PipelineError> {
        self.compiled.as_ref().ok_or_else(|| {
            PipelineError::Compile("compiled query context not yet available".into())
        })
    }

    pub fn security_context(&self) -> Result<&SecurityContext, PipelineError> {
        self.security_context
            .as_ref()
            .ok_or_else(|| PipelineError::Security("security context not yet available".into()))
    }
}

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
}

pub struct ExtractionOutput {
    pub query_result: QueryResult,
}

pub struct AuthorizationOutput {
    pub query_result: QueryResult,
    pub authorizations: Vec<ResourceAuthorization>,
}

pub struct RedactionOutput {
    pub query_result: QueryResult,
    pub redacted_count: usize,
}

pub struct HydrationOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redacted_count: usize,
}

pub struct PipelineOutput {
    pub formatted_result: Value,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
    pub redacted_count: usize,
}
