use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use ontology::Ontology;
use query_engine::{CompiledQueryContext, SecurityContext};

use crate::error::PipelineError;

/// Type-erased map for storing values by their concrete type.
#[derive(Default)]
pub struct TypeMap {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl TypeMap {
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

    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.map
            .remove(&TypeId::of::<T>())
            .and_then(|b| (b as Box<dyn Any>).downcast().ok())
            .map(|b| *b)
    }
}

pub struct QueryPipelineContext {
    pub query_json: String,
    pub compiled: Option<Arc<CompiledQueryContext>>,
    pub ontology: Arc<Ontology>,
    pub security_context: Option<SecurityContext>,
    /// Server-specific infrastructure (ClickHouse client, etc.)
    pub extensions: TypeMap,
    /// Inter-stage data flowing between pipeline stages.
    pub phases: TypeMap,
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
