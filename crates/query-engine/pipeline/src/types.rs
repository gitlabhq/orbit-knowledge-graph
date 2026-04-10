use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use compiler::{CompiledQueryContext, SecurityContext};
use ontology::Ontology;

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

    pub fn get_or_insert_default<T: Send + Sync + Default + 'static>(&mut self) -> &mut T {
        self.map
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(T::default()))
            .downcast_mut()
            .expect("type mismatch in TypeMap")
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
    /// Table prefix derived from the active schema version (e.g. `"v1_"`).
    /// Empty string means no prefix (schema version 0 or fresh install).
    pub table_prefix: String,
    /// Server-specific infrastructure (ClickHouse client, etc.)
    pub server_extensions: TypeMap,
    /// Inter-stage data flowing between pipeline stages.
    pub phases: TypeMap,
}

impl QueryPipelineContext {
    pub fn compiled(&self) -> Result<&Arc<CompiledQueryContext>, PipelineError> {
        self.compiled
            .as_ref()
            .ok_or_else(|| PipelineError::Compile {
                message: "compiled query context not yet available".into(),
                client_safe: false,
            })
    }

    pub fn security_context(&self) -> Result<&SecurityContext, PipelineError> {
        self.security_context
            .as_ref()
            .ok_or_else(|| PipelineError::Security("security context not yet available".into()))
    }
}
