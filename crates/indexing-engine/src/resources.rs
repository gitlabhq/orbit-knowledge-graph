use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::Arc;

pub trait FromResources: Sized {
    fn from_resources(resources: &Resources) -> Result<Self, MissingResource>;
}

#[derive(Clone, Default)]
pub struct Resources {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("no resource of type {type_name} was provided")]
pub struct MissingResource {
    type_name: &'static str,
}

impl Resources {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn provide<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.values.insert(TypeId::of::<T>(), Arc::new(value));
        self
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Result<Arc<T>, MissingResource> {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
            .ok_or(MissingResource {
                type_name: type_name::<T>(),
            })
    }
}
