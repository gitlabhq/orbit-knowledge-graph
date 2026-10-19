pub mod relational;

pub use relational::{Relational, RelationalBackend};

pub trait Mapping<S>: Sized {
    fn derive(graph: &crate::GraphCatalog, schema: &S) -> Result<Self, crate::DataModelError>;
}

#[derive(Debug)]
pub struct Layout<S, M: Mapping<S>> {
    schema: S,
    mapping: M,
}

impl<S, M: Mapping<S>> Layout<S, M> {
    pub(crate) fn build(
        graph: &crate::GraphCatalog,
        schema: S,
    ) -> Result<Self, crate::DataModelError> {
        let mapping = M::derive(graph, &schema)?;
        Ok(Self { schema, mapping })
    }

    pub fn schema(&self) -> &S {
        &self.schema
    }
    pub fn mapping(&self) -> &M {
        &self.mapping
    }
}
