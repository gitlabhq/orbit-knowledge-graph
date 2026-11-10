mod builder;

use crate::DataType;

pub use builder::{Field, Matches, SearchBuilder};

use crate::EntityId;
use crate::catalog::Catalog;
use crate::implementations::search::Search;
use crate::plan::tracking::{Construction, Declared, Language, Plan, Ref, Rejected};

#[derive(Debug)]
pub struct SearchLanguage;

pub type SearchPlan = Plan<SearchLanguage>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Text,
    Keyword(DataType),
    Score,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Index {
    store: usize,
    entity: EntityId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ranking {
    Relevance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchOperation {
    Search { index: Index, text: String },
    Rank(Ranking),
    Top(u64),
    Select(Vec<Ref>),
}

impl Language for SearchLanguage {
    type Operation = SearchOperation;
    type Value = FieldType;

    fn references(operation: &mut SearchOperation, visit: &mut dyn FnMut(&mut Ref)) {
        if let SearchOperation::Select(fields) = operation {
            fields.iter_mut().for_each(visit);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SearchError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("unknown index {name}; available: {}", .available.join(", "))]
    UnknownIndex {
        name: String,
        available: Vec<String>,
    },
    #[error("index {0} has no text fields to search")]
    NoText(String),
    #[error("index {0} is on another connection")]
    OtherConnection(String),
    #[error("unknown field {name}; available: {}", .available.join(", "))]
    UnknownField {
        name: String,
        available: Vec<String>,
    },
    #[error("top needs ranked matches")]
    Unranked,
    #[error("the matches are already ranked")]
    Ranked,
}

impl Catalog<Search> {
    pub fn index(&self, store: &str, entity: &str) -> Result<Declared<Index>, SearchError> {
        let unknown = || SearchError::UnknownIndex {
            name: format!("{store}.{entity}"),
            available: self
                .stores()
                .flat_map(|(store, _)| {
                    store.sources().map(move |source| {
                        let entity = &self.graph().entity(source.entity).name;
                        format!("{}.{entity}", store.name)
                    })
                })
                .collect(),
        };
        let model = self.model();
        let index = model
            .stores()
            .iter()
            .position(|candidate| {
                candidate.name == store && candidate.connection == self.connection()
            })
            .ok_or_else(unknown)?;
        let entity = model
            .graph()
            .entity_id(entity)
            .filter(|entity| model.stores()[index].source(*entity).is_some())
            .ok_or_else(unknown)?;
        Ok(Declared::new(
            model.id(),
            Index {
                store: index,
                entity,
            },
        ))
    }

    pub fn build<'c>(
        &'c self,
        construct: impl FnOnce(&mut SearchBuilder<'c>) -> Result<Matches, SearchError>,
    ) -> Result<SearchPlan, SearchError> {
        let mut builder = SearchBuilder::new(self, Construction::new(self.model().id()));
        let root = construct(&mut builder);
        builder.finish(root)
    }
}
