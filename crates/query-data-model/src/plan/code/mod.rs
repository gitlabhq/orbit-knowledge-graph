mod apply;
mod builder;
mod pattern;

pub use builder::{CodeBuilder, Field, Matches, Trigrams};
pub use pattern::{Language, Pattern};

use crate::catalog::Catalog;
use crate::implementations::orbit_code::OrbitCode;
use crate::implementations::orbit_code::Repository;
use crate::plan::tracking::{self, Declared, Each, Plan, Ref, Rejected, Step};
use crate::{EntityId, PropertyId, StoreId};

#[derive(Debug)]
pub struct CodeLanguage;

pub type CodePlan = Plan<CodeLanguage>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeValue {
    /// A column the index's store lists, which every match carries.
    Column(PropertyId),
    Line,
    Capture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Index {
    pub(crate) store: usize,
    pub entity: EntityId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeOperation {
    Repositories {
        index: Index,
        projects: Vec<i64>,
    },
    /// The branch heads the search runs at, each a repository and a branch name.
    Branches(Vec<(i64, String)>),
    Restrict(Vec<i64>),
    Languages(Vec<Language>),
    Prefilter(Vec<String>),
    Structural(Pattern),
    Top(u64),
    Select(Vec<Ref>),
}

impl tracking::Language for CodeLanguage {
    type Operation = CodeOperation;
    type Value = CodeValue;

    fn references(operation: &mut CodeOperation, visit: &mut dyn FnMut(&mut Ref)) {
        if let CodeOperation::Select(fields) = operation {
            fields.iter_mut().for_each(visit);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodeError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("unknown code index {name}; available: {}", .available.join(", "))]
    UnknownIndex {
        name: String,
        available: Vec<String>,
    },
    #[error("a code search names at least one repository")]
    NoRepositories,
    #[error("the caller can read none of the requested repositories")]
    NoReadableRepository,
    #[error("{0} narrows files, so it comes before the structural match")]
    AfterMatch(&'static str),
    #[error("{0} is not searchable by {1}")]
    Unsupported(String, &'static str),
    #[error("trigram atom {0:?} is shorter than three characters")]
    ShortAtom(String),
    #[error("invalid pattern {pattern:?}: {reason}")]
    Pattern { pattern: String, reason: String },
    #[error("unknown language {0}")]
    UnknownLanguage(String),
    #[error("unknown field {name}; available: {}", .available.join(", "))]
    UnknownField {
        name: String,
        available: Vec<String>,
    },
    #[error("unknown capture {name}; available: {}", .available.join(", "))]
    UnknownCapture {
        name: String,
        available: Vec<String>,
    },
    #[error("a repository is named by its integer project key, not {0}")]
    RepositoryKey(String),
    #[error("top keeps at least one match")]
    EmptyTop,
    #[error("a code request needs {0}")]
    Incomplete(&'static str),
}

impl Catalog<OrbitCode> {
    /// The index of `entity` in `store`, the store placement put the read on.
    pub fn index(&self, store: StoreId, entity: EntityId) -> Result<Declared<Index>, CodeError> {
        let model = self.model();
        let held = model.store(store);
        if held.connection() != self.connection() || held.source(entity).is_none() {
            return Err(CodeError::UnknownIndex {
                name: format!("{}.{}", held.name, model.graph().entity(entity).name),
                available: self
                    .stores()
                    .flat_map(|(store, _)| store.sources().map(|source| source.entity))
                    .map(|indexed| model.graph().entity(indexed).name.clone())
                    .collect(),
            });
        }
        Ok(Declared::new(
            model.id(),
            Index {
                store: store.index(),
                entity,
            },
        ))
    }

    /// The columns `store` names the repository and branch of each row of `entity` by.
    pub fn repository(&self, store: StoreId, entity: EntityId) -> Option<Repository> {
        let held = self.model().store(store);
        self.stores()
            .find(|(candidate, _)| candidate.id() == held.id())?
            .1
            .repository(entity)
    }

    pub fn build<'c>(
        &'c self,
        construct: impl FnOnce(&mut CodeBuilder<'c>) -> Result<Matches, CodeError>,
    ) -> Result<CodePlan, CodeError> {
        let mut builder = CodeBuilder::new(self);
        let root = construct(&mut builder);
        builder.finish(root)
    }

    pub fn rebuild<'c>(
        &'c self,
        plan: CodePlan,
        visit: impl FnMut(
            &mut CodeBuilder<'c>,
            &mut Step<'_, '_, CodeBuilder<'c>>,
        ) -> Result<Matches, CodeError>,
    ) -> Result<CodePlan, CodeError> {
        let mut builder = CodeBuilder::new(self);
        let root = tracking::rebuild(&mut builder, &plan, &mut Each(visit));
        builder.finish(root)
    }
}
