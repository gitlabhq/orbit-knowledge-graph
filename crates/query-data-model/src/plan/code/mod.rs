mod apply;
mod builder;
mod pattern;

pub use builder::{CodeBuilder, Field, Matches, Trigrams};
pub use pattern::{Language, Pattern};

use crate::EntityId;
use crate::catalog::Catalog;
use crate::implementations::orbit_code::OrbitCode;
use crate::plan::tracking::{self, Declared, Each, Plan, Ref, Rejected, Step};

#[derive(Debug)]
pub struct CodeLanguage;

pub type CodePlan = Plan<CodeLanguage>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeValue {
    /// The repository a match is in, named after the property the code store declares.
    Repository,
    Path,
    Line,
    Capture,
    /// The branch whose head a match is at.
    Branch,
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
    #[error("top keeps at least one match")]
    EmptyTop,
    #[error("a code request needs {0}")]
    Incomplete(&'static str),
}

impl Catalog<OrbitCode> {
    pub fn index(&self, store: &str, entity: &str) -> Result<Declared<Index>, CodeError> {
        let model = self.model();
        let found = model
            .stores()
            .iter()
            .position(|candidate| {
                candidate.name == store && candidate.connection == self.connection()
            })
            .and_then(|index| {
                let entity = model.graph().entity_id(entity)?;
                model.stores()[index].source(entity)?;
                Some(Index {
                    store: index,
                    entity,
                })
            });
        found
            .map(|index| Declared::new(model.id(), index))
            .ok_or_else(|| CodeError::UnknownIndex {
                name: format!("{store}.{entity}"),
                available: self
                    .stores()
                    .flat_map(|(store, _)| {
                        store.sources().map(move |source| {
                            format!(
                                "{}.{}",
                                store.name,
                                model.graph().entity(source.entity).name
                            )
                        })
                    })
                    .collect(),
            })
    }

    /// The key that names a match's repository: the foreign key the read's `via` chain starts
    /// from; none for a read only its checks restrict.
    /// The property that names the repository of each row of `entity`: the one its store
    /// declares, or the foreign key its `via` chain starts from.
    pub fn repository(&self, entity: EntityId) -> Option<crate::PropertyId> {
        let (store, facts) = self.home(entity)?;
        match &store.guard(entity)?.rule {
            crate::catalog::Rule::Via(hops) => {
                let key = self.model().foreign_key(hops.first()?.variant)?;
                (key.holder == crate::Endpoint::Source).then_some(key.property)
            }
            _ => facts.repository(entity),
        }
    }

    pub fn repository_key(&self, index: Index) -> Option<&str> {
        let property = self.repository(index.entity)?;
        Some(self.graph().property(property).name.as_str())
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
