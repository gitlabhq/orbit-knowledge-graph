mod apply;
mod builder;

pub use builder::{Calls, Conditions, Field, LiveBuilder};

use crate::catalog::{Catalog, Guard};
use crate::implementations::live::{Endpoint, Live};
use crate::plan::logical::Literal;
use crate::plan::tracking::{self, Declared, Each, Plan, Ref, Rejected, Step};
use crate::{DataType, EntityId, PropertyId, StoreId};

#[derive(Debug)]
pub struct LiveLanguage;

pub type LivePlan = Plan<LiveLanguage>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveValue {
    pub property: PropertyId,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Operation {
    pub(crate) store: usize,
    pub entity: EntityId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Equals { field: Ref, value: Literal },
    Contains { field: Ref, text: String },
    OneOf { field: Ref, values: Vec<Literal> },
}

impl Condition {
    fn field(&mut self) -> &mut Ref {
        match self {
            Self::Equals { field, .. }
            | Self::Contains { field, .. }
            | Self::OneOf { field, .. } => field,
        }
    }
}

/// The workspaces a call reads: every one the caller's own sign-in reaches, when only the
/// entity's checks restrict its rows, or only these.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workspaces {
    Any,
    Only(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiveOperation {
    Call(Operation),
    Filter(Vec<Condition>),
    Window {
        field: Ref,
        from: Option<String>,
        to: Option<String>,
    },
    Workspaces(Workspaces),
    Limit(u64),
    Select(Vec<Ref>),
}

impl tracking::Language for LiveLanguage {
    type Operation = LiveOperation;
    type Value = LiveValue;

    fn references(operation: &mut LiveOperation, visit: &mut dyn FnMut(&mut Ref)) {
        match operation {
            LiveOperation::Filter(conditions) => conditions
                .iter_mut()
                .for_each(|condition| visit(condition.field())),
            LiveOperation::Window { field, .. } => visit(field),
            LiveOperation::Select(fields) => fields.iter_mut().for_each(visit),
            LiveOperation::Call(_) | LiveOperation::Workspaces(_) | LiveOperation::Limit(_) => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LiveError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("unknown live operation {name}; available: {}", .available.join(", "))]
    UnknownOperation {
        name: String,
        available: Vec<String>,
    },
    #[error("{operation} cannot push down {condition} on {field}; it filters on {}", .filters.join(", "))]
    NotPushedDown {
        operation: String,
        field: String,
        condition: &'static str,
        filters: Vec<String>,
    },
    #[error("{field}: {value:?} is not a {expected} value")]
    Value {
        field: String,
        value: Literal,
        expected: String,
    },
    #[error("a time window needs a bound on a pushed-down time field")]
    Window,
    #[error("the caller can read none of the workspaces")]
    NoWorkspace,
    #[error("unknown field {name}; available: {}", .available.join(", "))]
    UnknownField {
        name: String,
        available: Vec<String>,
    },
    #[error("a limit keeps at least one row")]
    EmptyLimit,
    #[error("a live request needs {0}")]
    Incomplete(&'static str),
}

impl Catalog<Live> {
    /// The operation that reads `entity` in `store`, the store placement put the read on.
    pub fn operation(
        &self,
        store: StoreId,
        entity: EntityId,
    ) -> Result<Declared<Operation>, LiveError> {
        let model = self.model();
        let held = model.store(store);
        if held.connection() != self.connection() || held.source(entity).is_none() {
            return Err(LiveError::UnknownOperation {
                name: format!("{}.{}", held.name, model.graph().entity(entity).name),
                available: self
                    .stores()
                    .flat_map(|(store, _)| store.sources().map(|source| source.entity))
                    .map(|served| model.graph().entity(served).name.clone())
                    .collect(),
            });
        }
        Ok(Declared::new(
            model.id(),
            Operation {
                store: store.index(),
                entity,
            },
        ))
    }

    pub fn endpoint(&self, entity: EntityId) -> Option<&Endpoint> {
        self.home(entity)?.1.endpoint(entity)
    }

    pub fn operation_guard(&self, operation: Operation) -> Option<&Guard> {
        self.model().stores()[operation.store].guard(operation.entity)
    }

    pub fn build<'c>(
        &'c self,
        construct: impl FnOnce(&mut LiveBuilder<'c>) -> Result<Calls, LiveError>,
    ) -> Result<LivePlan, LiveError> {
        let mut builder = LiveBuilder::new(self);
        let root = construct(&mut builder);
        builder.finish(root)
    }

    pub fn rebuild<'c>(
        &'c self,
        plan: LivePlan,
        visit: impl FnMut(
            &mut LiveBuilder<'c>,
            &mut Step<'_, '_, LiveBuilder<'c>>,
        ) -> Result<Calls, LiveError>,
    ) -> Result<LivePlan, LiveError> {
        let mut builder = LiveBuilder::new(self);
        let root = tracking::rebuild(&mut builder, &plan, &mut Each(visit));
        builder.finish(root)
    }
}
