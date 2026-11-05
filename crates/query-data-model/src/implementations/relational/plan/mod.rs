mod builder;
mod error;
mod expr;
mod operation;
mod relation;
mod stage;

use std::marker::PhantomData;

use ontology::DataType;

pub use builder::{Aggregated, RelationalBuilder, Subquery};
pub use error::RelationalError;
pub use expr::{BinaryOp, Expr, Function, TextMatch, TimeUnit, UnaryOp};
pub use operation::{
    AggregateFunction, JoinKind, Measure, Operation, Order, ReadMode, Selection, Table,
};
pub use relation::{Column, Relation};

use super::{RelationalBackend, RelationalFacts};
use crate::catalog::Catalog;
use crate::plan::tracking::{BodyId, Declared, Language, Plan, Ref};

#[derive(Debug)]
pub struct Relational<B>(PhantomData<B>);

impl<B> Language for Relational<B> {
    type Operation = Operation;
    type Value = Option<DataType>;

    fn references(operation: &mut Operation, visit: &mut dyn FnMut(&mut Ref)) {
        operation.visit(visit);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sharing {
    Inline,
    Shared { materialized: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyUse {
    pub sharing: Sharing,
    pub with: Vec<BodyId>,
}

#[derive(Debug)]
pub struct RelationalPlan<B> {
    pub plan: Plan<Relational<B>>,
    pub with: Vec<BodyId>,
    pub bodies: Vec<BodyUse>,
    pub settings: Vec<(String, String)>,
}

impl<B> RelationalPlan<B> {
    pub fn body(&self, body: BodyId) -> &BodyUse {
        &self.bodies[body.index()]
    }
}

impl<B: RelationalBackend> Catalog<B> {
    pub fn table(&self, name: &str) -> Result<Declared<Table>, RelationalError> {
        self.stores()
            .enumerate()
            .find_map(|(store, (_, facts))| {
                let index = facts
                    .relational()
                    .tables
                    .iter()
                    .position(|table| table.name == name)?;
                Some(Table { store, index })
            })
            .map(|table| Declared::new(self.model().id(), table))
            .ok_or_else(|| RelationalError::UnknownTable(name.to_string()))
    }

    pub fn build<'c>(
        &'c self,
        construct: impl FnOnce(&mut RelationalBuilder<'c, B>) -> Result<Relation, RelationalError>,
    ) -> Result<RelationalPlan<B>, RelationalError> {
        let mut builder = RelationalBuilder::new(self);
        let root = construct(&mut builder);
        builder.finish(root)
    }

    pub fn rewrite<'c>(
        &'c self,
        plan: RelationalPlan<B>,
        visit: impl FnMut(&mut RelationalBuilder<'c, B>, Relation) -> Result<Relation, RelationalError>,
    ) -> Result<RelationalPlan<B>, RelationalError> {
        RelationalBuilder::rewriting(self, plan)?.rewrite(visit)
    }
}
