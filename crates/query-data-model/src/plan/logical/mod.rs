mod builder;
mod check;
mod error;
mod expr;
mod handle;

use ontology::DataType;

pub use builder::LogicalBuilder;
pub use error::PlanError;
pub use expr::{Comparison, Expr, Literal, Measure, Named, Order, Predicate, count};
pub use handle::{Binding, Rows};

use crate::plan::tracking::{self, Body, Declared, Language, Plan, Ref};
use crate::{EntityId, Hop, Model, RelationshipId};

#[derive(Debug)]
pub struct Logical;

pub type LogicalPlan = Plan<Logical>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    Entity(EntityId),
    Scalar(DataType),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperation {
    Entities(EntityId),
    Filter(Predicate),
    Follow {
        from: Ref,
        hops: Vec<Hop>,
    },
    Aggregate {
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    },
    Select(Vec<Expr>),
    Order(Vec<(Expr, Order)>),
    Limit(u64),
    Read(Body),
}

impl Language for Logical {
    type Operation = LogicalOperation;
    type Value = LogicalType;

    fn read(body: Body) -> LogicalOperation {
        LogicalOperation::Read(body)
    }

    fn references(operation: &mut LogicalOperation, visit: &mut dyn FnMut(&mut Ref)) {
        match operation {
            LogicalOperation::Filter(predicate) => predicate.references(visit),
            LogicalOperation::Follow { from, .. } => visit(from),
            LogicalOperation::Aggregate { groups, measures } => {
                let inputs = measures.iter_mut().filter_map(Measure::input);
                groups
                    .iter_mut()
                    .chain(inputs)
                    .for_each(|expr| expr.references(visit));
            }
            LogicalOperation::Select(exprs) => {
                exprs.iter_mut().for_each(|expr| expr.references(visit))
            }
            LogicalOperation::Order(keys) => {
                keys.iter_mut().for_each(|(key, _)| key.references(visit))
            }
            LogicalOperation::Entities(_)
            | LogicalOperation::Limit(_)
            | LogicalOperation::Read(_) => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalCatalog<'m> {
    model: &'m Model,
}

impl Model {
    pub fn logical(&self) -> LogicalCatalog<'_> {
        LogicalCatalog { model: self }
    }
}

impl<'m> LogicalCatalog<'m> {
    pub fn model(self) -> &'m Model {
        self.model
    }

    pub fn entity(self, name: &str) -> Result<Declared<EntityId>, PlanError> {
        Ok(Declared::new(self.model.id(), self.model.entity(name)?))
    }

    pub fn relationship(self, name: &str) -> Result<Declared<RelationshipId>, PlanError> {
        Ok(Declared::new(
            self.model.id(),
            self.model.relationship(name)?,
        ))
    }

    pub fn build(
        self,
        construct: impl FnOnce(&mut LogicalBuilder<'m>) -> Result<Rows<'m>, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let mut builder = LogicalBuilder::new(self, tracking::Construction::new(self.model.id()));
        let root = construct(&mut builder).map(Rows::into_inner);
        builder.finish(root)
    }

    pub fn rewrite(
        self,
        plan: LogicalPlan,
        mut visit: impl FnMut(&mut LogicalBuilder<'m>, Rows<'m>) -> Result<Rows<'m>, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let construction = tracking::Construction::rewrite(plan, self.model.id())?;
        let mut builder = LogicalBuilder::new(self, construction);
        let root = tracking::rewrite(&mut builder, LogicalBuilder::construction, |q, rows| {
            let rows = q.view(rows)?;
            visit(q, rows).map(Rows::into_inner)
        });
        builder.finish(root)
    }
}
