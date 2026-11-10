use super::{RelationalBuilder, supported};
use crate::DataType;
use crate::implementations::relational::plan::{
    Operation, Relation, Relational, RelationalError, RelationalPlan, Selection,
};
use crate::implementations::relational::{RelationalBackend, RelationalFacts};
use crate::plan::tracking::{BodyId, Construction, Outputs, Plan, Ref, Rows};

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    /// Checks `operation` on `inputs` and appends it; `outputs` names the values a compute declares.
    pub fn apply(
        &mut self,
        operation: Operation,
        mut inputs: Vec<Relation>,
        outputs: Outputs<Option<DataType>>,
    ) -> Result<Relation, RelationalError> {
        let mut input = || match inputs.is_empty() {
            true => Err(RelationalError::Incomplete("an input")),
            false => Ok(inputs.remove(0)),
        };
        match operation {
            Operation::Scan { table, read, alias } => self.scanned(table, read, alias),
            Operation::Read { body, alias } => self.reading(body, alias),
            Operation::Join(kind) => {
                let left = input()?;
                self.joined(left, input()?, kind)
            }
            Operation::Filter(predicate) => {
                let rows = input()?;
                let stage = self.guard(rows.stage.filter())?;
                self.check(&predicate)?;
                self.pass(Operation::Filter(predicate), rows, stage)
            }
            Operation::Order(orders) => {
                let rows = input()?;
                let stage = self.guard(rows.stage.order())?;
                for order in &orders {
                    self.check(&order.value)?;
                }
                self.pass(Operation::Order(orders), rows, stage)
            }
            Operation::Latest { key, version } => self.latest_of(input()?, key, version),
            Operation::LimitBy { count, by } => {
                let rows = input()?;
                self.guard(supported(B::CAPABILITIES.limit_by, "limit by"))?;
                let stage = self.guard(rows.stage.limit_by())?;
                for value in &by {
                    self.check(value)?;
                }
                self.pass(Operation::LimitBy { count, by }, rows, stage)
            }
            Operation::Limit { count, offset } => {
                let rows = input()?;
                let stage = self.guard(rows.stage.limit())?;
                self.pass(Operation::Limit { count, offset }, rows, stage)
            }
            Operation::Aggregate { groups, measures } => {
                self.aggregated(input()?, groups, measures)
            }
            Operation::Compute(values) => self.computed(input()?, values, outputs),
            Operation::Select(Selection::All) => {
                let rows = input()?;
                let stage = self.guard(rows.stage.select())?;
                let operation = Operation::Select(Selection::All);
                let (relation, _) =
                    self.connect(operation, vec![rows], stage, Outputs::preserve())?;
                Ok(relation)
            }
            Operation::Select(Selection::Columns(columns)) => self.selected(input()?, columns),
            Operation::Union => self.united(inputs),
        }
    }

    fn latest_of(
        &mut self,
        rows: Relation,
        key: Vec<Ref>,
        version: Ref,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.latest())?;
        let (name, _) = self.construction.value(version)?;
        let name = name.unwrap_or_default().to_string();
        let versioned = self.catalog.stores().any(|(_, facts)| {
            facts
                .relational()
                .versioning
                .as_ref()
                .is_some_and(|versioning| versioning.version == name)
        });
        self.guard(match versioned {
            true => Ok(()),
            false => Err(RelationalError::NotVersion(name)),
        })?;
        self.pass(Operation::Latest { key, version }, rows, stage)
    }
}

impl<B: RelationalBackend> crate::plan::tracking::Rebuilder for RelationalBuilder<'_, B> {
    type Language = Relational<B>;
    type Old = RelationalPlan<B>;
    type Rows = Relation;
    type Error = RelationalError;

    fn plan(old: &RelationalPlan<B>) -> &Plan<Relational<B>> {
        &old.plan
    }

    fn construction(&mut self) -> &mut Construction<Relational<B>> {
        &mut self.construction
    }

    fn rows(rows: &Relation) -> &Rows {
        &rows.rows
    }

    fn apply(
        &mut self,
        operation: Operation,
        inputs: Vec<Relation>,
        outputs: Outputs<Option<DataType>>,
    ) -> Result<Relation, RelationalError> {
        RelationalBuilder::apply(self, operation, inputs, outputs)
    }

    fn bodies(operation: &mut Operation, visit: &mut dyn FnMut(&mut BodyId)) {
        if let Operation::Read { body, .. } = operation {
            visit(body);
        }
        operation.bodies(visit);
    }

    fn scope<H: crate::plan::tracking::Hook<Self>>(
        rebuild: &mut crate::plan::tracking::Rebuild<'_, Self>,
        hook: &mut H,
        q: &mut Self,
        body: Option<BodyId>,
    ) -> Result<(), RelationalError> {
        q.define_scope(rebuild, hook, body)
    }
}
