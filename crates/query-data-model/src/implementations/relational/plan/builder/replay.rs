use super::{RelationalBuilder, Subquery};
use crate::DataType;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{
    Expr, Operation, Relation, Relational, RelationalError, RelationalPlan, Selection, Sharing,
};
use crate::plan::tracking::{
    BodyId, Construction, Declare, Declared, Hook, Outputs, Plan, Rebuild, Rebuilder, Rejected,
    Rows, Step,
};

impl<'c, B: RelationalBackend> Rebuilder for RelationalBuilder<'c, B> {
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

    fn replay(
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
            Operation::Scan { table, read, alias } => {
                let table = Declared::new(self.catalog.model().id(), table);
                self.scan(table, read, &alias)
            }
            Operation::Read { body, alias } => {
                let subquery = self.subquery_of(body)?;
                self.read(&subquery, alias.as_deref())
            }
            Operation::Join(kind) => {
                let left = input()?;
                self.join(left, input()?, kind)
            }
            Operation::Filter(predicate) => self.filter(input()?, predicate),
            Operation::Order(orders) => self.order(input()?, orders),
            Operation::Limit { count, offset } => self.limit(input()?, count, offset),
            Operation::LimitBy { count, by } => self.limit_by(input()?, count, by),
            Operation::Latest { key, version } => {
                let key = key.into_iter().map(|output| self.column(output));
                let key = key.collect::<Result<_, _>>()?;
                let version = self.column(version)?;
                self.latest(input()?, key, version)
            }
            Operation::Aggregate { groups, measures } => {
                Ok(self.aggregate(input()?, groups, measures)?.rows)
            }
            Operation::Compute(values) => {
                let Outputs::Preserve(declared) = outputs else {
                    return Err(RelationalError::Incomplete("computed names"));
                };
                let items = declared
                    .into_iter()
                    .zip(values)
                    .map(|(declared, value)| Declare {
                        name: declared.name,
                        value,
                    });
                Ok(self.compute(input()?, items.collect())?.0)
            }
            Operation::Select(Selection::All) => self.select_all(input()?),
            Operation::Select(Selection::Columns(columns)) => {
                let columns = columns.into_iter().map(|output| self.column(output));
                let columns = columns.collect::<Result<_, _>>()?;
                self.select(input()?, columns)
            }
            Operation::Union => self.union(inputs),
        }
    }

    fn bodies(operation: &mut Operation, visit: &mut dyn FnMut(&mut BodyId)) {
        if let Operation::Read { body, .. } = operation {
            visit(body);
        }
        operation.bodies(visit);
    }

    fn scope<H: Hook<Self>>(
        rebuild: &mut Rebuild<'_, Self>,
        hook: &mut H,
        q: &mut Self,
        body: Option<BodyId>,
    ) -> Result<(), RelationalError> {
        let old = rebuild.old();
        for inner in old.plan.defines(body) {
            let (name, root) = old.plan.body(inner);
            let mut statement = |q: &mut Self| {
                hook.scope(rebuild, q, Some(inner))?;
                rebuild.visit(hook, q, root)
            };
            let built = match old.body(inner).sharing {
                Sharing::Shared { materialized } => q.cte(name, materialized, statement)?,
                Sharing::Inline => q.subquery(name, statement)?,
                Sharing::Pending { lookup: false } => {
                    let rows = statement(q)?;
                    q.share(rows)?
                }
                Sharing::Pending { lookup: true } => {
                    let rows = statement(q)?;
                    q.share_lookup(rows)?
                }
            };
            rebuild.define(inner, built.id());
        }
        Ok(())
    }
}

impl<B: RelationalBackend> Step<'_, '_, RelationalBuilder<'_, B>> {
    /// `expr`, written against the old plan, with its columns pointing into the rebuilt plan.
    pub fn expr(&self, mut expr: Expr) -> Result<Expr, RelationalError> {
        let mut missing = None;
        expr.visit(&mut |output| match self.output(*output) {
            Ok(mapped) => *output = mapped,
            Err(rejected) => missing = Some(rejected),
        });
        match missing {
            Some(rejected) => Err(rejected.into()),
            None => Ok(expr),
        }
    }
}

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub(super) fn subquery_of(&self, body: BodyId) -> Result<Subquery, RelationalError> {
        let name = self.names.get(body.index()).ok_or(Rejected::ForeignPlan)?;
        Ok(Subquery {
            body: self.construction.body(body)?,
            name: name.clone(),
        })
    }
}
