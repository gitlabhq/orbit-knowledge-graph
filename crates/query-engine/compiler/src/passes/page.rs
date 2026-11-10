//! Row window pass. Runs after lowering establishes the final ORDER BY:
//! overfetches one probe row for `has_more` and lowers `SKIP` into an
//! OFFSET over a total order. Compiler tie-breakers extend the sort whenever
//! SKIP is present, so consecutive windows of the same statement neither
//! overlap nor drop rows.

use query_data_model::Catalog;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::RelationalPlan;
use query_data_model::implementations::relational::plan::{
    Expr, Operation, Order, Relation, RelationalBuilder, RelationalError,
};
use query_data_model::plan::tracking::Step;

use crate::error::Result;
use crate::logical::Query;
use crate::passes::codegen::{SqlBackend, rejected};
use crate::passes::plan::build::term;
use crate::passes::plan::{PlanMetadata, Term};

pub fn apply<B: SqlBackend>(
    plan: RelationalPlan<B>,
    model: &Catalog<B>,
    query: Query<'_>,
    metadata: &PlanMetadata,
) -> Result<RelationalPlan<B>> {
    let root = plan.plan.root();
    let block = plan.block(root);
    let skip = query.skip();
    let (count, offset) = (Some(query.fetch_limit()?), skip.filter(|skip| *skip > 0));
    let keys = match skip {
        Some(_) => metadata.stable_order.as_slice(),
        None => &[],
    };
    let groups: Vec<Expr> = match block
        .aggregate
        .map(|node| (node, plan.plan.operation(node)))
    {
        Some((node, Operation::Aggregate { groups, .. })) => plan
            .plan
            .outputs(node)
            .take(groups.len())
            .map(Expr::Column)
            .collect(),
        _ => Vec::new(),
    };
    let unordered = block.order.is_none() && !keys.is_empty();
    model
        .rebuild(plan, |q, step| {
            let node = Some(step.node());
            let ties = |step: &Step<'_, '_, RelationalBuilder<'_, B>>| {
                let groups = groups.iter().map(|group| step.expr(group.clone()));
                Ok::<_, RelationalError>(Ties {
                    keys,
                    groups: groups.collect::<std::result::Result<_, _>>()?,
                })
            };
            if node == block.order && !keys.is_empty() {
                let missing = ties(step)?.orders::<B>(&step.inputs[0])?;
                if let Operation::Order(orders) = &mut step.operation {
                    extend(orders, missing);
                }
            }
            if node == block.limit {
                if unordered {
                    let source = step.inputs.remove(0);
                    step.inputs.insert(0, ties(step)?.ordered(q, source)?);
                }
                step.operation = Operation::Limit { count, offset };
                return step.keep(q);
            }
            if block.limit.is_none() && block.select.is_some() && node == block.select {
                let mut source = step.inputs.remove(0);
                if unordered {
                    source = ties(step)?.ordered(q, source)?;
                }
                step.inputs.insert(0, q.limit(source, count, offset)?);
                return step.keep(q);
            }
            let mut rows = step.keep(q)?;
            if block.limit.is_none() && block.select.is_none() && node == Some(root) {
                if unordered {
                    rows = ties(step)?.ordered(q, rows)?;
                }
                rows = q.limit(rows, count, offset)?;
            }
            Ok(rows)
        })
        .map_err(rejected)
}

struct Ties<'a> {
    keys: &'a [Term],
    groups: Vec<Expr>,
}

impl Ties<'_> {
    fn orders<B: RelationalBackend>(
        &self,
        rows: &Relation,
    ) -> std::result::Result<Vec<Order>, RelationalError> {
        self.keys
            .iter()
            .map(|key| {
                Ok(Order {
                    value: term::<B>(&[rows], key, &self.groups)?,
                    descending: false,
                })
            })
            .collect()
    }

    fn ordered<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        rows: Relation,
    ) -> std::result::Result<Relation, RelationalError> {
        let mut orders = Vec::new();
        extend(&mut orders, self.orders::<B>(&rows)?);
        q.order(rows, orders)
    }
}

fn extend(orders: &mut Vec<Order>, ties: Vec<Order>) {
    let current = orders.len();
    for tie in ties {
        if !orders[..current]
            .iter()
            .any(|order| order.value == tie.value)
        {
            orders.push(tie);
        }
    }
}
