//! Row window pass. Runs after lowering establishes the final ORDER BY:
//! overfetches one probe row for `has_more` and lowers `SKIP` into an
//! OFFSET over a total order. Compiler tie-breakers extend the sort whenever
//! SKIP is present, so consecutive windows of the same statement neither
//! overlap nor drop rows.

use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Operation, Order, Relation, RelationalBuilder, RelationalError,
};

use crate::error::Result;
use crate::logical::Query;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::passes::plan::build::term;
use crate::passes::plan::{PlanMetadata, Term};

pub fn apply<M: RelationalModel>(
    plan: PlanOf<M>,
    model: &M,
    query: Query<'_>,
    metadata: &PlanMetadata,
) -> Result<PlanOf<M>> {
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
    let ties = Ties {
        keys,
        groups: &groups,
    };
    model
        .catalog()
        .rewrite(plan, |q, rows| {
            let node = q.original();
            let mut rows = rows;
            if node.is_some() && node == block.order && !keys.is_empty() {
                let mut detached = q.detach(rows)?;
                let missing = ties.orders::<M::Backend>(&detached.inputs[0])?;
                if let Operation::Order(orders) = &mut detached.operation {
                    extend(orders, missing);
                }
                rows = q.reattach(detached)?;
            }
            if node.is_some() && node == block.limit {
                let mut detached = q.detach(rows)?;
                if block.order.is_none() && !keys.is_empty() {
                    let source = detached.inputs.remove(0);
                    detached.inputs.insert(0, ties.ordered(q, source)?);
                }
                detached.operation = Operation::Limit { count, offset };
                return q.reattach(detached);
            }
            if block.limit.is_some() {
                return Ok(rows);
            }
            if block.select.is_some() && node == block.select {
                let mut detached = q.detach(rows)?;
                let mut source = detached.inputs.remove(0);
                if block.order.is_none() && !keys.is_empty() {
                    source = ties.ordered(q, source)?;
                }
                detached.inputs.insert(0, q.limit(source, count, offset)?);
                return q.reattach(detached);
            }
            if block.select.is_none() && node == Some(root) {
                if block.order.is_none() && !keys.is_empty() {
                    rows = ties.ordered(q, rows)?;
                }
                return q.limit(rows, count, offset);
            }
            Ok(rows)
        })
        .map_err(rejected)
}

struct Ties<'a> {
    keys: &'a [Term],
    groups: &'a [Expr],
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
                    value: term::<B>(&[rows], key, self.groups)?,
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
