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
use crate::input::Input;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected, resolve};
use crate::passes::lower::LoweredMetadata;

pub fn apply<M: RelationalModel>(
    plan: PlanOf<M>,
    model: &M,
    input: &Input,
    metadata: &LoweredMetadata,
) -> Result<PlanOf<M>> {
    let root = plan.plan.root();
    let block = plan.block(root);
    let (count, offset) = (
        Some(input.fetch_limit()),
        input.skip.filter(|skip| *skip > 0),
    );
    let keys = match input.skip {
        Some(_) => metadata.stable_order.as_slice(),
        None => &[],
    };
    let aggregate = block.aggregate.map(|node| {
        let Operation::Aggregate { groups, .. } = plan.plan.operation(node) else {
            unreachable!("blocks name aggregates by their operation")
        };
        let outputs = plan.plan.outputs(node).map(Expr::Column);
        (
            plan.plan.inputs(node)[0],
            groups.clone(),
            outputs.collect::<Vec<_>>(),
        )
    });
    let mut ties: Vec<Option<Expr>> = vec![None; keys.len()];
    model
        .catalog()
        .rewrite(plan, |q, rows| {
            let node = q.original();
            if let Some((source, groups, outputs)) = &aggregate
                && node == Some(*source)
            {
                for (key, tie) in keys.iter().zip(&mut ties) {
                    let value = resolve(q, M::DIALECT, &key.expr, &rows)?;
                    *tie = groups
                        .iter()
                        .position(|group| *group == value)
                        .map(|index| outputs[index].clone());
                }
                return Ok(rows);
            }
            let mut rows = rows;
            if node.is_some() && node == block.order && !keys.is_empty() {
                let mut detached = q.detach(rows)?;
                let source = &detached.inputs[0];
                let missing = resolved(q, M::DIALECT, keys, &ties, source)?;
                if let Operation::Order(orders) = &mut detached.operation {
                    extend(orders, missing);
                }
                rows = q.reattach(detached)?;
            }
            if node.is_some() && node == block.limit {
                let mut detached = q.detach(rows)?;
                if block.order.is_none() && !keys.is_empty() {
                    let source = detached.inputs.remove(0);
                    detached
                        .inputs
                        .insert(0, ordered(q, M::DIALECT, keys, &ties, source)?);
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
                    source = ordered(q, M::DIALECT, keys, &ties, source)?;
                }
                detached.inputs.insert(0, q.limit(source, count, offset)?);
                return q.reattach(detached);
            }
            if block.select.is_none() && node == Some(root) {
                if block.order.is_none() && !keys.is_empty() {
                    rows = ordered(q, M::DIALECT, keys, &ties, rows)?;
                }
                return q.limit(rows, count, offset);
            }
            Ok(rows)
        })
        .map_err(rejected)
}

fn resolved<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    dialect: crate::passes::codegen::SqlDialect,
    keys: &[crate::ast::OrderExpr],
    ties: &[Option<Expr>],
    rows: &Relation,
) -> std::result::Result<Vec<Order>, RelationalError> {
    keys.iter()
        .zip(ties)
        .map(|(key, tie)| {
            let value = match tie {
                Some(value) => value.clone(),
                None => resolve(q, dialect, &key.expr, rows)?,
            };
            Ok(Order {
                value,
                descending: key.desc,
            })
        })
        .collect()
}

fn ordered<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    dialect: crate::passes::codegen::SqlDialect,
    keys: &[crate::ast::OrderExpr],
    ties: &[Option<Expr>],
    rows: Relation,
) -> std::result::Result<Relation, RelationalError> {
    let mut orders = Vec::new();
    extend(&mut orders, resolved(q, dialect, keys, ties, &rows)?);
    q.order(rows, orders)
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
