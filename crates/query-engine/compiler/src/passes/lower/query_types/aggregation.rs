use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Measure, Order, Relation, RelationalBuilder, TimeUnit,
};

use crate::passes::lower::build::predicates::Lowering;
use crate::passes::lower::build::{Built, computed, conjoin, named};
use crate::passes::lower::{Term, columns, graph};
use crate::passes::plan::predicates::OutputValue;
use crate::passes::plan::query_types::aggregation::Group;
use crate::passes::plan::{Aggregation, Plan};
use crate::tp_bounds::TpBounds;
use query_data_model::plan::logical::TimeUnit as Unit;

pub(crate) fn emit<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    plan: &Plan<Aggregation>,
    limit: u32,
    tp_bounds: Option<&TpBounds>,
) -> Built<Relation> {
    let result = &plan.operation.result;
    let execution = &plan.operation.execution;
    let mut read = super::super::redaction_columns(plan, execution);
    for condition in &result.condition {
        columns::referenced(condition, &mut read);
    }
    read.extend(
        result
            .groups
            .iter()
            .filter_map(|group| group.value.column().cloned()),
    );
    read.extend(result.measures.iter().filter_map(|measure| {
        measure
            .argument
            .as_ref()
            .and_then(OutputValue::column)
            .cloned()
    }));
    let executed = graph::execute(q, execution, &read, tp_bounds)?;
    let rows = executed.reads.filter(q, executed.source)?;
    let lowering = Lowering {
        scope: &[&rows],
        shared: &executed.reads.shared,
    };
    let mut groups = Vec::new();
    for value in &result.groups {
        groups.push(group::<B>(&lowering, value)?);
    }
    let mut outputs = Vec::new();
    for (value, name) in &result.group_outputs {
        outputs.push((name.clone(), group::<B>(&lowering, value)?));
    }
    let mut measures: Vec<Measure> = Vec::new();
    let mut keys = Vec::new();
    let mut picks = Vec::new();
    for measure in &result.measures {
        let key = (measure.function, &measure.argument, measure.distinct);
        if let Some(position) = keys.iter().position(|existing| *existing == key) {
            picks.push((measure.name.clone(), position));
            continue;
        }
        keys.push(key);
        let mut condition = Vec::new();
        for predicate in &result.condition {
            condition.push(lowering.predicate(q, predicate)?);
        }
        let args = match &measure.argument {
            Some(value) => vec![lowering.output::<B>(value)?],
            None => Vec::new(),
        };
        let lowered = Measure {
            function: measure.function,
            args,
            distinct: measure.distinct,
            condition: conjoin(condition),
        };
        measures.push(lowered);
        picks.push((measure.name.clone(), measures.len() - 1));
    }
    let projections = lowering.projections::<B>(&execution.outputs)?;
    let aggregated = q.aggregate(rows, groups.clone(), measures)?;
    let bound = |value: Expr| match groups.iter().position(|group| *group == value) {
        Some(index) => aggregated.groups[index].expr(),
        None => value,
    };
    let mut items: Vec<(String, Expr)> = outputs
        .into_iter()
        .map(|(name, value)| (name, bound(value)))
        .collect();
    items.extend(
        picks
            .into_iter()
            .map(|(name, position)| (name, aggregated.measures[position].expr())),
    );
    items.extend(
        projections
            .into_iter()
            .map(|(name, value)| (name, bound(value))),
    );
    let (rows, columns) = computed(q, aggregated.rows, items)?;
    let rows = match &result.order {
        Some((column, descending)) => {
            let value = named(&[&rows], column)?;
            let order = Order {
                value,
                descending: *descending,
            };
            q.order(rows, vec![order])?
        }
        None => rows,
    };
    let rows = q.limit(rows, Some(limit), None)?;
    q.select(rows, columns)
}

fn group<B: RelationalBackend>(lowering: &Lowering<'_>, value: &Group) -> Built<Expr> {
    let column = lowering.output::<B>(&value.value)?;
    Ok(match value.truncate {
        Some(unit) => Expr::TimeBucket {
            unit: time_unit(unit),
            value: Box::new(column),
        },
        None => column,
    })
}

pub(crate) fn stable_order(plan: &Plan<Aggregation>) -> Vec<Term> {
    (0..plan.operation.result.groups.len())
        .map(Term::Group)
        .collect()
}

fn time_unit(unit: Unit) -> TimeUnit {
    match unit {
        Unit::Minute => TimeUnit::Minute,
        Unit::Hour => TimeUnit::Hour,
        Unit::Day => TimeUnit::Day,
        Unit::Week => TimeUnit::Week,
        Unit::Month => TimeUnit::Month,
        Unit::Quarter => TimeUnit::Quarter,
        Unit::Year => TimeUnit::Year,
    }
}
