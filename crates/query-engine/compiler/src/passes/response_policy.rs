use query_data_model::Catalog;
use query_data_model::implementations::relational::plan::RelationalPlan;
use std::collections::{HashMap, HashSet};

use orbit_utils::query_types::SqlType;
use query_data_model::DataType;
use query_data_model::implementations::relational::plan::{BinaryOp, Expr, Function, Operation};

use crate::error::Result;
use crate::logical::Query;
use crate::logical::columns::{returned, selections};
use crate::passes::codegen::{SqlBackend, rejected};

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

pub fn apply_text_excerpts<B: SqlBackend>(
    plan: RelationalPlan<B>,
    model: &Catalog<B>,
    query: Query<'_>,
) -> Result<RelationalPlan<B>> {
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(query.sorted_rows()?.max(1))) as u32;
    let selected = selections(query);
    let columns: HashMap<String, HashSet<String>> = query
        .bindings()
        .into_iter()
        .map(|binding| {
            let graph = model.graph();
            let entity = graph.entity(binding.entity);
            let selection = selected.get(&binding.output).copied();
            let requested = returned(model, binding.entity, selection);
            let mut excerpted: HashSet<String> = entity
                .properties
                .iter()
                .map(|property| graph.property(*property))
                .filter(|property| {
                    model.column(property.id).is_some() && property.data_type == DataType::String
                })
                .map(|property| property.name.clone())
                .collect();
            for property in &entity.properties {
                if let Some(lookup) = model.lookup(*property) {
                    for key in &lookup.keys {
                        excerpted.remove(&graph.property(*key).name);
                    }
                }
            }
            excerpted.retain(|column| requested.contains(column));
            (binding.name.to_string(), excerpted)
        })
        .collect();

    let root = plan.plan.root();
    let top = match plan.block(root).select {
        Some(_) => root,
        None => plan.plan.inputs(plan.block(root).base)[0],
    };
    let Some(compute) = plan.block(top).compute else {
        return Ok(plan);
    };
    model
        .rewrite(plan, |q, rows| {
            if q.original() != Some(compute) {
                return Ok(rows);
            }
            let mut detached = q.detach(rows)?;
            let source = &detached.inputs[0];
            let excerpted: Vec<_> = columns
                .iter()
                .flat_map(|(alias, names)| names.iter().map(move |name| (alias, name)))
                .filter_map(|(alias, name)| source.qualified(alias, name).ok())
                .map(|column| column.output())
                .collect();
            if let Operation::Compute(values) = &mut detached.operation {
                for value in values {
                    if matches!(value, Expr::Column(output) if excerpted.contains(output)) {
                        *value = excerpt(value.clone(), max_chars);
                    }
                }
            }
            q.reattach(detached)
        })
        .map_err(rejected)
}

fn excerpt(value: Expr, max_chars: u32) -> Expr {
    let call = |function, args| Expr::Call { function, args };
    let literal = |value: serde_json::Value| Expr::Parameter {
        data_type: SqlType::from_value(&value),
        value,
    };
    let excerpt = call(
        Function::Substring,
        vec![value.clone(), literal(1.into()), literal(max_chars.into())],
    );
    let shortened = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(call(Function::ByteLength, vec![value])),
        right: Box::new(call(Function::ByteLength, vec![excerpt.clone()])),
    };
    let suffix = call(
        Function::If,
        vec![
            shortened,
            literal(TEXT_TRUNCATION_SUFFIX.into()),
            literal("".into()),
        ],
    );
    call(Function::Concat, vec![excerpt, suffix])
}
