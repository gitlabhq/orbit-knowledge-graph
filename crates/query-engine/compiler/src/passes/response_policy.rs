use std::collections::{HashMap, HashSet};

use ontology::DataType;
use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::plan::{BinaryOp, Expr, Function, Operation};

use crate::error::Result;
use crate::input::{ColumnSelection, Input};
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

pub fn apply_text_excerpts<M: RelationalModel>(
    plan: PlanOf<M>,
    model: &M,
    input: &Input,
) -> Result<PlanOf<M>> {
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.sorted_rows().max(1))) as u32;
    let columns: HashMap<String, HashSet<String>> = input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = model.entity(node.entity.as_deref()?)?;
            let requested = match &node.columns {
                Some(ColumnSelection::List(columns)) => columns,
                _ => return None,
            };
            let mut excerpted: HashSet<String> = model
                .graph()
                .entity(entity.id)
                .properties
                .iter()
                .map(|property| model.graph().property(*property))
                .filter(|property| {
                    model.property_is_stored(property.id) && property.data_type == DataType::String
                })
                .map(|property| property.name.clone())
                .collect();
            for property in &entity.properties {
                if let Some(query_data_model::PropertyRealization::Virtual(source)) =
                    model.property_realization(*property)
                {
                    for dependency in &source.depends_on {
                        excerpted.remove(dependency);
                    }
                }
            }
            excerpted.retain(|column| requested.contains(column));
            Some((node.id.clone(), excerpted))
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
        .catalog()
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
