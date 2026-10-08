use std::collections::HashSet;

use crate::input::{ColumnSelection, Input};
use crate::query_graph::api::{Expr, Function, LoweredGraph, QueryId};
use ontology::DataType;
use query_data_model::QueryDataModel;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
pub(crate) const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

fn excerpt_columns(input: &Input, model: &(impl QueryDataModel + ?Sized)) -> HashSet<String> {
    let mut outputs = HashSet::new();
    for node in &input.nodes {
        let Some(entity) = node.entity.as_deref().and_then(|name| model.entity(name)) else {
            continue;
        };
        let Some(ColumnSelection::List(requested)) = &node.columns else {
            continue;
        };
        let dependencies = entity
            .properties
            .iter()
            .filter_map(|property| match model.property_realization(*property) {
                Some(query_data_model::PropertyRealization::Virtual(source)) => {
                    Some(&source.depends_on)
                }
                _ => None,
            })
            .flatten()
            .collect::<HashSet<_>>();
        for property in &entity.properties {
            let property = model.graph().property(*property);
            if property.data_type == DataType::String
                && model.property_is_stored(property.id)
                && requested.contains(&property.name)
                && !dependencies.contains(&property.name)
            {
                outputs.insert(format!("{}_{}", node.id, property.name));
            }
        }
    }
    outputs
}

pub fn apply_graph_excerpts<'a, M: QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    input: &Input,
) -> crate::error::Result<LoweredGraph<'a, M>> {
    let columns = excerpt_columns(input, graph.graph().catalog());
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.fetch_limit().max(1))) as u32;
    graph.map_result(root, |q, rows| {
        if !rows
            .columns()
            .iter()
            .any(|column| columns.contains(column.name()))
        {
            return Ok(rows);
        }
        let values = rows
            .columns()
            .iter()
            .map(|column| {
                let value = if columns.contains(column.name()) {
                    Expr::call(Function::Excerpt(max_chars), [column.expr()])
                } else {
                    column.expr()
                };
                value.named(column.name())
            })
            .collect::<Vec<_>>();
        Ok(q.select(rows, values)?)
    })
}
