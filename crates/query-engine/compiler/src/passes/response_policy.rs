use std::collections::{HashMap, HashSet};
use std::convert::Infallible;

use crate::input::{ColumnSelection, Input};
use crate::query_graph::{BlockId, Expression, Port, QueryGraph, ScanInput};
use ontology::DataType;
use query_data_model::QueryDataModel;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
pub(crate) const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

fn excerpt_columns(
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
) -> HashMap<String, HashSet<String>> {
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = model.entity(node.entity.as_deref()?)?;
            let Some(ColumnSelection::List(requested)) = &node.columns else {
                return None;
            };
            let mut excerpted = entity
                .properties
                .iter()
                .map(|property| model.graph().property(*property))
                .filter(|property| {
                    model.property_is_stored(property.id) && property.data_type == DataType::String
                })
                .map(|property| property.name.clone())
                .collect::<HashSet<_>>();
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
        .collect()
}

pub fn apply_graph_excerpts<'a, M: QueryDataModel + ?Sized>(
    graph: &mut QueryGraph<'a, M, Infallible>,
    root: BlockId,
    input: &Input,
) -> crate::error::Result<()> {
    let columns = excerpt_columns(input, graph.catalog());
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.fetch_limit().max(1))) as u32;
    for output in graph.outputs(root)? {
        let Expression::Column(column) = graph.projection(output)? else {
            continue;
        };
        let Port::Stored(property) = column.port() else {
            continue;
        };
        let Some(ScanInput::Node(index)) = graph.relation(column.relation())?.input else {
            continue;
        };
        if columns
            .get(&input.nodes[index].id)
            .is_some_and(|columns| columns.contains(property.name()))
        {
            graph.rewrite_output(
                output,
                Expression::Excerpt {
                    value: Box::new(Expression::Column(*column)),
                    max_chars,
                },
            )?;
        }
    }
    Ok(())
}
