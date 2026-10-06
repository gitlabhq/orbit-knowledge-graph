use std::collections::HashSet;

use crate::ast::{Expr, Function, Node, Op};
use crate::input::{ColumnSelection, Input};
use ontology::DataType;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

pub fn apply_text_excerpts(
    node: &mut Node,
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    bindings: &query_data_model::bindings::QueryBindings,
) {
    let Node::Query(query) = node else { return };
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.fetch_limit().max(1))) as u32;
    let columns: HashSet<_> = input
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
            Some(
                excerpted
                    .into_iter()
                    .filter_map(|property| {
                        match model.property_realization(
                            model.graph().property_id(entity.id, &property)?,
                        )? {
                            query_data_model::PropertyRealization::Stored { column } => {
                                Some(*column)
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .flatten()
        .collect();

    for select in &mut query.select {
        let Expr::Column(column) = select.expr else {
            continue;
        };
        if let Ok(query_data_model::bindings::ExportOrigin::Stored(stored)) =
            bindings.origin(column.export())
            && columns.contains(&stored)
        {
            select.expr = excerpt(Expr::Column(column), max_chars);
        }
    }
}

fn excerpt(value: Expr, max_chars: u32) -> Expr {
    let excerpt = Expr::func(
        Function::Substring,
        vec![value.clone(), Expr::lit(1), Expr::lit(max_chars)],
    );
    let shortened = Expr::binary(
        Op::Gt,
        Expr::func(Function::ByteLength, vec![value]),
        Expr::func(Function::ByteLength, vec![excerpt.clone()]),
    );
    Expr::func(
        Function::Concat,
        vec![
            excerpt,
            Expr::func(
                Function::If,
                vec![
                    shortened,
                    Expr::string(TEXT_TRUNCATION_SUFFIX),
                    Expr::string(""),
                ],
            ),
        ],
    )
}
