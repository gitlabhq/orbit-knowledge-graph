use std::collections::HashSet;

use crate::ast::{Expr, Identifier, Node, Op};
use crate::input::{ColumnSelection, Input};
use ontology::DataType;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

pub fn apply_text_excerpts(
    node: &mut Node,
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) {
    let Node::Query(query) = node else { return };
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.fetch_limit().max(1))) as u32;
    let columns: HashSet<String> = input
        .nodes
        .iter()
        .flat_map(|node| {
            let Some(entity) = node
                .entity
                .as_deref()
                .and_then(|entity| model.entity(entity))
            else {
                return Vec::new();
            };
            let requested = match &node.columns {
                Some(ColumnSelection::List(columns)) => columns,
                _ => return Vec::new(),
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
            excerpted
                .into_iter()
                .map(|column| format!("{}_{}", node.id, column))
                .collect::<Vec<_>>()
        })
        .collect();

    for select in &mut query.select {
        if select
            .alias
            .as_ref()
            .and_then(Identifier::name)
            .is_some_and(|alias| columns.contains(alias))
        {
            select.expr = excerpt(select.expr.clone(), max_chars);
        }
    }
}

pub(crate) fn excerpt(value: Expr, max_chars: u32) -> Expr {
    let excerpt = Expr::func(
        "substringUTF8",
        vec![value.clone(), Expr::lit(1), Expr::lit(max_chars)],
    );
    let shortened = Expr::binary(
        Op::Gt,
        Expr::func("length", vec![value]),
        Expr::func("length", vec![excerpt.clone()]),
    );
    Expr::func(
        "concat",
        vec![
            excerpt,
            Expr::func(
                "if",
                vec![
                    shortened,
                    Expr::string(TEXT_TRUNCATION_SUFFIX),
                    Expr::string(""),
                ],
            ),
        ],
    )
}
