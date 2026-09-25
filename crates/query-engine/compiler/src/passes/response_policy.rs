use std::collections::{HashMap, HashSet};

use ontology::DataType;
use query_data_model::{PropertyRealization, QueryBackendCatalog};

use crate::ast::{Expr, Node, Op, SelectExpr};
use crate::input::{ColumnSelection, Input};

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
                    model.query_backend().property_column(property.id).is_some()
                        && property.data_type == DataType::String
                })
                .map(|property| property.name.clone())
                .collect();
            for property in &entity.properties {
                if let PropertyRealization::Virtual(source) =
                    &model.graph().property(*property).realization
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

    for select in &mut query.select {
        rewrite_select(select, &columns, max_chars);
    }
}

fn rewrite_select(
    select: &mut SelectExpr,
    columns: &HashMap<String, HashSet<String>>,
    max_chars: u32,
) {
    let Expr::Column { table, column } = &select.expr else {
        return;
    };
    if columns
        .get(table)
        .is_some_and(|values| values.contains(column))
    {
        select.expr = excerpt(table, column, max_chars);
    }
}

fn excerpt(alias: &str, column: &str, max_chars: u32) -> Expr {
    let value = Expr::col(alias, column);
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
