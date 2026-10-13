use std::collections::{HashMap, HashSet};

use crate::ast::{Ast, Expr, Function, Op, SelectExpr};
use crate::query::Query;
use ontology::DataType;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

pub fn apply_text_excerpts<S>(
    ast: &mut Ast<S>,
    query: &Query,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) {
    let Some(select) = ast.select_mut() else {
        return;
    };
    let max_chars = (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(query.fetch_limit().max(1))) as u32;
    let columns: HashMap<String, HashSet<String>> = query
        .nodes
        .iter()
        .map(|node| {
            let entity = model.graph().entity(node.entity);
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
            excerpted.retain(|column| node.columns.iter().any(|property| &property.name == column));
            (node.alias.clone(), excerpted)
        })
        .collect();

    for expression in &mut select.select {
        rewrite_select(expression, &columns, max_chars);
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
