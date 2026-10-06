use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, Function, Node, Op, SelectExpr};
use crate::input::{ColumnSelection, Input};
use ontology::DataType;

const WORKHORSE_GRPC_MESSAGE_CAP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_UTF8_BYTES_PER_CHAR: u64 = 4;
pub(crate) const TEXT_TRUNCATION_SUFFIX: &str = " [truncated]";

fn excerpt_limit(input: &Input) -> u32 {
    (WORKHORSE_GRPC_MESSAGE_CAP_BYTES
        / MAX_UTF8_BYTES_PER_CHAR
        / u64::from(input.fetch_limit().max(1))) as u32
}

pub fn apply_text_excerpts(
    node: &mut Node,
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) {
    let Node::Query(query) = node else { return };
    let max_chars = excerpt_limit(input);
    let columns = excerpt_columns(input, model);
    for select in &mut query.select {
        rewrite_select(select, &columns, max_chars);
    }
}

fn excerpt_columns(
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> HashMap<String, HashSet<String>> {
    input
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
        .collect()
}

pub fn apply_graph_excerpts<'a, M: query_data_model::QueryDataModel + ?Sized>(
    graph: &mut crate::query_graph::QueryGraph<
        'a,
        M,
        crate::query_graph::Expression<'a>,
        crate::query_graph::LoweredOperation<'a>,
    >,
    root: crate::query_graph::BlockId,
    input: &Input,
) -> crate::error::Result<()> {
    use crate::query_graph::{Expression as E, Port, ScanInput};
    let columns = excerpt_columns(input, graph.catalog());
    let max_chars = excerpt_limit(input);
    let outputs = graph.outputs(root)?.collect::<Vec<_>>();
    for output in outputs {
        let E::Column(column) = graph.projection(output)?.value else {
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
            graph.replace_output(
                output,
                E::Excerpt {
                    value: Box::new(E::Column(column)),
                    max_chars,
                },
            )?;
        }
    }
    Ok(())
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
