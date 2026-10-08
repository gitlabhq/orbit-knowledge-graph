use super::super::api::*;
use crate::input::{ColumnSelection, Input};
use crate::passes::plan::{
    HydrationCompileOptions,
    hydration::{HydrationPathFilter, path_filter},
};
use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;

pub(super) fn build<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
    options: HydrationCompileOptions,
) -> Result<Rows<'a>> {
    let mut arms = Vec::with_capacity(input.nodes.len());
    for node in &input.nodes {
        arms.push(q.subquery(|q| {
            let entity = node.entity.as_deref().ok_or(Error::Outputs)?;
            let table = q
                .catalog()
                .entity_table(entity)
                .ok_or_else(|| Error::Unknown(entity.into()))?;
            let Some(ColumnSelection::List(properties)) = &node.columns else {
                return Err(Error::Outputs);
            };
            let mut rows = q.scan(table, Read::Raw)?.labeled(&node.id)?;
            let identity = rows.column(&node.id_property)?;
            let version = rows.column(ontology::VERSION_COLUMN)?;
            let deleted = rows.column(ontology::DELETED_COLUMN)?;
            let keys = q
                .catalog()
                .table_sort_key(table)
                .ok_or(Error::Latest)?
                .iter()
                .map(|name| rows.column(name))
                .collect::<Result<Vec<_>>>()?;
            let mut fields = Vec::with_capacity(properties.len());
            for property in properties {
                let column = q
                    .catalog()
                    .property_column_named(entity, property)
                    .unwrap_or(property);
                fields.push(Expr::call(
                    Function::ToString,
                    [rows.column(column)?.expr()],
                ));
            }
            if let Some(paths) = path_filter(&node.traversal_paths, options) {
                let path = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
                let predicate = match paths {
                    HydrationPathFilter::PrefixUnion(paths) => paths
                        .iter()
                        .map(|prefix| path.starts_with(prefix.as_str()))
                        .reduce(Expr::or)
                        .unwrap_or_else(|| lit(false)),
                    HydrationPathFilter::PrefixSet(paths) => Expr::call(
                        Function::StartsWithAny,
                        [
                            path.expr(),
                            Expr::literal(
                                SqlType::String.to_array(),
                                paths
                                    .iter()
                                    .map(|path| path.as_str())
                                    .collect::<Vec<_>>()
                                    .into(),
                            ),
                        ],
                    ),
                };
                rows = q.filter(rows, predicate)?;
            }
            if !node.node_ids.is_empty() {
                rows = q.filter(
                    rows,
                    identity.expr().binary(
                        Operator::In,
                        Expr::literal(SqlType::Int64.to_array(), node.node_ids.clone().into()),
                    ),
                )?;
            }
            rows = q.latest(rows, keys, version)?;
            rows = q.filter(rows, deleted.eq(false))?;
            q.select(
                rows,
                [
                    identity.named(format!("{}_{}", node.id, node.id_property)),
                    lit(entity).named(format!("{}_entity_type", node.id)),
                    Expr::call(Function::JsonObject(properties.clone()), fields)
                        .named(format!("{}_props", node.id)),
                ],
            )
        })?);
    }
    let rows = if arms.len() == 1 {
        q.from(arms[0])?
    } else {
        q.union_all(arms)?
    };
    q.limit(rows, input.limit)
}
