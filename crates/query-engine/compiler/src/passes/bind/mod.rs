mod aggregation;
mod properties;
mod relationships;
mod validation;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::query::{Cursor, NodeId, Query, QueryType};

use aggregation::{bind_aggregation, bind_comparisons, bind_neighbors, bind_order, bind_path};
use properties::{bind_filters, bind_hydration_node, bind_node};
use relationships::bind_relationships;
use validation::{check_query, check_shape};

pub(crate) use validation::validate_identifier;

#[derive(Clone, Copy)]
pub enum Mode {
    Public,
    Local,
}

pub fn bind<M: OrbitQueryModel>(mut input: Input, model: &M, mode: Mode) -> Result<Query> {
    check_shape(&input)?;
    let mut query = Query::new(input.query_type);
    query.limit = input.limit;
    query.cursor = input.cursor.take().map(|cursor| Cursor {
        page_size: cursor.page_size,
        after: cursor.after,
    });
    query.dynamic_columns = input.options.dynamic_columns;
    query.include_debug_sql = input.options.include_debug_sql;

    let mut node_filters = Vec::new();
    for mut input_node in input.nodes {
        node_filters.push(std::mem::take(&mut input_node.filters));
        bind_node(&mut query, input_node, model, mode)?;
    }
    for (index, filters) in node_filters.into_iter().enumerate() {
        bind_filters(&mut query, NodeId(index), filters, model, mode)?;
    }
    bind_relationships(&mut query, input.relationships, model)?;
    bind_aggregation(&mut query, input.aggregation, model)?;
    bind_order(&mut query, input.order_by, model)?;
    bind_path(&mut query, input.path, model)?;
    bind_neighbors(&mut query, input.neighbors, model)?;
    bind_comparisons(&mut query, input.join_predicates, model)?;
    check_query(&query, mode)?;
    Ok(query)
}

pub fn bind_hydration<M, I>(nodes: I, limit: u32, model: &M) -> Result<Query>
where
    M: OrbitQueryModel,
    I: IntoIterator<Item = (String, Vec<String>, Vec<i64>, Vec<TraversalPath>)>,
{
    let mut query = Query::new(QueryType::Hydration);
    query.limit = limit;
    for (entity_name, columns, node_ids, traversal_paths) in nodes {
        bind_hydration_node(
            &mut query,
            entity_name,
            columns,
            node_ids,
            traversal_paths,
            model,
        )?;
    }
    if query.nodes.is_empty() || query.limit == 0 {
        return Err(QueryError::Validation(
            "hydration requires at least one node and a positive limit".into(),
        ));
    }
    Ok(query)
}
