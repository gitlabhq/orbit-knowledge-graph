use super::{cursor_column, decode, nullable_flags};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::{Input, OrderDirection, QueryType};
use crate::query_graph::api::*;
use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;

pub fn apply_graph<'a, M: QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    input: &Input,
    query_hash: u64,
) -> Result<(LoweredGraph<'a, M>, QueryId, usize)> {
    let mut directions = Vec::new();
    let mut names = Vec::new();
    let mut graph = graph.map_result(root, |q, rows| {
        let (mut rows, mut values) = if matches!(rows.kind(), OperationKind::Select { .. }) {
            rows.into_select()?
        } else {
            let values = rows
                .columns()
                .iter()
                .map(|column| column.named(column.name()))
                .collect();
            (rows, values)
        };
        rows = rows.remove_limit()?;
        if input.cursor.is_none() && input.aggregation.sort.is_none() {
            let rows = q.limit(rows, input.fetch_limit())?;
            return Ok::<_, QueryError>(q.select(rows, values)?);
        }
        if matches!(rows.kind(), OperationKind::Sort { .. }) {
            rows = rows.remove_sort()?;
        }
        let keys = cursor_keys(&rows, &values, input)?;
        directions = keys.iter().map(|key| key.descending).collect();
        names = values.iter().map(|value| value.name.clone()).collect();
        values.extend(
            keys.into_iter()
                .enumerate()
                .map(|(index, key)| key.value.named(cursor_column(index))),
        );
        Ok(q.select(rows, values)?)
    })?;
    if input.cursor.is_none() && input.aggregation.sort.is_none() {
        return Ok((graph, root, 0));
    }
    let after = input
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.after.as_ref())
        .map(|after| decode(after, query_hash))
        .transpose()?;
    if after
        .as_ref()
        .is_some_and(|values| values.len() != directions.len())
    {
        return Err(QueryError::PaginationError(
            "cursor key count mismatch".into(),
        ));
    }
    let mut page_error = None;
    let page = graph.query(|q| {
        let mut rows = q.from(root)?;
        let keys = directions
            .iter()
            .enumerate()
            .map(|(index, descending)| Ok((rows.column(&cursor_column(index))?, *descending)))
            .collect::<crate::query_graph::Result<Vec<_>>>()?;
        if let Some(values) = after {
            let predicate = match seek(&keys, values, &nullable_flags(input, keys.len())) {
                Ok(predicate) => predicate,
                Err(error) => {
                    page_error = Some(error);
                    return Err(Error::Type);
                }
            };
            rows = q.filter(rows, predicate)?;
        }
        let mut values = names
            .iter()
            .map(|name| rows.column(name).map(|column| column.named(name)))
            .collect::<crate::query_graph::Result<Vec<_>>>()?;
        if input.cursor.is_some() {
            values.extend(keys.iter().enumerate().map(|(index, (key, _))| {
                Expr::call(Function::ToString, [key.expr()]).named(cursor_column(index))
            }));
        }
        rows = q.sort(
            rows,
            keys.iter().map(|(column, descending)| Order {
                value: column.expr(),
                descending: *descending,
            }),
        )?;
        let rows = q.limit(rows, input.fetch_limit())?;
        q.select(rows, values)
    });
    if let Some(error) = page_error {
        return Err(error);
    }
    let count = if input.cursor.is_some() {
        directions.len()
    } else {
        0
    };
    Ok((graph, page?, count))
}

fn cursor_keys(rows: &Rows<'_>, outputs: &[Named], input: &Input) -> Result<Vec<Order>> {
    let mut keys = Vec::new();
    let named = |name: &str| {
        outputs
            .iter()
            .find(|output| output.name == name)
            .map(|output| output.value.clone())
            .ok_or_else(|| QueryError::PaginationError(format!("missing cursor output {name}")))
    };
    if let Some(order) = &input.order_by {
        keys.push(Order {
            value: rows.column_from(&order.node, &order.property)?.expr(),
            descending: order.direction == OrderDirection::Desc,
        });
    }
    match input.query_type {
        QueryType::Traversal => {
            for node in &input.nodes {
                let value = rows.column_from(&node.id, "id")?.expr();
                if !keys.iter().any(|key| key.value == value) {
                    keys.push(value.asc());
                }
            }
            for output in outputs {
                if output.name.ends_with("_path_nodes") {
                    keys.push(Expr::call(Function::ToString, [output.value.clone()]).asc());
                }
            }
        }
        QueryType::Aggregation => {
            if let Some(sort) = &input.aggregation.sort {
                keys.push(Order {
                    value: named(&sort.column)?,
                    descending: sort.direction == OrderDirection::Desc,
                });
            }
            if input.cursor.is_some()
                && let OperationKind::Aggregate { groups, .. } = rows.kind()
            {
                for column in &rows.columns()[..groups.len()] {
                    let value = column.expr();
                    if !keys.iter().any(|key| key.value == value) {
                        keys.push(value.asc());
                    }
                }
            }
        }
        QueryType::Neighbors => {
            let primary = primary_key_column(&input.nodes[0].id);
            let identity = if outputs.iter().any(|output| output.name == primary) {
                primary
            } else {
                redaction_id_column(&input.nodes[0].id)
            };
            for name in [
                identity.as_str(),
                neighbor_id_column(),
                neighbor_type_column(),
                relationship_type_column(),
                neighbor_is_outgoing_column(),
            ] {
                keys.push(named(name)?.asc());
            }
        }
        QueryType::PathFinding => {
            keys.push(named("depth")?.asc());
            for name in [path_column(), edge_kinds_column()] {
                keys.push(Expr::call(Function::ToString, [named(name)?]).asc());
            }
        }
        QueryType::Hydration => {}
    }
    Ok(keys)
}

fn seek(keys: &[(Column, bool)], values: Vec<Option<String>>, nullable: &[bool]) -> Result<Expr> {
    let mut prefix: Option<Expr> = None;
    let mut predicate: Option<Expr> = None;
    for (index, ((key, descending), value)) in keys.iter().zip(values).enumerate() {
        let is_null = || Expr::call(Function::IsNull, [key.expr()]);
        let equal = if let Some(value) = value {
            let ValueType::Scalar(data_type) = key.data_type() else {
                return Err(QueryError::PaginationError(
                    "cursor requires scalar keys".into(),
                ));
            };
            let value = if matches!(
                data_type,
                SqlType::Int64 | SqlType::UInt32 | SqlType::Float64 | SqlType::Bool
            ) {
                serde_json::from_str(&value)
                    .map_err(|_| QueryError::PaginationError("invalid cursor value".into()))?
            } else {
                serde_json::Value::String(value)
            };
            let value = Expr::literal(*data_type, value);
            let mut advance = if *descending {
                key.lt(value.clone())
            } else {
                key.gt(value.clone())
            };
            if nullable[index] {
                advance = advance.or(is_null());
            }
            if let Some(prefix) = &prefix {
                advance = prefix.clone().and(advance);
            }
            predicate = Some(match predicate {
                Some(previous) => previous.or(advance),
                None => advance,
            });
            key.eq(value)
        } else {
            is_null()
        };
        prefix = Some(match prefix {
            Some(prefix) => prefix.and(equal),
            None => equal,
        });
    }
    Ok(predicate.unwrap_or_else(|| lit(false)))
}
