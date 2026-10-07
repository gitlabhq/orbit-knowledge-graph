use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::ast::*;
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{AggFunction, Input, QueryType};
use crate::passes::lower::LoweredMetadata;
use orbit_utils::query_types::SqlType;

mod graph;
pub use graph::apply_graph;

pub fn cursor_column(index: usize) -> String {
    format!("{}cursor_{index}", internal_column_prefix())
}

#[derive(Serialize, Deserialize)]
struct CursorToken {
    h: String,
    k: Vec<Option<String>>,
}

pub fn encode(query_hash: u64, keys: &[Option<String>]) -> String {
    let token = CursorToken {
        h: format!("{query_hash:016x}"),
        k: keys.to_vec(),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).expect("token serializes"))
}

pub fn decode(after: &str, query_hash: u64) -> Result<Vec<Option<String>>> {
    let bytes = URL_SAFE_NO_PAD
        .decode(after)
        .map_err(|_| QueryError::PaginationError("malformed cursor.after token".into()))?;
    let token: CursorToken = serde_json::from_slice(&bytes)
        .map_err(|_| QueryError::PaginationError("malformed cursor.after token".into()))?;
    if token.h != format!("{query_hash:016x}") {
        return Err(QueryError::PaginationError(
            "cursor.after was issued for a different query; restart pagination".into(),
        ));
    }
    Ok(token.k)
}

pub fn canonical_hash(query: &serde_json::Value) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    canonicalize(query, true, &mut |bytes| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    });
    hash
}

fn canonicalize(value: &serde_json::Value, skip_cursor: bool, write: &mut impl FnMut(&[u8])) {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys = map
                .keys()
                .filter(|key| !(skip_cursor && *key == "cursor"))
                .collect::<Vec<_>>();
            keys.sort();
            write(b"{");
            for key in keys {
                write(key.as_bytes());
                write(b":");
                canonicalize(&map[key], false, write);
                write(b",");
            }
            write(b"}");
        }
        serde_json::Value::Array(values) => {
            write(b"[");
            for value in values {
                canonicalize(value, false, write);
                write(b",");
            }
            write(b"]");
        }
        value => write(value.to_string().as_bytes()),
    }
}

pub fn apply(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    query_hash: u64,
) -> Result<usize> {
    let Node::Query(query) = node else {
        return Ok(0);
    };
    let query = query.as_mut();
    query.limit = Some(input.fetch_limit());
    let Some(cursor) = &input.cursor else {
        return Ok(0);
    };
    let additional = metadata
        .stable_order
        .iter()
        .filter(|key| {
            !query
                .order_by
                .iter()
                .any(|current| current.expr == key.expr)
        })
        .cloned()
        .collect::<Vec<_>>();
    query.order_by.extend(additional);
    let order = query.order_by.clone();
    if order.is_empty() {
        return Ok(0);
    }
    for (index, key) in order.iter().enumerate() {
        let readback = SelectExpr::new(
            Expr::func(Function::ToString, vec![key.expr.clone()]),
            cursor_column(index),
        );
        for arm in &mut query.union_all {
            arm.select.push(readback.clone());
        }
        query.select.push(readback);
    }
    let Some(after) = &cursor.after else {
        return Ok(order.len());
    };
    let values = decode(after, query_hash)?;
    if values.len() != order.len() {
        return Err(QueryError::PaginationError(
            "cursor.after was issued for a different query; restart pagination".into(),
        ));
    }
    let nullable = nullable_flags(input, order.len());
    if !query.group_by.is_empty() {
        let seek = seek_predicate(&order, &values, &nullable);
        query.having = Some(match query.having.take() {
            Some(previous) => Expr::and(previous, seek),
            None => seek,
        });
    } else if !query.union_all.is_empty()
        || order
            .iter()
            .any(|key| matches!(key.expr, Expr::Identifier(_)))
    {
        let order = order
            .iter()
            .map(|key| OrderExpr {
                expr: match &key.expr {
                    Expr::Column { column, .. } => Expr::ident(column.clone()),
                    value => value.clone(),
                },
                desc: key.desc,
            })
            .collect::<Vec<_>>();
        let seek = seek_predicate(&order, &values, &nullable);
        let mut inner = std::mem::take(query);
        let limit = inner.limit.take();
        inner.order_by.clear();
        *query = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::subquery(inner, "_page"),
            where_clause: Some(seek),
            order_by: order,
            limit,
            ..Default::default()
        };
    } else {
        let seek = seek_predicate(&order, &values, &nullable);
        query.where_clause = Some(match query.where_clause.take() {
            Some(previous) => Expr::and(previous, seek),
            None => seek,
        });
    }
    Ok(order.len())
}

fn nullable_flags(input: &Input, key_count: usize) -> Vec<bool> {
    let mut flags = vec![false; key_count];
    match input.query_type {
        QueryType::Aggregation => {
            let mut grouped_start = 0;
            if let Some(sort) = &input.aggregation.sort {
                let count = input.aggregation.metrics.iter().any(|metric| {
                    metric.output_name() == sort.column
                        && matches!(metric.expr.function(), AggFunction::Count)
                });
                if let Some(first) = flags.first_mut() {
                    *first = !count;
                }
                grouped_start = 1;
            }
            flags
                .iter_mut()
                .skip(grouped_start)
                .for_each(|flag| *flag = true);
        }
        QueryType::PathFinding => {}
        _ => {
            if input.order_by.is_some()
                && let Some(first) = flags.first_mut()
            {
                *first = true;
            }
        }
    }
    flags
}

fn seek_predicate(order: &[OrderExpr], values: &[Option<String>], nullable: &[bool]) -> Expr {
    let param = |value: &String| Expr::param(SqlType::String, value.clone());
    (0..order.len())
        .filter_map(|index| {
            let value = values[index].as_ref()?;
            let mut prefix = (0..index)
                .map(|index| match &values[index] {
                    Some(value) => Expr::eq(order[index].expr.clone(), param(value)),
                    None => Expr::unary(Op::IsNull, order[index].expr.clone()),
                })
                .collect::<Vec<_>>();
            let mut advance = Expr::binary(
                if order[index].desc { Op::Lt } else { Op::Gt },
                order[index].expr.clone(),
                param(value),
            );
            if nullable[index] {
                advance = Expr::binary(
                    Op::Or,
                    advance,
                    Expr::unary(Op::IsNull, order[index].expr.clone()),
                );
            }
            prefix.push(advance);
            Some(Expr::conjoin(prefix).expect("seek arm is nonempty"))
        })
        .reduce(|left, right| Expr::binary(Op::Or, left, right))
        .unwrap_or_else(|| Expr::eq(Expr::int(0), Expr::int(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|value| Some((*value).into())).collect()
    }

    #[test]
    fn token_roundtrip_and_hash_binding() {
        let keys = some(&["2026-01-16 19:15:23.456", "1234"]);
        let token = encode(42, &keys);
        assert_eq!(decode(&token, 42).unwrap(), keys);
        assert!(matches!(
            decode(&token, 43),
            Err(QueryError::PaginationError(_))
        ));
        assert!(matches!(
            decode("not-a-token", 42),
            Err(QueryError::PaginationError(_))
        ));
    }

    #[test]
    fn token_roundtrips_null_keys() {
        let keys = vec![None, Some("7".into())];
        assert_eq!(decode(&encode(9, &keys), 9).unwrap(), keys);
    }

    #[test]
    fn canonical_hash_ignores_cursor_and_key_order() {
        let first = serde_json::json!({"limit": 5, "query_type": "traversal"});
        let second =
            serde_json::json!({"query_type": "traversal", "cursor": {"page_size": 10}, "limit": 5});
        let different = serde_json::json!({"limit": 6, "query_type": "traversal"});
        assert_eq!(canonical_hash(&first), canonical_hash(&second));
        assert_ne!(canonical_hash(&first), canonical_hash(&different));
    }

    #[test]
    fn seek_predicate_is_lexicographic_dnf() {
        let order = vec![
            OrderExpr::desc(Expr::col("mr", "created_at")),
            OrderExpr::asc(Expr::col("e0", "source_id")),
        ];
        let Expr::BinaryOp {
            op: Op::Or,
            left,
            right,
        } = seek_predicate(&order, &some(&["2026-01-16", "7"]), &[false, false])
        else {
            panic!("expected OR")
        };
        assert!(matches!(*left, Expr::BinaryOp { op: Op::Lt, .. }));
        assert!(matches!(*right, Expr::BinaryOp { op: Op::And, .. }));
    }

    #[test]
    fn nullable_key_advance_arm_admits_null_tail() {
        let order = vec![
            OrderExpr::desc(Expr::col("mr", "merged_at")),
            OrderExpr::asc(Expr::col("mr", "id")),
        ];
        let Expr::BinaryOp {
            op: Op::Or, left, ..
        } = seek_predicate(&order, &some(&["2026-01-16", "7"]), &[true, false])
        else {
            panic!("expected OR")
        };
        let Expr::BinaryOp {
            op: Op::Or, right, ..
        } = *left
        else {
            panic!("expected nullable tail")
        };
        assert!(matches!(*right, Expr::UnaryOp { op: Op::IsNull, .. }));
    }

    #[test]
    fn null_boundary_recurses_on_tie_breaker_under_is_null_prefix() {
        let order = vec![
            OrderExpr::desc(Expr::col("mr", "merged_at")),
            OrderExpr::asc(Expr::col("mr", "id")),
        ];
        let Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } = seek_predicate(&order, &[None, Some("7".into())], &[true, false])
        else {
            panic!("expected AND")
        };
        assert!(matches!(*left, Expr::UnaryOp { op: Op::IsNull, .. }));
        assert!(matches!(*right, Expr::BinaryOp { op: Op::Gt, .. }));
    }

    #[test]
    fn all_null_boundary_yields_false() {
        let order = vec![OrderExpr::asc(Expr::col("g", "name"))];
        assert!(matches!(
            seek_predicate(&order, &[None], &[true]),
            Expr::BinaryOp { op: Op::Eq, .. }
        ));
    }
}
