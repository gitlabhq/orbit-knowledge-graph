//! Keyset pagination pass. Runs after enforce/security so the final
//! ORDER BY is known: appends hidden `_gkg_cursor_N` readback columns for each
//! sort key, lowers the decoded `after` token into a lexicographic seek
//! predicate, and records the key count for the output stage.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::ast::*;
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{AggFunction, Input, QueryType};
use crate::passes::lower::LoweredMetadata;
use orbit_utils::query_types::SqlType;

pub fn cursor_column(i: usize) -> String {
    format!("{}cursor_{i}", internal_column_prefix())
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

/// FNV-1a over the canonicalized (key-sorted) query JSON minus `cursor`, so a
/// token binds to the exact query it was issued for.
pub fn canonical_hash(query: &serde_json::Value) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut write = |bytes: &[u8]| {
        for b in bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    };
    fn canonicalize(v: &serde_json::Value, skip_cursor: bool, out: &mut dyn FnMut(&[u8])) {
        match v {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<&String> = map
                    .keys()
                    .filter(|k| !(skip_cursor && *k == "cursor"))
                    .collect();
                keys.sort();
                out(b"{");
                for k in keys {
                    out(k.as_bytes());
                    out(b":");
                    canonicalize(&map[k], false, out);
                    out(b",");
                }
                out(b"}");
            }
            serde_json::Value::Array(items) => {
                out(b"[");
                for item in items {
                    canonicalize(item, false, out);
                    out(b",");
                }
                out(b"]");
            }
            other => out(other.to_string().as_bytes()),
        }
    }
    canonicalize(query, true, &mut write);
    hash
}

pub fn apply(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    query_hash: u64,
) -> Result<usize> {
    let Node::Query(q) = node else {
        return Ok(0);
    };
    q.limit = Some(input.fetch_limit());
    let Some(cursor) = &input.cursor else {
        return Ok(0);
    };
    let additional: Vec<_> = metadata
        .stable_order
        .iter()
        .filter(|key| !q.order_by.iter().any(|current| current.expr == key.expr))
        .cloned()
        .collect();
    q.order_by.extend(additional);
    let order_by = q.order_by.clone();
    if order_by.is_empty() {
        return Ok(0);
    }

    append_readback_columns(q, &order_by);

    let Some(after) = &cursor.after else {
        return Ok(order_by.len());
    };
    let values = decode(after, query_hash)?;
    if values.len() != order_by.len() {
        return Err(QueryError::PaginationError(
            "cursor.after was issued for a different query; restart pagination".into(),
        ));
    }
    let nullable = nullable_flags(input, order_by.len());
    let alias_scoped = order_by
        .iter()
        .any(|o| matches!(o.expr, Expr::Identifier(_)));
    if !q.group_by.is_empty() {
        place_seek_in_having(q, &order_by, &values, &nullable);
    } else if !q.union_all.is_empty() || alias_scoped {
        hoist_page_subquery(q, &order_by, &values, &nullable);
    } else {
        merge_seek_into_where(q, &order_by, &values, &nullable);
    }
    Ok(order_by.len())
}

fn append_readback_columns(q: &mut Query, order_by: &[OrderExpr]) {
    for (i, o) in order_by.iter().enumerate() {
        let hidden = SelectExpr::new(
            Expr::func(crate::ast::Function::ToString, vec![o.expr.clone()]),
            cursor_column(i),
        );
        for arm in &mut q.union_all {
            arm.select.push(hidden.clone());
        }
        q.select.push(hidden);
    }
}

pub fn apply_graph<'a, M: query_data_model::QueryDataModel + ?Sized>(
    graph: &mut crate::query_graph::QueryGraph<
        'a,
        M,
        crate::query_graph::Expression<'a>,
        crate::query_graph::LoweredOperation<'a>,
    >,
    root: crate::query_graph::BlockId,
    input: &Input,
    query_hash: u64,
) -> Result<(crate::query_graph::BlockId, usize)> {
    use crate::query_graph::{Expression as E, Relational};
    let operation = graph.operation_mut(root)?;
    let Relational::Limit { count, .. } = operation else {
        return Err(QueryError::PipelineInvariant(
            "graph traversal has no page limit".into(),
        ));
    };
    *count = input.fetch_limit();
    if input.cursor.is_none() && input.aggregation.sort.is_none() {
        return Ok((root, 0));
    }
    let mut keys = Vec::new();
    if let Some(order) = &input.order_by {
        let index = input
            .nodes
            .iter()
            .position(|node| node.id == order.node)
            .ok_or_else(|| QueryError::PaginationError("sort node missing".into()))?;
        let relation = graph.input_node(root, index)?;
        keys.push((
            graph.column(relation, &order.property)?,
            order.direction == crate::input::OrderDirection::Desc,
        ));
    }
    let mut keys = keys
        .into_iter()
        .map(|(column, descending)| (E::Column(column), descending))
        .collect::<Vec<_>>();
    match input.query_type {
        QueryType::Traversal => {
            for index in 0..input.nodes.len() {
                let identity = E::Column(graph.input_identity(root, input, index)?);
                if !keys.iter().any(|(value, _)| *value == identity) {
                    keys.push((identity, false));
                }
            }
            for output in graph.outputs(root)? {
                if graph.output_label(output)?.ends_with("_path_nodes") {
                    keys.push((
                        E::ToString(Box::new(graph.projection(output)?.value.clone())),
                        false,
                    ));
                }
            }
        }
        QueryType::Aggregation => {
            if let Some(sort) = &input.aggregation.sort {
                let output = graph
                    .outputs(root)?
                    .find(|output| {
                        graph
                            .output_label(*output)
                            .is_ok_and(|label| label == sort.column)
                    })
                    .ok_or_else(|| {
                        QueryError::PaginationError("aggregate sort output missing".into())
                    })?;
                keys.push((
                    graph.projection(output)?.value.clone(),
                    sort.direction == crate::input::OrderDirection::Desc,
                ));
            }
            if input.cursor.is_some() {
                for group in graph.operation(root)?.groups() {
                    if !keys.iter().any(|(value, _)| value == group) {
                        keys.push((group.clone(), false));
                    }
                }
            }
        }
        QueryType::Neighbors | QueryType::PathFinding => {
            let names = if input.query_type == QueryType::Neighbors {
                let center = &input.nodes[0];
                let primary_key = crate::constants::primary_key_column(&center.id);
                let center_key = if graph.outputs(root)?.any(|output| {
                    graph
                        .output_label(output)
                        .is_ok_and(|label| label == primary_key)
                }) {
                    primary_key
                } else {
                    crate::constants::redaction_id_column(&center.id)
                };
                vec![
                    center_key,
                    crate::constants::neighbor_id_column().into(),
                    crate::constants::neighbor_type_column().into(),
                    crate::constants::relationship_type_column().into(),
                    crate::constants::neighbor_is_outgoing_column().into(),
                ]
            } else {
                vec![
                    "depth".into(),
                    crate::constants::path_column().into(),
                    crate::constants::edge_kinds_column().into(),
                ]
            };
            for name in names {
                let output = graph
                    .outputs(root)?
                    .find(|output| graph.output_label(*output).is_ok_and(|label| label == name))
                    .ok_or_else(|| {
                        QueryError::PaginationError(format!("missing cursor output {name}"))
                    })?;
                let mut value = graph.projection(output)?.value.clone();
                if input.query_type == QueryType::PathFinding && name != "depth" {
                    value = E::ToString(Box::new(value));
                }
                keys.push((value, false));
            }
        }
        QueryType::Hydration => {}
    }
    let operation = std::mem::replace(graph.operation_mut(root)?, Relational::One);
    let Relational::Limit { input: source, .. } = operation else {
        unreachable!()
    };
    *graph.operation_mut(root)? = match *source {
        Relational::Sort { input, .. } => *input,
        operation => operation,
    };
    let outputs = graph.outputs(root)?.collect::<Vec<_>>();
    let page = graph.select(Relational::One);
    let relation = graph.derive(page, root, "page")?;
    for output in outputs {
        graph.project(
            page,
            graph.output_label(output)?.to_owned(),
            E::Column(graph.output_column(relation, output)?),
        )?;
    }
    let mut page_keys = Vec::new();
    for (index, (value, descending)) in keys.into_iter().enumerate() {
        let output = graph.project(root, cursor_column(index), value)?;
        page_keys.push((graph.output_column(relation, output)?, descending));
    }
    let root = page;
    let keys = page_keys;
    *graph.operation_mut(root)? = Relational::source(relation).limit(input.fetch_limit());
    let mut predicate = None;
    if let Some(after) = input
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.after.as_ref())
    {
        let values = decode(after, query_hash)?;
        if values.len() != keys.len() {
            return Err(QueryError::PaginationError(
                "cursor key count mismatch".into(),
            ));
        }
        let nullable = nullable_flags(input, keys.len());
        let mut prefix: Option<E<'a>> = None;
        for (index, ((key, descending), value)) in keys.iter().zip(values).enumerate() {
            let column = E::Column(*key);
            let test = |operator, argument| E::Predicate {
                operator,
                value: Box::new(column.clone()),
                argument,
                fold_case: false,
            };
            let equal = if let Some(value) = value {
                let crate::query_graph::ValueType::Scalar(data_type) =
                    graph.column_type(*key, &mut Default::default())?
                else {
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
                let value = E::Literal { data_type, value };
                let mut advance = test(
                    if *descending {
                        crate::input::FilterOp::Lt
                    } else {
                        crate::input::FilterOp::Gt
                    },
                    Some(Box::new(value.clone())),
                );
                if nullable[index] {
                    advance = E::Or(
                        Box::new(advance),
                        Box::new(test(crate::input::FilterOp::IsNull, None)),
                    );
                }
                let advance = match &prefix {
                    Some(prefix) => E::And(Box::new(prefix.clone()), Box::new(advance)),
                    None => advance,
                };
                predicate = Some(match predicate {
                    Some(previous) => E::Or(Box::new(previous), Box::new(advance)),
                    None => advance,
                });
                E::equal(column, value)
            } else {
                test(crate::input::FilterOp::IsNull, None)
            };
            prefix = Some(match prefix {
                Some(prefix) => E::And(Box::new(prefix), Box::new(equal)),
                None => equal,
            });
        }
        predicate.get_or_insert(E::Boolean(false));
    }
    if input.cursor.is_some() {
        for (index, (key, _)) in keys.iter().enumerate() {
            graph.project(
                root,
                cursor_column(index),
                E::ToString(Box::new(E::Column(*key))),
            )?;
        }
    }
    let key_count = if input.cursor.is_some() {
        keys.len()
    } else {
        0
    };
    let Relational::Limit { input: source, .. } = graph.operation_mut(root)? else {
        unreachable!()
    };
    let mut operation = std::mem::replace(source.as_mut(), Relational::One);
    if let Relational::Sort { input, .. } = operation {
        operation = *input;
    }
    if let Some(predicate) = predicate {
        operation = operation.filter(predicate);
    }
    **source = operation.sort(keys);
    Ok((root, key_count))
}

/// User sort properties and aggregation keys can be NULL (NULLs sort last in
/// ClickHouse, both directions); compiler-generated tie-breakers are primary
/// keys and never are.
fn nullable_flags(input: &Input, key_count: usize) -> Vec<bool> {
    let mut flags = vec![false; key_count];
    match input.query_type {
        QueryType::Aggregation => {
            let mut idx = 0;
            if let Some(sort) = &input.aggregation.sort {
                let sorts_on_count = input.aggregation.metrics.iter().any(|a| {
                    a.output_name() == sort.column
                        && matches!(a.expr.function(), AggFunction::Count)
                });
                if let Some(f) = flags.first_mut() {
                    *f = !sorts_on_count;
                }
                idx = 1;
            }
            for f in flags.iter_mut().skip(idx) {
                *f = true;
            }
        }
        QueryType::PathFinding => {}
        _ => {
            if input.order_by.is_some()
                && let Some(f) = flags.first_mut()
            {
                *f = true;
            }
        }
    }
    flags
}

fn place_seek_in_having(
    q: &mut Query,
    order_by: &[OrderExpr],
    values: &[Option<String>],
    nullable: &[bool],
) {
    let seek = seek_predicate(order_by, values, nullable);
    q.having = Some(match q.having.take() {
        Some(h) => Expr::and(h, seek),
        None => seek,
    });
}

/// A WHERE that references SELECT aliases (union arms, fused-neighbors
/// arrayJoin projections) silently fails to filter in ClickHouse, so hoist
/// ORDER BY/LIMIT and the seek above a subquery whose aliases are real columns.
fn hoist_page_subquery(
    q: &mut Query,
    order_by: &[OrderExpr],
    values: &[Option<String>],
    nullable: &[bool],
) {
    let outer_order = inner_order_as_outer(order_by);
    let seek = seek_predicate(&outer_order, values, nullable);
    let mut inner = std::mem::take(q);
    let limit = inner.limit.take();
    inner.order_by = vec![];
    *q = Query {
        select: vec![SelectExpr::star()],
        from: TableRef::subquery(inner, "_page"),
        where_clause: Some(seek),
        order_by: outer_order,
        limit,
        ..Default::default()
    };
}

fn merge_seek_into_where(
    q: &mut Query,
    order_by: &[OrderExpr],
    values: &[Option<String>],
    nullable: &[bool],
) {
    let seek = seek_predicate(order_by, values, nullable);
    q.where_clause = Some(match q.where_clause.take() {
        Some(w) => Expr::and(w, seek),
        None => seek,
    });
}

/// Column refs lose their table alias once hoisted above the `_page` subquery.
fn inner_order_as_outer(order_by: &[OrderExpr]) -> Vec<OrderExpr> {
    order_by
        .iter()
        .map(|o| OrderExpr {
            expr: match &o.expr {
                Expr::Column { column, .. } => Expr::ident(column.clone()),
                other => other.clone(),
            },
            desc: o.desc,
        })
        .collect()
}

/// `(k0 > v0) OR (k0 = v0 AND k1 > v1) OR ...` with `<` on DESC keys. Values
/// travel as strings; ClickHouse coerces them to each key's native type.
///
/// NULL boundaries rely on ClickHouse sorting NULLs last in both directions:
/// a non-null boundary on a nullable key must also admit the key's NULL tail,
/// and a null boundary contributes no advance arm of its own (progress happens
/// on deeper keys under an `IS NULL` prefix). An all-null boundary yields
/// FALSE, which is correct: NULLs-last ordering puts such a row at the very
/// end of the stream.
fn seek_predicate(order_by: &[OrderExpr], values: &[Option<String>], nullable: &[bool]) -> Expr {
    let param = |v: &String| Expr::param(SqlType::String, v.clone());
    (0..order_by.len())
        .filter_map(|j| {
            let Some(vj) = &values[j] else {
                return None;
            };
            let mut parts: Vec<Expr> = (0..j)
                .map(|i| match &values[i] {
                    Some(vi) => Expr::eq(order_by[i].expr.clone(), param(vi)),
                    None => Expr::unary(Op::IsNull, order_by[i].expr.clone()),
                })
                .collect();
            let op = if order_by[j].desc { Op::Lt } else { Op::Gt };
            let mut advance = Expr::binary(op, order_by[j].expr.clone(), param(vj));
            if nullable[j] {
                advance = Expr::binary(
                    Op::Or,
                    advance,
                    Expr::unary(Op::IsNull, order_by[j].expr.clone()),
                );
            }
            parts.push(advance);
            Some(Expr::conjoin(parts).expect("seek arm is non-empty"))
        })
        .reduce(|a, b| Expr::binary(Op::Or, a, b))
        .unwrap_or_else(|| Expr::eq(Expr::int(0), Expr::int(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn some(vals: &[&str]) -> Vec<Option<String>> {
        vals.iter().map(|v| Some(v.to_string())).collect()
    }

    #[test]
    fn token_roundtrip_and_hash_binding() {
        let t = encode(42, &some(&["2026-01-16 19:15:23.456", "1234"]));
        assert_eq!(
            decode(&t, 42).unwrap(),
            some(&["2026-01-16 19:15:23.456", "1234"])
        );
        assert!(matches!(
            decode(&t, 43),
            Err(QueryError::PaginationError(_))
        ));
        assert!(matches!(
            decode("not-a-token", 42),
            Err(QueryError::PaginationError(_))
        ));
    }

    #[test]
    fn token_roundtrips_null_keys() {
        let keys = vec![None, Some("7".to_string())];
        let t = encode(9, &keys);
        assert_eq!(decode(&t, 9).unwrap(), keys);
    }

    #[test]
    fn canonical_hash_ignores_cursor_and_key_order() {
        let a: serde_json::Value =
            serde_json::from_str(r#"{"limit":5,"query_type":"traversal"}"#).unwrap();
        let b: serde_json::Value = serde_json::from_str(
            r#"{"query_type":"traversal","cursor":{"page_size":10},"limit":5}"#,
        )
        .unwrap();
        let c: serde_json::Value =
            serde_json::from_str(r#"{"limit":6,"query_type":"traversal"}"#).unwrap();
        assert_eq!(canonical_hash(&a), canonical_hash(&b));
        assert_ne!(canonical_hash(&a), canonical_hash(&c));
    }

    #[test]
    fn seek_predicate_is_lexicographic_dnf() {
        let order = vec![
            OrderExpr::desc(Expr::col("mr", "created_at")),
            OrderExpr::asc(Expr::col("e0", "source_id")),
        ];
        let values = some(&["2026-01-16", "7"]);
        let expr = seek_predicate(&order, &values, &[false, false]);
        let Expr::BinaryOp {
            op: Op::Or,
            left,
            right,
        } = expr
        else {
            panic!("expected OR of two arms");
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
        let expr = seek_predicate(&order, &some(&["2026-01-16", "7"]), &[true, false]);
        let Expr::BinaryOp {
            op: Op::Or, left, ..
        } = expr
        else {
            panic!("expected OR of two arms");
        };
        let Expr::BinaryOp {
            op: Op::Or, right, ..
        } = *left
        else {
            panic!("nullable advance arm should be `< OR IS NULL`");
        };
        assert!(matches!(*right, Expr::UnaryOp { op: Op::IsNull, .. }));
    }

    #[test]
    fn null_boundary_recurses_on_tie_breaker_under_is_null_prefix() {
        let order = vec![
            OrderExpr::desc(Expr::col("mr", "merged_at")),
            OrderExpr::asc(Expr::col("mr", "id")),
        ];
        let expr = seek_predicate(&order, &[None, Some("7".to_string())], &[true, false]);
        let Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } = expr
        else {
            panic!("null boundary should leave a single AND arm");
        };
        assert!(matches!(*left, Expr::UnaryOp { op: Op::IsNull, .. }));
        assert!(matches!(*right, Expr::BinaryOp { op: Op::Gt, .. }));
    }

    #[test]
    fn all_null_boundary_yields_false() {
        let order = vec![OrderExpr::asc(Expr::col("g", "name"))];
        let expr = seek_predicate(&order, &[None], &[true]);
        assert!(matches!(expr, Expr::BinaryOp { op: Op::Eq, .. }));
    }
}
