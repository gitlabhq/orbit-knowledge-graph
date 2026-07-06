//! Keyset pagination pass. Runs after enforce/security/partition so the final
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
use gkg_utils::clickhouse::ChType;

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

pub fn apply(node: &mut Node, input: &mut Input) -> Result<()> {
    let Some(cursor) = &input.cursor else {
        return Ok(());
    };
    let Node::Query(q) = node else {
        return Ok(());
    };
    let order_by = q.order_by.clone();
    input.compiler.cursor_key_count = order_by.len();
    if order_by.is_empty() {
        return Ok(());
    }

    append_readback_columns(q, &order_by);

    let Some(values) = &cursor.seek else {
        return Ok(());
    };
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
        place_seek_in_having(q, &order_by, values, &nullable);
    } else if !q.union_all.is_empty() || alias_scoped {
        hoist_page_subquery(q, &order_by, values, &nullable);
    } else {
        merge_seek_into_where(q, &order_by, values, &nullable);
    }
    Ok(())
}

fn append_readback_columns(q: &mut Query, order_by: &[OrderExpr]) {
    for (i, o) in order_by.iter().enumerate() {
        let hidden = SelectExpr::new(
            Expr::func("toString", vec![o.expr.clone()]),
            cursor_column(i),
        );
        for arm in &mut q.union_all {
            arm.select.push(hidden.clone());
        }
        q.select.push(hidden);
    }
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
                    let default = a.function.as_sql().to_lowercase();
                    a.alias.as_deref().unwrap_or(&default) == sort.column
                        && matches!(a.function, AggFunction::Count)
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
    let param = |v: &String| Expr::param(ChType::String, v.clone());
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
